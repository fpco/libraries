{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE ViewPatterns #-}

-- | This module provides a particular way of using
-- "Distributed.WorkQueue" along with "Distributed.RedisQueue".  It
-- also makes rather non-generic decisions like using 'Vector' and
-- "STMContainers.Map".
module Distributed.JobQueue
    ( WorkerConfig(..)
    , defaultWorkerConfig
    , jobQueueWorker
    , requestSlave
    , ClientVars(..)
    , newClientVars
    , jobQueueClient
    , jobQueueRequest
    , jobQueueRequest'
    , slaveRequestKey
    -- * Internals
    , SlaveRequest(..)
    ) where

import           ClassyPrelude
import           Control.Concurrent.Async (withAsync, race)
import           Control.Concurrent.Lifted (fork, threadDelay)
import           Control.Concurrent.STM (check)
import           Control.Monad.Logger (MonadLogger, logWarnS, logErrorS, logDebugS)
import           Control.Monad.Trans.Control (control, liftBaseWith, MonadBaseControl)
import           Data.Binary (Binary, decode, encode)
import           Data.List.NonEmpty (NonEmpty((:|)))
import           Data.Streaming.Network
import           Data.Streaming.NetworkMessage
import           Data.WorkQueue
import           Distributed.RedisQueue
import           Distributed.WorkQueue
import           FP.Redis ( MonadConnect, MonadCommand, Seconds(..)
                          , LKey(..), Key(..), ConnectInfo
                          , runCommand, runCommand_, brpop, rpush, lpush
                          )
import           Focus (Decision(Remove, Replace))
import           Network.HostName (getHostName)
import qualified STMContainers.Map as SM
import           System.Posix.Process (getProcessID)

-- | Configuration of a 'jobQueueWorker'.
data WorkerConfig = WorkerConfig
    { workerResponseDataExpiry :: Seconds
      -- ^ How many seconds the response data should be kept around.
      -- The longer it's kept around, the more opportunity there is to
      -- eliminate redundant work, if identical requests are made.  A
      -- longer expiry also allows more time between sending a
      -- response notification and the client reading its data.
    , workerKeyPrefix :: ByteString
      -- ^ Prefix used for redis keys.
    , workerConnectInfo :: ConnectInfo
      -- ^ Info used to connect to the redis server.
    , workerMasterLocalSlaves :: Int
      -- ^ How many local slaves a master server should run.
    , workerMasterPort :: Int
      -- ^ Which port to use.
    , workerPopRequestRetry :: Int
      -- ^ Microseconds to retry popRequest.  This is only used in the
      -- erroneous case where the in-progress queue is blocked yet the
      -- slave isn't running.
    }

-- | Given a redis key prefix and redis connection information, builds
-- a default 'WorkerConfig'.
--
-- This config has response data expire every hour, and masters have 0
-- local slaves.
defaultWorkerConfig :: ByteString -> ConnectInfo -> WorkerConfig
defaultWorkerConfig prefix connectInfo = WorkerConfig
    { workerResponseDataExpiry = Seconds 3600
    , workerKeyPrefix = prefix
    , workerConnectInfo = connectInfo
    , workerMasterLocalSlaves = 0
    , workerMasterPort = 4000
    , workerPopRequestRetry = 1000 * 1000 * 5
    }

-- Hostname and port of the master the slave should connect to.
data SlaveRequest = SlaveRequest ByteString Int
    deriving (Generic, Show)

instance Binary SlaveRequest

data WorkerState = Idle | Slave | Master
    deriving (Eq, Show)

-- | This runs a job queue worker.
jobQueueWorker
    :: forall m initialData request response payload result.
       ( MonadConnect m
       , Sendable initialData
       , Sendable request
       , Sendable response
       , Sendable payload
       , Sendable result
       )
    => WorkerConfig
    -> IO initialData
    -- ^ This is run once per worker, if it ever becomes a master
    -- server.  The data is then sent to the slaves.
    -> (initialData -> payload -> IO result)
    -- ^ This is the computation function run by slaves.  It computes
    -- @result@ from @payload@.  These types need to 'Sendable' such
    -- that they can be sent with "Data.Streaming.NetworkMessage".
    -> (initialData -> RedisInfo -> request -> WorkQueue payload result -> IO response)
    -- ^ This function runs on the master after it's received a
    -- request.  It's expected that the master will use this request
    -- to enqueue work items on the provided 'WorkQueue'.  The results
    -- of this can then be accumulated into a @response@ to be sent
    -- back to the client.
    -> m ()
jobQueueWorker config init calc inner = do
    wid <- withRedis' getWorkerId
    initialDataRef <- newIORef Nothing
    state <- liftIO $ newTVarIO Idle
    ready <- liftIO $ newTVarIO False
    let worker = WorkerInfo wid (workerResponseDataExpiry config)
        waitFor s = atomically $ check . (s ==) =<< readTVar state
        tryTransition s s' = do
            (cur, success) <- atomically $ do
                cur <- readTVar state
                if s == cur
                    then do
                        writeTVar state s'
                        return (cur, True)
                    else return (cur, False)
            if success
                then $logDebugS "JobQueue" ("Transitioned from " <> tshow s <> " to " <> tshow s')
                else $logDebugS "JobQueue" ("Didn't transition from " <> tshow s <> " to " <> tshow s' <> " (Currently " <> tshow cur <> ")")
            return success
        -- Loops forever, becoming a master to service a request
        -- when this worker isn't acting as a slave.
        masterThread :: m void
        masterThread = forever $ do
            waitFor Idle
            withRedis' $ \r -> do
                atomically $ check =<< readTVar ready
                mreq <- popRequest r worker
                case mreq of
                    Nothing -> do
                        s <- atomically $ readTVar state
                        when (s == Idle) $ do
                            $logErrorS "JobQueue" "popRequest failed, but worker is not in slave mode"
                            threadDelay (workerPopRequestRetry config)
                    Just req  -> do
                        $logDebugS "JobQueue" "Got popRequest response"
                        success <- tryTransition Idle Master
                        case success of
                            False -> unpopRequest r worker
                            True -> do
                                master req initialDataRef r worker
                                success' <- tryTransition Master Idle
                                when (not success') $ fail "Failed to transition from Master to Idle"
        slaveThread :: m void
        slaveThread = withRedis' $ \r -> forever $ do
            waitFor Idle
            req <- popSlaveRequest r
            $logDebugS "JobQueue" $ "Got popSlaveRequest response: " <> tshow req
            success <- tryTransition Idle Slave
            case success of
                False -> unpopSlaveRequest r req
                True -> do
                    slave r req worker
                    success' <- tryTransition Slave Idle
                    when (not success') $ fail "Failed to transition from Slave to Idle"
        heartbeats = withRedis' $ \r -> sendHeartbeats r worker ready
    masterThread `raceLifted` slaveThread `raceLifted` heartbeats
  where
    -- This needs to have its own redis connection as we have two
    -- blocking redis calls.
    --
    -- Since this uses brpop rather than brpoplpush, if the server
    -- dies, then slave requests can be lost.  This is acceptable for
    -- now, but may be fixed later.
    popSlaveRequest :: RedisInfo -> m SlaveRequest
    popSlaveRequest r = do
        let k = slaveRequestKey r
        mreq <- runCommand (redisConnection r) $ brpop (k :| []) (Seconds 0)
        case mreq of
            Nothing -> fail "impossible: blpop reported timeout"
            Just (decode . fromStrict . snd -> req) -> return req
    unpopSlaveRequest :: RedisInfo -> SlaveRequest -> m ()
    unpopSlaveRequest r req = do
        let k = slaveRequestKey r
            bs = toStrict (encode req)
        runCommand_ (redisConnection r) $ rpush k (bs :| [])
    slave :: RedisInfo -> SlaveRequest -> WorkerInfo -> m ()
    slave r (SlaveRequest host port) worker = do
        deactivateWorker r worker
        runSlave
            (clientSettingsTCP port host)
            defaultNMSettings
            calc
        reactivateWorker r worker
    master :: (RequestId, BackchannelId, ByteString)
           -> IORef (Maybe initialData)
           -> RedisInfo
           -> WorkerInfo
           -> m ()
    master (requestId, backchannelId, request) initialDataRef r worker = do
        initialData <- liftIO $ do
            minitialData <- readIORef initialDataRef
            case minitialData of
                Just initialData -> return initialData
                Nothing -> do
                    initialData <- init
                    writeIORef initialDataRef (Just initialData)
                    return initialData
        withMaster (runTCPServer ss) defaultNMSettings initialData $ \queue ->
            withLocalSlaves queue (workerMasterLocalSlaves config) (calc initialData) $ do
                let decoded = decode (fromStrict request)
                response <- liftIO $ inner initialData r decoded queue
                let encoded = toStrict (encode response)
                sendResponse r worker requestId backchannelId encoded
    ss = setAfterBind (const $ putStrLn $ "Listening on " ++ tshow (workerMasterPort config))
                      (serverSettingsTCP (workerMasterPort config) "*")
    withRedis' = withRedis (workerKeyPrefix config) (workerConnectInfo config)

requestSlave
    :: MonadCommand m
    => WorkerConfig
    -> RedisInfo
    -> m ()
requestSlave config r = do
    --TODO: store hostName somewhere?
    hostName <- liftIO getHostName
    let request = SlaveRequest (encodeUtf8 (pack hostName :: Text)) (workerMasterPort config)
        encoded = toStrict (encode request)
    runCommand_ (redisConnection r) $ lpush (slaveRequestKey r) (encoded :| [])

-- Key used to for storing requests for slaves.
slaveRequestKey :: RedisInfo -> LKey
slaveRequestKey r = LKey $ Key $ redisKeyPrefix r <> "slave-request"

-- | Variables and settings used by 'jobQueueClient' /
-- 'jobQueueRequest'.
data ClientVars m response = ClientVars
    { clientSubscribed :: TVar Bool
      -- ^ This is set to 'True' once the client is subscribed to its
      -- backchannel, and so ready to send reqeusts.
      -- 'jobQueueRequest' blocks until this is 'True'.
    , clientDispatch :: SM.Map RequestId (response -> m ())
      -- ^ A map between 'RequestId's and their associated handlers.
      -- 'jobQueueClient' uses this to invoke the handlers inserted by
      -- 'jobQueueRequest'.
    , clientHeartbeatCheckIvl :: Seconds
      -- ^ How often to send heartbeat requests to the workers, and
      -- check for responses.  This value should be the same for all
      -- clients.
    , clientInfo :: ClientInfo
      -- ^ Information about the client needed to invoke the client
      -- functions in "Distributed.RedisQueue".
    }

-- | Create a new 'ClientVars' value.  This uses a default method of
-- computing a 'BackChannelId', which combines the host name with the
-- process ID.  The user must provide values for
-- 'clientHeartbeatCheckIvl' and 'clientRequestExpiry' as arguments.
newClientVars :: Seconds -> Seconds -> IO (ClientVars m response)
newClientVars heartbeatCheckIvl requestExpiry = ClientVars
    <$> newTVarIO False
    <*> SM.newIO
    <*> pure heartbeatCheckIvl
    <*> (ClientInfo <$> getBackchannelId <*> pure requestExpiry)

-- | Runs a listener for responses from workers, which dispatches to
-- callbacks registered with 'jobQueueRequest'.  It also runs
-- 'checkHeartbeats', to ensure that some server periodically checks
-- the worker heartbeats.
--
-- This function should be run in its own thread, as it never returns
-- (the return type is @void@).
jobQueueClient
    :: (MonadConnect m, Sendable response)
    => ClientVars m response
    -> RedisInfo
    -> m void
jobQueueClient cvs r = do
    let checker = checkHeartbeats r (clientHeartbeatCheckIvl cvs)
        subscribe = subscribeToResponses r (clientInfo cvs) (clientSubscribed cvs)
    control $ \restore -> withAsync (restore checker) $ \_ -> restore $
        subscribe $ \requestId -> do
            -- Lookup the handler before fetching / deleting the response,
            -- as the message may get delivered to multiple clients.
            let lookupAndRemove handler = return (handler, Remove)
            mhandler <- atomically $
                SM.focus lookupAndRemove requestId (clientDispatch cvs)
            case mhandler of
                -- TODO: Is a mere warning sufficient? Perhaps we need
                -- guarantees about uniqueness of back channel, and number
                -- of times a response is yielded, in order to have
                -- guarantees about delivery.
                Nothing -> $logWarnS "JobQueue" $
                    "Couldn't find handler to deal with response to " <>
                    tshow requestId
                Just handler -> do
                    response <- readResponse r requestId
                    handler (decode (fromStrict response))

-- | Once a 'jobQueueClient' has been run with the 'ClientVars' value,
-- this function can be used to make requests and block on their
-- response.  It's up to the user of this and 'jobQueueWorker' to
-- ensure that the types of @payload@ match up, and that the
-- 'ByteString' responses are encoded as expected.
jobQueueRequest
    :: (MonadCommand m, MonadLogger m, Sendable request, Sendable response)
    => ClientVars m response
    -> RedisInfo
    -> request
    -> m response
jobQueueRequest cvs r request = do
    resultVar <- newEmptyMVar
    jobQueueRequest' cvs r request $ putMVar resultVar
    takeMVar resultVar

-- | This is a non-blocking version of jobQueueRequest.  When the
-- response comes back, the provided callback is invoked.  One thing
-- to note is that exceptions thrown by the callback do not get
-- rethrown.  Instead, they're printed, due to jobQueueClient using
-- 'FP.Redis.withSubscriptionsWrapped'.
--
-- This command does block on the request being enqueued.  First, it
-- blocks on 'clientSubscribed', then it may also need to wait for the
-- Redis server to become available.
jobQueueRequest'
    :: (MonadCommand m, MonadLogger m, Sendable request, Sendable response)
    => ClientVars m response
    -> RedisInfo
    -> request
    -> (response -> m ())
    -> m ()
jobQueueRequest' cvs r request handler = do
    -- TODO: Does it make sense to block on subscription like this?
    -- Perhaps instead servers should block even accepting requests
    -- until it's subscribed.
    atomically $ check =<< readTVar (clientSubscribed cvs)
    (k, mresponse) <- pushRequest r (clientInfo cvs) (toStrict (encode request))
    case mresponse of
        Nothing ->
            atomically $ SM.focus addOrExtend k (clientDispatch cvs)
        Just response ->
            void $ fork $ runHandler (decode (fromStrict response))
  where
    addOrExtend Nothing = return ((), Replace runHandler)
    addOrExtend (Just old) = return ((), Replace (\x -> old x >> runHandler x))
    runHandler response =
        catchAny (handler response) $ \ex ->
            $logErrorS "JobQueue" $ "jobQueueRequest' callbackHandler: " ++ tshow ex

getBackchannelId :: IO BackchannelId
getBackchannelId = BackchannelId <$> getHostAndProcessId

getWorkerId :: MonadCommand m => RedisInfo -> m WorkerId
getWorkerId redis = getUnusedWorkerId redis =<< liftIO getHostAndProcessId

getHostAndProcessId :: IO ByteString
getHostAndProcessId = do
    hostName <- getHostName
    pid <- getProcessID
    return $ encodeUtf8 $ pack $ hostName <> ":" <> show pid

-- Note: Ideally this would yield (Either a b)
--
-- We don't need that for the usage here, though.
raceLifted :: MonadBaseControl IO m => m a -> m b -> m ()
raceLifted f g =
    liftBaseWith $ \restore ->
        void $ restore f `race` restore g
