{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE ViewPatterns #-}

-- | This module provides a particular way of using
-- "Distributed.WorkQueue" along with "Distributed.RedisQueue".  It
-- also makes rather non-generic decisions like using 'Vector' and
-- "STMContainers.Map".
--
-- Because it has heartbeats, it adds the guarantee that enqueued work
-- will not be lost until it's completed, even in the presence of
-- server failure.  A failure in Redis persistence can invalidate this
-- guarantee.
--
-- SIDENOTE: We may need a high reliability redis configuration
-- for this guarantee as well. The redis wikipedia article
-- mentions that the default config can lose changes received
-- during the 2 seconds before failure.
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
    -- * Internals, used by tests
    , slaveRequestsKey
    , SlaveRequest(..)
    ) where

import           ClassyPrelude
import           Control.Concurrent.Async (Async, async, link, withAsync, race)
import           Control.Concurrent.Lifted (fork, threadDelay)
import           Control.Concurrent.STM (check)
import           Control.Monad.Logger (MonadLogger, logWarnS, logErrorS, logDebugS)
import           Control.Monad.Trans.Control (control, liftBaseWith, MonadBaseControl, StM)
import           Data.Binary (Binary, decode, encode)
import           Data.List.NonEmpty (NonEmpty((:|)), nonEmpty)
import           Data.Streaming.Network (clientSettingsTCP, runTCPServer, setAfterBind, serverSettingsTCP)
import           Data.Streaming.NetworkMessage (Sendable, defaultNMSettings)
import           Data.WorkQueue
import           Distributed.RedisQueue
import           Distributed.RedisQueue.Internal (run, run_, activeKey, requestsKey)
import           Distributed.WorkQueue
import           FP.Redis
import           FP.Redis.Mutex
import           Focus (Decision(Remove, Replace))
import           GHC.IO.Exception (IOException(IOError), ioe_type, IOErrorType(NoSuchThing))
import           Network.HostName (getHostName)
import qualified STMContainers.Map as SM
import           System.Posix.Process (getProcessID)
import           System.Random (randomRIO)

--TODO:
--
-- * Consider what happens when an error is thrown by the response
-- handler.  Gotta make this consistent between the 'fork' used when
-- the response is already available, and the one that's inside a
-- subscription.
--
-- * Make heartbeats only subscribe when needed.
--
-- * Send exceptions back to the client.
--
-- * Check types at runtime.
--
-- * Add a timeout to the slave connection

data DistributedJobQueueException
    = WorkStillInProgress WorkerId
    deriving (Eq, Show, Typeable)

instance Exception DistributedJobQueueException

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
defaultWorkerConfig prefix ci = WorkerConfig
    { workerResponseDataExpiry = Seconds 3600
    , workerKeyPrefix = prefix
    , workerConnectInfo = ci
    , workerMasterLocalSlaves = 0
    , workerMasterPort = 4000
    , workerPopRequestRetry = 1000 * 1000 * 5
    }

-- Hostname and port of the master the slave should connect to.
data SlaveRequest = SlaveRequest ByteString Int
    deriving (Generic, Show)

instance Binary SlaveRequest

data SubscribeOrCheck
    -- | When 'loop' doesn't find any work, it's called again using
    -- this constructor as its argument.  When this happens, it will
    -- block on the 'MVar', waiting on the requestsChannel.  However,
    -- before blocking, it makes one more attempt at popping work,
    -- because some work may have come in before the subscription was
    -- established.
    = SubscribeToRequests (MVar ()) (IORef Connection)
    -- | This constructor indicates that 'loop' should just check for
    -- work, without subscribing to requestsChannel.  When the worker
    -- successfully popped work last time, there's no reason to
    -- believe there isn't more work immediately available.
    | CheckRequests

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
jobQueueWorker config init calc inner = withRedis' config $ \redis -> do
    wid <- getWorkerId redis
    initialDataRef <- newIORef Nothing
    heartbeatsReady <- liftIO $ newTVarIO False
    let worker = WorkerInfo wid (workerResponseDataExpiry config)
        loop soc = do
            -- If there's slave work to be done, then do it.
            mslave <- popSlaveRequest redis
            case mslave of
                Just slave -> do
                    unsubscribeToRequests soc
                    becomeSlave slave
                    loop CheckRequests
                Nothing -> do
                    -- If there isn't any slave work, then check if
                    -- there's a request, and become a master if there
                    -- is one.
                    mreq <- popRequest redis worker
                    case (mreq, soc) of
                        (Just req, _) -> do
                            unsubscribeToRequests soc
                            becomeMaster req
                            loop CheckRequests
                        (Nothing, CheckRequests) -> do
                            subscribeToRequests
                        (Nothing, SubscribeToRequests notified _) -> do
                            takeMVar notified
                            loop soc
        -- When there isn't any work, run the loop again, this time
        -- with a subscription to the channel.
        subscribeToRequests = do
            ready <- liftIO $ newTVarIO False
            notified <- newEmptyMVar
            -- This error shouldn't happen because we block on 'ready'
            -- below.  In order for 'ready' to be set to 'True', this
            -- IORef will have been set.
            connVar <- newIORef (error "impossible: connVar not initialized.")
            let subs = subscribe (requestsChannel redis :| []) :| []
            thread <- asyncLifted $
                withSubscriptionsExConn (redisConnectInfo redis) subs $ \conn -> do
                    writeIORef connVar conn
                    return $ trackSubscriptionStatus ready $ \_ _ ->
                        void $ tryPutMVar notified ()
            liftIO $ link thread
            loop $ SubscribeToRequests notified connVar
        unsubscribeToRequests CheckRequests = return ()
        unsubscribeToRequests (SubscribeToRequests _ connVar) = do
            conn <- readIORef connVar
            disconnect conn
        becomeSlave :: SlaveRequest -> m ()
        becomeSlave req@(SlaveRequest host port) = do
            $logDebugS "JobQueue" "Becoming Slave"
            deactivateWorker redis worker
            eres <- try $ runSlave (clientSettingsTCP port host) defaultNMSettings calc
            case eres of
                Right () -> return ()
                -- This indicates that the slave couldn't connect.
                Left (IOError { ioe_type = NoSuchThing }) ->
                    $logDebugS "JobQueue" $ "Failed to connect with " <> tshow req
                Left err -> throwM err
        becomeMaster :: (RequestId, BackchannelId, ByteString) -> m ()
        becomeMaster (rid, bid, req) = do
            $logDebugS "JobQueue" "Becoming master"
            initialData <- do
                minitialData <- readIORef initialDataRef
                case minitialData of
                    Just initialData -> return initialData
                    Nothing -> do
                        initialData <- liftIO init
                        writeIORef initialDataRef (Just initialData)
                        return initialData
            withMaster (runTCPServer ss) defaultNMSettings initialData $ \queue ->
                withLocalSlaves queue (workerMasterLocalSlaves config) (calc initialData) $ do
                    let decoded = decode (fromStrict req)
                    response <- liftIO $ inner initialData redis decoded queue
                    let encoded = toStrict (encode response)
                    sendResponse redis worker rid bid encoded
        ss = setAfterBind (const $ putStrLn $ "Listening on " ++ tshow (workerMasterPort config))
                          (serverSettingsTCP (workerMasterPort config) "*")
        start = do
            atomically $ check =<< readTVar heartbeatsReady
            loop CheckRequests
        heartbeats =
            withRedis' config $ \r -> sendHeartbeats r worker heartbeatsReady
    start `raceLifted` heartbeats

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
    runCommand_ (redisConnection r) $ lpush (slaveRequestsKey r) (encoded :| [])
    notifyRequestAvailable r

notifyRequestAvailable :: MonadCommand m => RedisInfo -> m ()
notifyRequestAvailable r =
    runCommand_ (redisConnection r) $ publish (requestsChannel r) ""

popSlaveRequest :: MonadCommand m => RedisInfo -> m (Maybe SlaveRequest)
popSlaveRequest r =
    fmap (decode . fromStrict) <$>
    runCommand (redisConnection r) (rpop (slaveRequestsKey r))

-- Key used to for storing requests for slaves.
slaveRequestsKey :: RedisInfo -> LKey
slaveRequestsKey r = LKey $ Key $ redisKeyPrefix r <> "slave-requests"

-- 'Channel' which is used to notify idle workers that there is a new
-- client request or slave request available.
requestsChannel :: RedisInfo -> Channel
requestsChannel r = Channel $ redisKeyPrefix r <> "requests-channel"

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
        sub = subscribeToResponses r (clientInfo cvs) (clientSubscribed cvs)
    control $ \restore -> withAsync (restore checker) $ \_ -> restore $
        sub $ \rid -> do
            -- Lookup the handler before fetching / deleting the response,
            -- as the message may get delivered to multiple clients.
            let lookupAndRemove handler = return (handler, Remove)
            mhandler <- atomically $
                SM.focus lookupAndRemove rid (clientDispatch cvs)
            case mhandler of
                -- TODO: Is a mere warning sufficient? Perhaps we need
                -- guarantees about uniqueness of back channel, and number
                -- of times a response is yielded, in order to have
                -- guarantees about delivery.
                Nothing -> $logWarnS "JobQueue" $
                    "Couldn't find handler to deal with response to " <>
                    tshow rid
                Just handler -> do
                    response <- readResponse r rid
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
        Nothing -> do
            notifyRequestAvailable r
            atomically $ SM.focus addOrExtend k (clientDispatch cvs)
        Just response -> do
            void $ fork $ runHandler (decode (fromStrict response))
  where
    addOrExtend Nothing = return ((), Replace runHandler)
    addOrExtend (Just old) = return ((), Replace (\x -> old x >> runHandler x))
    runHandler response =
        catchAny (handler response) $ \ex ->
            $logErrorS "JobQueue" $ "jobQueueRequest' callbackHandler: " ++ tshow ex

-- * Heartbeats

-- | This listens for a notification telling the worker to send a
-- heartbeat.  In this case, that means the worker needs to remove its
-- key from a Redis set.  If this doesn't happen in a timely fashion,
-- then the worker will be considered to be dead, and its work items
-- get re-enqueued.
--
-- The @TVar Bool@ is changed to 'True' once the subscription is made
-- and the 'WorkerId' has been added to the list of active workers.
sendHeartbeats
    :: MonadConnect m => RedisInfo -> WorkerInfo -> TVar Bool -> m void
sendHeartbeats r (WorkerInfo wid _) ready = do
    let sub = subscribe (heartbeatChannel r :| [])
    withSubscriptionsWrapped (redisConnectInfo r) (sub :| []) $ \msg ->
        case msg of
            Subscribe {} -> do
                run_ r $ sadd (heartbeatActiveKey r) (unWorkerId wid :| [])
                atomically $ writeTVar ready True
            Unsubscribe {} ->
                atomically $ writeTVar ready False
            Message {} ->
                run_ r $ srem (heartbeatInactiveKey r) (unWorkerId wid :| [])

-- | Periodically check worker heartbeats.  This uses
-- 'periodicActionWrapped' to share the responsibility of checking the
-- heartbeats amongst multiple client servers.  All invocations of
-- this should use the same time interval.
checkHeartbeats
    :: MonadConnect m => RedisInfo -> Seconds -> m void
checkHeartbeats r ivl =
    periodicActionWrapped (redisConnection r) (heartbeatTimeKey r) ivl $ do
        -- Check if the last iteration of this heartbeat check ran
        -- successfully.  If it did, then we can use the contents of
        -- the inactive list.  The flag also gets set to False here,
        -- such that if a failure happens in the middle, the next run
        -- will know to not use the data.
        functioning <- fmap (fmap (decode . fromStrict)) $
            run r $ getset (heartbeatFunctioningKey r) (toStrict (encode False))
        inactive <- if functioning == Just True
            then do
                -- Fetch the list of inactive workers and move their
                -- jobs back to the requests queue.  If we re-enqueued
                -- some requests, then send out a notification about
                -- it.
                inactive <- run r $ smembers (heartbeatInactiveKey r)
                reenqueedSome <- any id <$>
                    mapM (handleWorkerFailure r . WorkerId) inactive
                when reenqueedSome $ notifyRequestAvailable r
                return inactive
            else do
                -- The reasoning here is that if the last heartbeat
                -- check failed, it might have enqueued requests.  We
                -- check if there are any, and if so, send a
                -- notification.
                requestsCount <- run r $ llen (requestsKey r)
                when (requestsCount > 0) $ notifyRequestAvailable r
                return []
        -- Remove the inactive workers from the list of workers.
        mapM_ (run_ r . srem (heartbeatActiveKey r)) (nonEmpty inactive)
        -- Populate the list of inactive workers for the next
        -- heartbeat.
        workers <- run r $ smembers (heartbeatActiveKey r)
        run_ r $ del (unSKey (heartbeatInactiveKey r) :| [])
        mapM_ (run_ r . sadd (heartbeatInactiveKey r)) (nonEmpty workers)
        -- Ask all of the workers to remove their IDs from the inactive
        -- list.
        -- TODO: Remove this threadDelay (see #26)
        liftIO $ threadDelay (100 * 1000)
        run_ r $ publish (heartbeatChannel r) ""
        -- Record that the heartbeat check was successful.
        run_ r $ set (heartbeatFunctioningKey r) (toStrict (encode True)) []

handleWorkerFailure
    :: (MonadCommand m, MonadLogger m) => RedisInfo -> WorkerId -> m Bool
handleWorkerFailure r wid = do
    $logErrorS "JobQueue" $ tshow wid <>
        " failed to respond to heartbeat.  Re-enquing its items."
    let k = activeKey r wid
    requests <- run r $ lrange k 0 (-1)
    mapM_ (run_ r . rpush (requestsKey r)) (nonEmpty requests)
    -- Delete the active list after re-enquing is successful.
    -- This way, we can't lose data.
    run_ r $ del (unLKey (activeKey r wid) :| [])
    return $ isJust (nonEmpty requests)

-- | This is used to remove the worker from the set of workers checked
-- for heartbeats.  It's used when a master becomes a slave.
--
-- It throws a 'WorkStillInProgress' exception if there is enqueued
-- work, so callers should ensure that this isn't the case.  In order
-- to ensure that work isn't enqueued to the deactivated worker, it
-- also replaces the list with a dummy string value.  This will cause
-- 'popRequest' to yield 'Nothing'.
deactivateWorker :: (MonadCommand m, MonadThrow m)
                 => RedisInfo -> WorkerInfo -> m ()
deactivateWorker r (WorkerInfo wid _) = do
    activeCount <- run r $ llen (activeKey r wid)
    when (activeCount /= 0) $ throwM (WorkStillInProgress wid)
    run_ r $ srem (heartbeatActiveKey r) (unWorkerId wid :| [])

-- | Given a name to start with, this finds a 'WorkerId' which has
-- never been used before.  It also adds the new 'WorkerId' to the set
-- of all worker IDs.
getUnusedWorkerId
    :: MonadCommand m => RedisInfo -> ByteString -> m WorkerId
getUnusedWorkerId r initial = go (0 :: Int)
  where
    go n = do
        let toWord8 = fromIntegral . fromEnum
        postfix <- liftIO $ replicateM n $ randomRIO (toWord8 'a', toWord8 'z')
        let k | n == 0 = initial
              | otherwise = initial <> "-" <> postfix
        numberAdded <- run r $ sadd (workersKey r) (k :| [])
        if numberAdded == 0
            then go (n+1)
            else return (WorkerId k)

-- * Functions to compute Redis keys

heartbeatInactiveKey, heartbeatActiveKey, workersKey :: RedisInfo -> SKey
-- A set of 'WorkerId' who have not yet removed their keys (indicating
-- that they're still alive and responding to heartbeats).
heartbeatInactiveKey r = SKey $ Key $ redisKeyPrefix r <> "heartbeat:inactive"
-- A set of 'WorkerId's that are currently thought to be running.
heartbeatActiveKey r = SKey $ Key $ redisKeyPrefix r <> "heartbeat:active"
-- A set of all 'WorkerId's that have ever been known.
workersKey r = SKey $ Key $ redisKeyPrefix r <> "heartbeat:workers"

-- Stores a "Data.Binary" encoded 'Bool'.
heartbeatFunctioningKey :: RedisInfo -> VKey
heartbeatFunctioningKey r = VKey $ Key $ redisKeyPrefix r <> "heartbeat:functioning"

-- Channel used for requesting that the workers remove their
-- 'WorkerId' from the set at 'heartbeatInactiveKey'.
heartbeatChannel :: RedisInfo -> Channel
heartbeatChannel r = Channel $ redisKeyPrefix r <> "heartbeat:channel"

-- Prefix used for the 'periodicActionWrapped' invocation, which is
-- used to share the responsibility of periodically checking
-- heartbeats.
heartbeatTimeKey :: RedisInfo -> PeriodicPrefix
heartbeatTimeKey r = PeriodicPrefix $ redisKeyPrefix r <> "heartbeat:time"

-- * Utilities

withRedis' :: MonadConnect m => WorkerConfig -> (RedisInfo -> m a) -> m a
withRedis' config = withRedis (workerKeyPrefix config) (workerConnectInfo config)

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

asyncLifted :: MonadBaseControl IO m => m a -> m (Async (StM m a))
asyncLifted f = liftBaseWith $ \restore -> async (restore f)
