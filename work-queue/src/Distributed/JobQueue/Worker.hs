{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}

-- | This module provides the API used by job-queue workers.  See
-- "Distributed.JobQueue" for more info.
module Distributed.JobQueue.Worker
    ( WorkerConfig(..)
    , defaultWorkerConfig
    , jobQueueWorker
    , requestSlave
    -- * For internal usage by tests
    , SlaveRequest(..)
    , slaveRequestsKey
    ) where

import           ClassyPrelude
import           Control.Concurrent.Async (Async, async, link, race)
import           Control.Concurrent.STM (check)
import           Control.Monad.Logger (logErrorS, logDebugS)
import           Control.Monad.Trans.Control (liftBaseWith, MonadBaseControl, StM)
import           Data.Binary (Binary, encode)
import           Data.ConcreteTypeRep (fromTypeRep)
import           Data.List.NonEmpty (NonEmpty((:|)))
import           Data.Streaming.Network (clientSettingsTCP, runTCPServer, serverSettingsTCP)
import           Data.Streaming.NetworkMessage (Sendable, defaultNMSettings)
import           Data.Typeable (typeRep,)
import           Data.UUID as UUID
import           Data.UUID.V4 as UUID
import           Data.WorkQueue
import           Distributed.JobQueue.Heartbeat (sendHeartbeats, deactivateWorker)
import           Distributed.JobQueue.Shared
import           Distributed.RedisQueue
import           Distributed.RedisQueue.Internal
import           Distributed.WorkQueue
import           FP.Redis
import           GHC.IO.Exception (IOException(IOError), ioe_type, IOErrorType(NoSuchThing))

-- | Configuration of a 'jobQueueWorker'.
data WorkerConfig = WorkerConfig
    { workerResponseDataExpiry :: Seconds
      -- ^ How many seconds the response data should be kept in redis.
      -- The longer it's kept in redis, the more opportunity there is to
      -- eliminate redundant work, if identical requests are made.  A
      -- longer expiry also allows more time between sending a
      -- response notification and the client reading its data.
    , workerKeyPrefix :: ByteString
      -- ^ Prefix used for redis keys.
    , workerConnectInfo :: ConnectInfo
      -- ^ Info used to connect to the redis server.
    , workerHostName :: ByteString
      -- ^ The host name sent to slaves, so that they can connect to
      -- this worker when it's acting as a master.
    , workerPort :: Int
      -- ^ Which port to use when the worker is acting as a master.
    , workerMasterLocalSlaves :: Int
      -- ^ How many local slaves a master server should run.
    } deriving (Typeable)

-- | Given a redis key prefix and redis connection information, builds
-- a default 'WorkerConfig'.
--
-- This config has response data expire every hour, and configures
-- masters to have one local slave.  The default of one local slave is
-- because there are cases in which masters may be starved of slaves.
-- For example, if a bunch of work items come in and they all
-- immediately become masters, then progress won't be made without
-- local slaves.
defaultWorkerConfig :: ByteString -> ConnectInfo -> ByteString -> Int -> WorkerConfig
defaultWorkerConfig prefix ci hostname port = WorkerConfig
    { workerResponseDataExpiry = Seconds 3600
    , workerKeyPrefix = prefix
    , workerConnectInfo = ci
    , workerHostName = hostname
    , workerPort = port
    , workerMasterLocalSlaves = 1
    }

-- | Internal datatype used by 'jobQueueWorker'
data SubscribeOrCheck
    -- | When 'loop' doesn't find any work, it's called again using
    -- this constructor as its argument.  When this happens, it will
    -- block on the 'MVar', waiting on the 'requestChannel'.  However,
    -- before blocking, it makes one more attempt at popping work,
    -- because some work may have come in before the subscription was
    -- established.
    = SubscribeToRequests (MVar ()) (IORef Connection)
    -- | This constructor indicates that 'loop' should just check for
    -- work, without subscribing to 'requestChannel'. When the worker
    -- successfully popped work last time, there's no reason to
    -- believe there isn't more work immediately available.
    | CheckRequests
    deriving (Typeable)

-- | This runs a job queue worker.  The data that's being sent between
-- the servers all need to be 'Sendable' so that they can be
-- serialized, and so that agreement on types can be checked at
-- runtime.
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
    -- ^ This is the computation function run by slaves. It computes
    -- @result@ from @payload@.
    -> (initialData -> RedisInfo -> request -> WorkQueue payload result -> IO response)
    -- ^ This function runs on the master after it's received a
    -- request. It's expected that the master will use this request to
    -- enqueue work items on the provided 'WorkQueue'.  The results of
    -- this can then be accumulated into a @response@ to be sent back
    -- to the client.
    --
    -- It's expected that this function will use 'queueItem',
    -- 'mapQueue', or related functions to enqueue work which is
    -- dispatched to the slaves.
    -> m ()
jobQueueWorker config init calc inner = withRedis' config $ \redis -> do
    -- Here's how this works:
    --
    -- 1) The worker starts out as neither a slave or master.
    --
    -- 2) If there is a pending 'SlaveRequest', then it connects to
    -- the specified master and starts working.
    --
    -- 3) If there is a 'JobRequest', then it becomes a master and
    -- runs @inner@.
    --
    -- 4) If there are neither, then it queries for requests once
    -- again, this time with a subscription to the 'requestsChannel'.
    --
    -- Not having this subscription the first time through is an
    -- optimization - it allows us to save redis connections and save
    -- redis the overhead of notifying many workers.  When the system
    -- isn't saturated in work, we don't really care about
    -- performance, and so it doesn't matter that we have so many
    -- connections to redis + it needs to notify many workers.
    wid <- liftIO getWorkerId
    initialDataRef <- newIORef Nothing
    nmSettings <- liftIO defaultNMSettings
    -- heartbeatsReady is used to track whether we're subscribed to
    -- redis heartbeats or not.  We must wait for this subscription
    -- before dequeuing a 'JobRequest', because otherwise the
    -- heartbeat checking won't function, and the request could be
    -- lost.
    heartbeatsReady <- liftIO $ newTVarIO False
    let loop :: SubscribeOrCheck -> m ()
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
                    -- is one.  If our server dies, then the heartbeat
                    -- code will re-enqueue the request.
                    mreq <- popRequest redis wid
                    case (mreq, soc) of
                        (Just req, _) -> do
                            unsubscribeToRequests soc
                            becomeMaster req
                            loop CheckRequests
                        (Nothing, CheckRequests) -> do
                            loop =<< subscribeToRequests
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
            let subs = subscribe (requestChannel redis :| []) :| []
            thread <- asyncLifted $
                withSubscriptionsExConn (redisConnectInfo redis) subs $ \conn -> do
                    writeIORef connVar conn
                    return $ trackSubscriptionStatus ready $ \_ _ ->
                        void $ tryPutMVar notified ()
            liftIO $ link thread
            atomically $ check =<< readTVar ready
            return $ SubscribeToRequests notified connVar
        unsubscribeToRequests CheckRequests = return ()
        unsubscribeToRequests (SubscribeToRequests _ connVar) =
            disconnect =<< readIORef connVar
        becomeSlave :: SlaveRequest -> m ()
        becomeSlave sr = do
            $logDebugS "JobQueue" "Becoming slave"
            let settings = clientSettingsTCP (srPort sr) (srHost sr)
            eres <- try $ runSlave settings nmSettings calc
            case eres of
                Right () -> return ()
                -- This indicates that the slave couldn't connect.
                Left (IOError { ioe_type = NoSuchThing }) ->
                    $logDebugS "JobQueue" $
                        "Failed to connect to master, with " <>
                        tshow sr <>
                        ".  This probably isn't an issue - the master likely already finished or died."
                Left err -> throwM err
        becomeMaster :: (RequestInfo, ByteString) -> m ()
        becomeMaster (ri, req) = do
            $logDebugS "JobQueue" "Becoming master"
            initialData <- do
                minitialData <- readIORef initialDataRef
                case minitialData of
                    Just initialData -> return initialData
                    Nothing -> do
                        initialData <- liftIO init
                        writeIORef initialDataRef (Just initialData)
                        return initialData
            settings <- liftIO defaultNMSettings
            eres <- tryAny $ withMaster (runTCPServer ss) settings initialData $ \queue ->
                withLocalSlaves queue (workerMasterLocalSlaves config) (calc initialData) $ do
                    JobRequest {..} <- decodeOrThrow "jobQueueWorker" req
                    when (jrRequestType /= requestType ||
                          jrResponseType /= responseType) $
                        throwM TypeMismatch
                            { expectedResponseType = responseType
                            , actualResponseType = jrResponseType
                            , expectedRequestType = requestType
                            , actualRequestType = jrRequestType
                            }
                    decoded <- decodeOrThrow "jobQueueWorker" jrBody
                    liftIO $ inner initialData redis decoded queue
            result <-
                case eres of
                    Left err -> do
                        $logErrorS "JobQueue" $
                            tshow ri <> " failed with " <> tshow err
                        return (Left (wrapException err))
                    Right x -> return (Right x)
            let expiry = workerResponseDataExpiry config
                encoded = toStrict (encode result)
            sendResponse redis expiry wid ri encoded
            deactivateWorker redis wid
        ss = serverSettingsTCP (workerPort config) "*"
        requestType = fromTypeRep (typeRep (Nothing :: Maybe request))
        responseType = fromTypeRep (typeRep (Nothing :: Maybe response))
        start = do
            atomically $ check =<< readTVar heartbeatsReady
            loop CheckRequests
        heartbeats =
            withRedis' config $ \r -> sendHeartbeats r wid heartbeatsReady
    start `raceLifted` heartbeats

-- * Slave Requests

-- | Hostname and port of the master the slave should connect to.  The
-- 'Binary' instance for this is used to serialize this info to the
-- list stored at 'slaveRequestsKey'.
data SlaveRequest = SlaveRequest
    { srHost :: ByteString
    , srPort :: Int
    }
    deriving (Generic, Show, Typeable)

instance Binary SlaveRequest

-- | This command is used by the master to request that a slave
-- connect to it.
--
-- This currently has the following caveats:
--
--   (1) The slave request is not guaranteed to be fulfilled, for
--   multiple reasons:
--
--       - All workers may be busy doing other work.
--
--       - The queue of slave requests might already be long.
--
--       - A worker might pop the slave request and then shut down
--       before establishing a connection to the master.
--
--   (2) A master may get slaves connecting to it that it didn't
--   request.  Here's why:
--
--       - When a worker stops being a master, it does not remove its
--       pending slave requests from the list.  This means they can
--       still be popped by workers.  Usually this means that the
--       worker will attempt to connect, fail, and find something else
--       to do.  However, in the case that the server becomes a master
--       again, it's possible that a slave will pop its request.
--
-- These caveats are not necessitated by any aspect of the overall
-- design, and may be resolved in the future.
requestSlave
    :: MonadCommand m
    => WorkerConfig
    -> RedisInfo
    -> m ()
requestSlave config r = do
    let request = SlaveRequest (workerHostName config) (workerPort config)
        encoded = toStrict (encode request)
    runCommand_ (redisConnection r) $ lpush (slaveRequestsKey r) (encoded :| [])
    notifyRequestAvailable r

-- | This command is used by a worker to fetch a 'SlaveRequest', if
-- one is available.
popSlaveRequest :: MonadCommand m => RedisInfo -> m (Maybe SlaveRequest)
popSlaveRequest r =
    runCommand (redisConnection r) (rpop (slaveRequestsKey r)) >>=
    mapM (decodeOrThrow "popSlaveRequest")

-- | Key used to for storing requests for slaves.
slaveRequestsKey :: RedisInfo -> LKey
slaveRequestsKey r = LKey $ Key $ redisKeyPrefix r <> "slave-requests"

-- * Utilities

withRedis' :: MonadConnect m => WorkerConfig -> (RedisInfo -> m a) -> m a
withRedis' config = withRedis (workerKeyPrefix config) (workerConnectInfo config)

getWorkerId :: IO WorkerId
getWorkerId = WorkerId . toStrict . UUID.toByteString <$> UUID.nextRandom

-- Note: Ideally this would yield (Either a b)
--
-- We don't need that for the usage here, though.
raceLifted :: MonadBaseControl IO m => m a -> m b -> m ()
raceLifted f g =
    liftBaseWith $ \restore ->
        void $ restore f `race` restore g

asyncLifted :: MonadBaseControl IO m => m a -> m (Async (StM m a))
asyncLifted f = liftBaseWith $ \restore -> async (restore f)
