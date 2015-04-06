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
    , MasterConnectInfo(..)
    , defaultWorkerConfig
    , jobQueueWorker
    , requestSlave
    -- * For internal usage by tests
    , slaveRequestsKey
    ) where

import ClassyPrelude
import Control.Concurrent.Async (Async, async, link, withAsync, cancel)
import Control.Concurrent.STM (check)
import Control.Monad.Logger (logErrorS, logInfoS)
import Control.Monad.Trans.Control (MonadBaseControl, liftBaseWith)
import Data.Binary (Binary, encode)
import Data.ConcreteTypeRep (fromTypeRep)
import Data.List.NonEmpty (NonEmpty((:|)))
import Data.Streaming.Network (clientSettingsTCP, runTCPServer, serverSettingsTCP, setAfterBind)
import Data.Streaming.NetworkMessage (Sendable, defaultNMSettings)
import Data.Typeable (Proxy(..), typeRep)
import Data.UUID as UUID
import Data.UUID.V4 as UUID
import Data.WorkQueue
import Distributed.JobQueue.Heartbeat
import Distributed.JobQueue.Shared
import Distributed.RedisQueue
import Distributed.RedisQueue.Internal
import Distributed.WorkQueue
import FP.Redis
import FP.ThreadFileLogger
import GHC.IO.Exception (IOException(IOError))
import Network.Socket (socketPort)

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
      -- If the port is set to 0, then a free port is automatically
      -- chosen by the (unix) system.
    , workerMasterLocalSlaves :: Int
      -- ^ How many local slaves a master server should run.
    , workerHeartbeatSendIvl :: Seconds
      -- ^ The time interval between heartbeata are sent to the
      -- server.  This must be substantially lower than the rate at
      -- which they are checked.
    } deriving (Typeable)

-- | Given a redis key prefix and redis connection information, builds
-- a default 'WorkerConfig'.
--
-- The defaults are:
--
--     * Heartbeats are sent every 15 seconds.  This should be
--     reasonable, given the client default of checking every 30
--     seconds.  However, if the client uses a different heartbeat
--     time, this should be changed.
--
--     Aside: This isn't ideal.  Ideally, requests from the client
--     would let the worker know how often to send heartbeats.  We
--     can't do that, though, because the heartbeats need to be sent
--     before the worker even has any work.  This is because
--     'rpoplpush' is used to take work and move it to the worker's
--     work list.
--
--     * Response data expire every hour.
--
--     * One local slave. This is because there are cases in which
--     masters may be starved of slaves. For example, if a bunch of
--     work items come in and they all immediately become masters,
--     then progress won't be made without local slaves.
--
--     * 'workerPort' is set to 0 by default, which means the port is
--     allocated dynamically (on most unix systems).
defaultWorkerConfig :: ByteString -> ConnectInfo -> ByteString -> WorkerConfig
defaultWorkerConfig prefix ci hostname = WorkerConfig
    { workerResponseDataExpiry = Seconds 3600
    , workerKeyPrefix = prefix
    , workerConnectInfo = ci
    , workerHostName = hostname
    , workerPort = 0
    , workerMasterLocalSlaves = 1
    , workerHeartbeatSendIvl = Seconds 15
    }

-- | Internal datatype used by 'jobQueueWorker'.
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
    :: forall m request response payload result.
       ( MonadConnect m
       , Sendable request
       , Sendable response
       , Sendable payload
       , Sendable result
       )
    => WorkerConfig
    -- ^ This is run once per worker, if it ever becomes a master
    -- server.  The data is then sent to the slaves.
    -> (payload -> IO result)
    -- ^ This is the computation function run by slaves. It computes
    -- @result@ from @payload@.
    -> (RedisInfo -> MasterConnectInfo -> request -> WorkQueue payload result -> IO response)
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
jobQueueWorker config calc inner = do
  wid <- liftIO getWorkerId
  let name = "worker-" ++ omap replaceChar (tshow (unWorkerId wid))
      replaceChar c | c `elem` ['\\', '/', '.', '\"'] = '_'
      replaceChar c = c
  withLogTag (LogTag name) $ withRedis' config $ \redis -> do
    -- Here's how this works:
    --
    -- 1) The worker starts out as neither a slave or master.
    --
    -- 2) If there is a pending 'MasterConnectInfo', then it connects to
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
    nmSettings <- liftIO defaultNMSettings
    let loop :: SubscribeOrCheck -> Async () -> m ()
        loop soc heartbeatThread = do
            -- If there's slave work to be done, then do it.
            mslave <- popSlaveRequest redis
            case mslave of
                Just slave -> do
                    unsubscribeToRequests soc
                    -- Now that we're a slave, doing a heartbeat check
                    -- is no longer necessary, so deactivate the
                    -- check, and kill the heartbeat thread.
                    deactivateHeartbeats redis wid
                    liftIO $ cancel heartbeatThread
                    becomeSlave slave
                    -- Restart the heartbeats thread before
                    -- re-entering the loop.
                    withHeartbeats $ loop CheckRequests
                Nothing -> do
                    -- There isn't any slave work, so instead check if
                    -- there's a job request, and become a master if
                    -- there is one.  If our server dies, then the
                    -- heartbeat code will re-enqueue the request.
                    prr <- popRequest redis wid
                    case prr of
                        RequestAvailable ri req -> do
                            unsubscribeToRequests soc
                            becomeMaster (ri, req)
                            loop CheckRequests heartbeatThread
                        NoRequestAvailable -> case soc of
                            -- If we weren't subscribed to
                            -- 'requestChannel', then the next
                            -- iteration should be subscribed.
                            CheckRequests -> do
                                soc' <- subscribeToRequests
                                loop soc' heartbeatThread
                            -- If we are subscribed to 'requestChannel',
                            -- then block waiting for a notification.
                            SubscribeToRequests notified _ -> do
                                takeMVar notified
                                loop soc heartbeatThread
                        -- Let the client know about missing requests.
                        RequestMissing ri -> do
                            send ri (Left (RequestMissingException (riRequest ri)))
                            loop soc heartbeatThread
                        -- Recover in circumstances where this worker
                        -- still functions, but failed to send its
                        -- heartbeat in time.
                        HeartbeatFailure -> do
                            $logInfoS "JobQueue" "Recovering from heartbeat failure"
                            recoverFromHeartbeatFailure redis wid
                            -- Restart the heartbeat thread, which
                            -- re-adds the worker to the list of
                            -- active workers.
                            liftIO $ cancel heartbeatThread
                            withHeartbeats $ loop soc
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
            -- FIXME: why does this logging stuff cause issues?
            -- tag <- getLogTag
            thread <- asyncLifted $ do
                -- setLogTag tag
                -- logNest "subscribeToRequests" $
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
        becomeSlave :: MasterConnectInfo -> m ()
        becomeSlave mci = do
            $logInfoS "JobQueue" (tshow wid ++ " becoming slave of " ++ tshow mci)
            let settings = clientSettingsTCP (mciPort mci) (mciHost mci)
            eres <- try $ runSlave settings nmSettings (\() -> calc)
            case eres of
                -- This indicates that the slave couldn't connect.
                Left err@(IOError {}) ->
                    $logInfoS "JobQueue" $
                        "Failed to connect to master, with " <>
                        tshow mci <>
                        ".  This probably isn't an issue - the master likely " <>
                        "already finished or died.  Here's the exception: " <>
                        tshow err
                Right (Right ()) -> return ()
                Right (Left err) -> do
                    $logErrorS "JobQueue" $ "Slave threw exception: " ++ tshow err
                    liftIO $ throwIO err
        becomeMaster :: (RequestInfo, ByteString) -> m ()
        becomeMaster (ri, req) = do
            $logInfoS "JobQueue" (tshow wid ++ " becoming master")
            boundPort <- newEmptyMVar
            let ss = setAfterBind
                    (putMVar boundPort . fromIntegral <=< socketPort)
                    (serverSettingsTCP (workerPort config) "*")
            settings <- liftIO defaultNMSettings
            eres <- tryAny $ withMaster (runTCPServer ss) settings () $ \queue ->
                withLocalSlaves queue (workerMasterLocalSlaves config) calc $ do
                    JobRequest {..} <- decodeOrThrow "jobQueueWorker" req
                    when (jrRequestType /= requestType ||
                          jrResponseType /= responseType) $
                        liftIO $ throwIO TypeMismatch
                            { expectedResponseType = responseType
                            , actualResponseType = jrResponseType
                            , expectedRequestType = requestType
                            , actualRequestType = jrRequestType
                            }
                    decoded <- decodeOrThrow "jobQueueWorker" jrBody
                    port <- readMVar boundPort
                    let mci = MasterConnectInfo (workerHostName config) port
                    liftIO $ inner redis mci decoded queue
            result <-
                case eres of
                    Left err -> do
                        $logErrorS "JobQueue" $
                            tshow ri <> " failed with " <> tshow err
                        return (Left (wrapException err))
                    Right x -> return (Right x)
            send ri result
        send :: RequestInfo
             -> Either DistributedJobQueueException response
             -> m ()
        send ri result = sendResponse redis expiry wid ri encoded
          where
            expiry = workerResponseDataExpiry config
            encoded = toStrict (encode result)
        requestType = fromTypeRep (typeRep (Proxy :: Proxy request))
        responseType = fromTypeRep (typeRep (Proxy :: Proxy response))
        -- Heartbeats get their own redis connection, as this way it's
        -- less likely that they'll fail due to the main redis
        -- connection transferring lots of data.
        withHeartbeats = do
            let logTag = LogTag (name <> "-heartbeat")
            withAsyncLifted $ withLogTag logTag $ withRedis' config $ \r ->
                sendHeartbeats r (workerHeartbeatSendIvl config) wid
    withLogTag (LogTag name) $ withHeartbeats $ loop CheckRequests

-- * Slave Requests

-- | Hostname and port of the master the slave should connect to.  The
-- 'Binary' instance for this is used to serialize this info to the
-- list stored at 'slaveRequestsKey'.
data MasterConnectInfo = MasterConnectInfo
    { mciHost :: ByteString
    , mciPort :: Int
    }
    deriving (Generic, Show, Typeable)

instance Binary MasterConnectInfo

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
    => RedisInfo
    -> MasterConnectInfo
    -> m ()
requestSlave r mci = do
    let encoded = toStrict (encode mci)
    run_ r $ lpush (slaveRequestsKey r) (encoded :| [])
    notifyRequestAvailable r

-- | This command is used by a worker to fetch a 'MasterConnectInfo', if
-- one is available.
popSlaveRequest :: MonadCommand m => RedisInfo -> m (Maybe MasterConnectInfo)
popSlaveRequest r =
    run r (rpop (slaveRequestsKey r)) >>=
    mapM (decodeOrThrow "popSlaveRequest")

-- | Key used to for storing requests for slaves.
slaveRequestsKey :: RedisInfo -> LKey
slaveRequestsKey r = LKey $ Key $ redisKeyPrefix r <> "slave-requests"

-- * Utilities

withRedis' :: MonadConnect m => WorkerConfig -> (RedisInfo -> m a) -> m a
withRedis' config = withRedis (workerKeyPrefix config) (workerConnectInfo config)

getWorkerId :: IO WorkerId
getWorkerId = WorkerId . toStrict . UUID.toByteString <$> UUID.nextRandom

asyncLifted :: MonadBaseControl IO m => m () -> m (Async ())
asyncLifted f = liftBaseWith $ \restore -> async (void (restore f))

withAsyncLifted :: MonadBaseControl IO m => m () -> (Async () -> m ()) -> m ()
withAsyncLifted f g = liftBaseWith $ \restore -> withAsync (void (restore f)) (void . restore . g)
