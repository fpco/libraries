{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE ExistentialQuantification #-}

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
import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (Async, async, withAsync, cancel, cancelWith, waitEither)
import Control.Monad.Logger (logErrorS, logInfoS, logDebugS)
import Control.Monad.Trans.Control (MonadBaseControl, liftBaseWith)
import Data.Binary (Binary, encode)
import Data.ConcreteTypeRep (fromTypeRep)
import Data.List.NonEmpty (NonEmpty((:|)))
import Data.Streaming.Network (clientSettingsTCP, runTCPServer, serverSettingsTCP, setAfterBind)
import Data.Streaming.NetworkMessage (NMSettings, Sendable, defaultNMSettings, setNMHeartbeat)
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
      -- ^ The time interval between heartbeats sent to the server.
      -- This must be substantially lower than the rate at which they
      -- are checked.
    , workerConnectionHeartbeatIvlMicros :: Int
      -- ^ The time interval between heartbeats sent between masters
      -- and slaves.
    , workerCancellationCheckIvl :: Seconds
      -- ^ Number of seconds between the worker checking if it's been
      -- cancelled.
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
    , workerConnectionHeartbeatIvlMicros = 1000 * 1000 * 2
    , workerCancellationCheckIvl = Seconds 10
    }

-- | This runs a job queue worker.  The data that's being sent between
-- the servers all need to be 'Sendable' so that they can be
-- serialized, and so that agreement on types can be checked at
-- runtime.
jobQueueWorker
    :: forall m request response payload result.
       ( MonadConnect m
       , Sendable request
       , Typeable request
       , Sendable response
       , Typeable response
       , Sendable payload
       , Sendable result
       )
    => WorkerConfig
    -- ^ This is run once per worker, if it ever becomes a master
    -- server.  The data is then sent to the slaves.
    -> (payload -> IO result)
    -- ^ This is the computation function run by slaves. It computes
    -- @result@ from @payload@.
    -> (RedisInfo -> MasterConnectInfo -> RequestId -> request -> WorkQueue payload result -> IO response)
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
    let name = "worker-" ++ tshow (unWorkerId wid)
    withLogTag (LogTag name) $ withRedis' config $ \redis -> do
        nmSettings <-
            setNMHeartbeat (workerConnectionHeartbeatIvlMicros config)  <$>
            liftIO defaultNMSettings
        jobQueueWorkerInternal JQWParams {..}

-- Parameters for helper functions
data JQWParams request response payload result =
   JQWParams
       { config :: WorkerConfig
       , calc :: payload -> IO result
       , inner :: RedisInfo -> MasterConnectInfo -> RequestId -> request -> WorkQueue payload result -> IO response
       , nmSettings :: NMSettings
       , wid :: WorkerId
       , name :: Text
       , redis :: RedisInfo
       }

-- datatype used by 'jobQueueWorkerInternal'.
data MaybeWithSubscription
    -- | When 'loop' doesn't find any work, it's called again using
    -- this constructor as its argument.  When this happens, it will
    -- block on the 'MVar', waiting on the 'requestChannel'.  However,
    -- before blocking, it makes one more attempt at popping work,
    -- because some work may have come in before the subscription was
    -- established.
    = WithSubscription (MVar ()) (IO ())
    -- | This constructor indicates that 'loop' should just check for
    -- work, without subscribing to 'requestChannel'. When the worker
    -- successfully popped work last time, there's no reason to
    -- believe there isn't more work immediately available.
    | NoSubscription
    deriving (Typeable)

-- Here's how this works:
--
-- 1) The worker starts out as neither a slave or master.
--
-- 2) If there is a pending 'MasterConnectInfo', then it connects to
-- the specified master and starts working.
--
-- 3) If there is a 'JobRequest', then it becomes a master and runs
-- @inner@.
--
-- 4) If there are neither, then it queries for requests once again,
-- this time with a subscription to the 'requestsChannel'.
--
-- Not having this subscription the first time through is an
-- optimization - it allows us to save redis connections and save
-- redis the overhead of notifying many workers.  When the system
-- isn't saturated in work, we don't really care about performance,
-- and so it doesn't matter that we have so many connections to redis
-- + it needs to notify many workers.
jobQueueWorkerInternal
    :: forall m request response payload result.
       ( MonadConnect m
       , Sendable request, Typeable request, Sendable response, Typeable response
       , Sendable payload, Sendable result
       )
    => JQWParams request response payload result
    -> m ()
jobQueueWorkerInternal jqwp@JQWParams {..} =
    withLogTag (LogTag name) $ withHeartbeats $ loop NoSubscription
  where
    loop :: MaybeWithSubscription -> Async () -> m ()
    loop mws heartbeatThread = do
        -- If there's slave work to be done, then do it.
        mslave <- popSlaveRequest redis
        case mslave of
            Just slave -> liftBaseWith $ \restore -> do
                initCalledRef <- newIORef False
                let init = void $ restore $ do
                        liftIO $ writeIORef initCalledRef True
                        unsubscribeToRequests mws
                        -- Now that we're a slave, doing a heartbeat check is
                        -- no longer necessary, so deactivate the check, and
                        -- kill the heartbeat thread.
                        deactivateHeartbeats redis wid
                        liftIO $ cancel heartbeatThread
                void $ restore $ do
                    becomeSlave jqwp slave init
                    -- Restart the heartbeats thread before re-entering
                    -- the loop, if it was stopped.
                    initCalled <- readIORef initCalledRef
                    if initCalled
                        then withHeartbeats (loop NoSubscription)
                        else loop mws heartbeatThread
            Nothing -> do
                -- There isn't any slave work, so instead check if
                -- there's a job request, and become a master if there
                -- is one.  If our server dies, then the heartbeat
                -- code will re-enqueue the request.
                prr <- popRequest redis wid
                case prr of
                    RequestAvailable k req -> do
                        unsubscribeToRequests mws
                        send k =<< becomeMaster jqwp k req
                        loop NoSubscription heartbeatThread
                    NoRequestAvailable -> case mws of
                        -- If we weren't subscribed to
                        -- 'requestChannel', then the next iteration
                        -- should be subscribed.
                        NoSubscription -> do
                            $logDebugS "JobQueue" "Re-running requests, with subscription"
                            (notified, unsub) <- subscribeToRequests redis
                            let mws' = WithSubscription notified unsub
                            loop mws' heartbeatThread
                        -- If we are subscribed to 'requestChannel',
                        -- then block waiting for a notification.
                        WithSubscription notified _ -> do
                            $logDebugS "JobQueue" "Waiting for request notification"
                            takeMVar notified
                            $logDebugS "JobQueue" "Got notified of an available request"
                            loop mws heartbeatThread
                    -- Let the client know about missing requests.
                    RequestMissing k -> do
                        send k (Left (RequestMissingException k))
                        loop mws heartbeatThread
                    -- Recover in circumstances where this worker
                    -- still functions, but failed to send its
                    -- heartbeat in time.
                    HeartbeatFailure -> do
                        $logInfoS "JobQueue" $ tshow wid <> " recovering from heartbeat failure"
                        recoverFromHeartbeatFailure redis wid
                        -- Restart the heartbeat thread, which re-adds
                        -- the worker to the list of active workers.
                        liftIO $ cancel heartbeatThread
                        withHeartbeats $ loop mws
    send :: RequestId
         -> Either DistributedJobQueueException response
         -> m ()
    send k result = sendResponse redis expiry wid k encoded
      where
        expiry = workerResponseDataExpiry config
        encoded = toStrict (encode result)
    -- Heartbeats get their own redis connection, as this way it's
    -- less likely that they'll fail due to the main redis connection
    -- transferring lots of data.
    withHeartbeats = do
        let logTag = LogTag (name <> "-heartbeat")
        withAsyncLifted $ withLogTag logTag $ withRedis' config $ \r ->
            sendHeartbeats r (workerHeartbeatSendIvl config) wid

becomeSlave
    :: ( MonadConnect m
       , Sendable request, Sendable response
       , Sendable payload, Sendable result
       )
    => JQWParams request response payload result
    -> MasterConnectInfo
    -> IO ()
    -> m ()
becomeSlave JQWParams {..} mci init = do
    $logInfoS "JobQueue" (tshow wid ++ " becoming slave of " ++ tshow mci)
    let settings = clientSettingsTCP (mciPort mci) (mciHost mci)
    eres <- try $ runSlave settings nmSettings (\() -> init) (\() -> calc)
    case eres of
        -- This indicates that the slave couldn't connect.
        Left err@(IOError {}) ->
            $logInfoS "JobQueue" $
                "Failed to connect to master, with " <>
                tshow mci <>
                ".  This probably isn't an issue - the master likely " <>
                "already finished or died.  Here's the exception: " <>
                tshow err
        Right (Right ()) ->
            $logInfoS "JobQueue" (tshow wid ++ " done being slave of " ++ tshow mci)
        Right (Left err) -> do
            $logErrorS "JobQueue" $ "Slave threw exception: " ++ tshow err
            liftIO $ throwIO err

becomeMaster
    :: forall m request response payload result.
       ( MonadConnect m
       , Sendable request, Typeable request, Sendable response, Typeable response
       , Sendable payload, Sendable result
       )
    => JQWParams request response payload result
    -> RequestId
    -> ByteString
    -> m (Either DistributedJobQueueException response)
becomeMaster JQWParams {..} k req = do
    $logInfoS "JobQueue" (tshow wid ++ " becoming master, for " ++ tshow k)
    let requestType = fromTypeRep (typeRep (Proxy :: Proxy request))
        responseType = fromTypeRep (typeRep (Proxy :: Proxy response))
    boundPort <- newEmptyMVar
    let ss = setAfterBind
            (putMVar boundPort . fromIntegral <=< socketPort)
            (serverSettingsTCP (workerPort config) "*")
    eres <- tryAny $ do
       JobRequest {..} <- decodeOrThrow "jobQueueWorker" req
       when (jrRequestType /= requestType ||
             jrResponseType /= responseType) $ do
           liftIO $ throwIO TypeMismatch
               { expectedResponseType = responseType
               , actualResponseType = jrResponseType
               , expectedRequestType = requestType
               , actualRequestType = jrRequestType
               }
       decoded <- decodeOrThrow "jobQueueWorker" jrBody
       watchForCancel redis k (workerCancellationCheckIvl config) $
           withMaster (runTCPServer ss) nmSettings () $ \queue ->
               withLocalSlaves queue (workerMasterLocalSlaves config) calc $ do
                   port <- readMVar boundPort
                   let mci = MasterConnectInfo (workerHostName config) port
                   liftIO $ inner redis mci k decoded queue
    case eres of
        Left err -> do
            $logErrorS "JobQueue" $
                tshow k <> " failed with " <> tshow err
            return (Left (wrapException err))
        Right x -> do
            $logInfoS "JobQueue" (tshow wid ++ " done being master")
            return (Right x)

watchForCancel :: MonadConnect m => RedisInfo -> RequestId -> Seconds -> m a -> m a
watchForCancel r k ivl f = do
    -- FIXME: avoid this MVar stuff, once we have good lifted async.
    resultVar <- newEmptyMVar
    thread <- asyncLifted $ do
        result <- f
        putMVar resultVar result
    watcher <- asyncLifted $ forever $ do
        mres <- run r (get (cancelKey r k))
        liftIO $ case mres of
            Just res
                | res == cancelValue ->
                    cancelWith thread (RequestCanceledException k)
                | otherwise -> throwIO $ InternalJobQueueException
                    "Didn't get expected value at cancelKey."
            Nothing -> threadDelay (1000 * 1000 * fromIntegral (unSeconds ivl))
    res <- liftIO $ waitEither thread watcher
    case res of
        Left () -> do
            liftIO $ cancel watcher
            takeMVar resultVar
        -- FIXME: use 'absurd', once we have a good lifted async.
        Right () -> liftIO $ throwIO $ InternalJobQueueException
            "Impossible: the watcher returned"

-- | This subscribes to the requests notification channel. The yielded
-- @MVar ()@ is filled when we receive a notification. The yielded @IO
-- ()@ action unsubscribes from the channel.
--
-- When the connection is lost, at reconnect, the notification MVar is
-- also filled. This way, things will still work even if we missed a
-- notification.
subscribeToRequests
    :: MonadConnect m
    => RedisInfo
    -> m (MVar (), IO ())
subscribeToRequests redis = do
    -- When filled, 'ready' indicates that the subscription has been established.
    ready <- newEmptyMVar
    -- This is the MVar yielded by the function. It gets filled when a
    -- message is received on the channel.
    notified <- newEmptyMVar
    -- This stores the disconnection action provided by
    -- 'withSubscription'.
    disconnectVar <- newIORef (error "impossible: disconnectVar not initialized.")
    let handleConnect dc = do
            writeIORef disconnectVar dc
            void $ tryPutMVar notified ()
            void $ tryPutMVar ready ()
    void $ asyncLifted $ logNest "subscribeToRequests" $
        withSubscription redis (requestChannel redis) handleConnect $ \_ ->
            void $ tryPutMVar notified ()
    -- Wait for the subscription to connect before returning.
    takeMVar ready
    -- 'notified' also gets filled by 'handleConnect', since this is
    -- needed when a reconnection occurs. We don't want it to be filled
    -- for the initial connection, though, so we take it.
    takeMVar notified
    -- Since we waited for ready to be filled, disconnectVar must no
    -- longer contains its error value.
    unsub <- readIORef disconnectVar
    return (notified, unsub)

unsubscribeToRequests :: MonadConnect m => MaybeWithSubscription -> m ()
unsubscribeToRequests NoSubscription = return ()
unsubscribeToRequests (WithSubscription _ unsub) = liftIO unsub

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
getWorkerId = WorkerId . UUID.toASCIIBytes <$> UUID.nextRandom

asyncLifted :: MonadBaseControl IO m => m () -> m (Async ())
asyncLifted f = liftBaseWith $ \restore -> async (void (restore f))

withAsyncLifted :: MonadBaseControl IO m => m () -> (Async () -> m ()) -> m ()
withAsyncLifted f g = liftBaseWith $ \restore -> withAsync (void (restore f)) (void . restore . g)
