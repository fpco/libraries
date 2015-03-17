{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE RecordWildCards #-}

-- | This module uses "Distributed.RedisQueue" atop
-- "Distributed.WorkQueue" to implement robust distribution of work
-- among many slaves.
--
-- Because it has heartbeats, it adds the guarantee that enqueued work
-- will not be lost until it's completed, even in the presence of
-- server failure.  A failure in Redis persistence can invalidate
-- this.
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
    -- * Main client API
    , ClientConfig(..)
    , defaultClientConfig
    , ClientVars(..)
    , withJobQueueClient
    , sendRequest
    , registerResponseCallback
    -- * Extra client APIs
    , newClientVars
    , jobQueueClient
    , jobQueueRequest
    , DistributedJobQueueException(..)
    -- * Internals, used by tests
    , slaveRequestsKey
    , SlaveRequest(..)
    ) where

import           ClassyPrelude
import           Control.Concurrent.Async (Async, async, link, withAsync, race)
import           Control.Concurrent.STM (check)
import           Control.Monad.Logger (MonadLogger, logErrorS, logDebugS)
import           Control.Monad.Trans.Control (control, liftBaseWith, MonadBaseControl, StM)
import           Data.Binary (Binary, encode)
import           Data.ConcreteTypeRep (ConcreteTypeRep, fromTypeRep)
import           Data.List.NonEmpty (NonEmpty((:|)), nonEmpty)
import           Data.Streaming.Network (clientSettingsTCP, runTCPServer, serverSettingsTCP)
import           Data.Streaming.NetworkMessage (NetworkMessageException, Sendable, defaultNMSettings)
import           Data.Text.Binary ()
import           Data.Typeable (typeRep, typeOf)
import           Data.UUID as UUID
import           Data.UUID.V4 as UUID
import           Data.WorkQueue
import           Distributed.RedisQueue
import           Distributed.RedisQueue.Internal
import           Distributed.WorkQueue
import           FP.Redis
import           FP.Redis.Mutex
import           Focus (Decision(Remove, Replace))
import           GHC.IO.Exception (IOException(IOError), ioe_type, IOErrorType(NoSuchThing))
import qualified STMContainers.Map as SM

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

-- | Hostname and port of the master the slave should connect to.
data SlaveRequest = SlaveRequest
    { srHost :: ByteString
    , srPort :: Int
    }
    deriving (Generic, Show, Typeable)

instance Binary SlaveRequest

data JobRequest = JobRequest
    { jrRequestType, jrResponseType :: ConcreteTypeRep
    , jrBody :: ByteString
    } deriving (Generic, Show, Typeable)

instance Binary JobRequest

data SubscribeOrCheck
    -- | When 'loop' doesn't find any work, it's called again using
    -- this constructor as its argument.  When this happens, it will
    -- block on the 'MVar', waiting on the requestChannel.  However,
    -- before blocking, it makes one more attempt at popping work,
    -- because some work may have come in before the subscription was
    -- established.
    = SubscribeToRequests (MVar ()) (IORef Connection)
    -- | This constructor indicates that 'loop' should just check for
    -- work, without subscribing to requestChannel. When the worker
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
        becomeSlave req@(SlaveRequest host port) = do
            $logDebugS "JobQueue" "Becoming slave"
            eres <- try $ runSlave (clientSettingsTCP port host) nmSettings calc
            case eres of
                Right () -> return ()
                -- This indicates that the slave couldn't connect.
                Left (IOError { ioe_type = NoSuchThing }) ->
                    $logDebugS "JobQueue" $
                        "Failed to connect to master, with " <>
                        tshow req <>
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

-- | This command is used by a master work server to request that a
-- slave connect to it.
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

notifyRequestAvailable :: MonadCommand m => RedisInfo -> m ()
notifyRequestAvailable r =
    runCommand_ (redisConnection r) $ publish (requestChannel r) ""

popSlaveRequest :: MonadCommand m => RedisInfo -> m (Maybe SlaveRequest)
popSlaveRequest r =
    runCommand (redisConnection r) (rpop (slaveRequestsKey r)) >>=
    mapM (decodeOrThrow "popSlaveRequest")

-- | Key used to for storing requests for slaves.
slaveRequestsKey :: RedisInfo -> LKey
slaveRequestsKey r = LKey $ Key $ redisKeyPrefix r <> "slave-requests"

-- | 'Channel' which is used to notify idle workers that there is a new
-- client request or slave request available.
requestChannel :: RedisInfo -> Channel
requestChannel r = Channel $ redisKeyPrefix r <> "request-channel"

-- | Variables used by 'jobQueueClient' / 'jobQueueRequest'.
data ClientVars m response = ClientVars
    { clientSubscribed :: TVar Bool
      -- ^ This is set to 'True' once the client is subscribed to its
      -- response backchannel, and so ready to send reqeusts.
      -- 'jobQueueRequest' blocks until this is 'True'.
    , clientDispatch :: SM.Map RequestId (Either DistributedJobQueueException response -> m ())
      -- ^ A map between 'RequestId's and their associated handlers.
      -- 'jobQueueClient' uses this to invoke the handlers inserted by
      -- 'jobQueueRequest'.
    } deriving (Typeable)

-- | Create a new 'ClientVars' value.
newClientVars :: IO (ClientVars m response)
newClientVars = ClientVars
    <$> newTVarIO False
    <*> SM.newIO

-- | Configuration used for running the client functions of job-queue.
data ClientConfig = ClientConfig
    { clientHeartbeatCheckIvl :: Seconds
      -- ^ How often to send heartbeat requests to the workers, and
      -- check for responses.  This value should be the same for all
      -- clients.
    , clientRequestExpiry :: Seconds
      -- ^ The expiry time of the request data stored in redis.
    , clientBackchannelId :: BackchannelId
      -- ^ Identifies the channel used to notify about a response.
    } deriving (Typeable)

defaultClientConfig :: ClientConfig
defaultClientConfig = ClientConfig
    { clientHeartbeatCheckIvl = Seconds 30
    , clientRequestExpiry = Seconds 3600
    , clientBackchannelId = defaultBackchannel
    }

defaultBackchannel :: BackchannelId
defaultBackchannel = "all-servers"

withJobQueueClient
    :: (MonadConnect m, Sendable response)
    => ClientConfig -> RedisInfo -> (ClientVars m response -> m a) -> m a
withJobQueueClient config r f = do
    control $ \restore -> do
        cvs <- newClientVars
        withAsync (restore (jobQueueClient config cvs r)) $ \_ -> do
            restore (f cvs)

-- | Runs a listener for responses from workers, which dispatches to
-- callbacks registered with 'jobQueueRequest'.  It also runs
-- 'checkHeartbeats', to ensure that some server periodically checks
-- the worker heartbeats.
--
-- This function should be run in its own thread, as it never returns
-- (the return type is @void@).
jobQueueClient
    :: (MonadConnect m, Sendable response)
    => ClientConfig
    -> ClientVars m response
    -> RedisInfo
    -> m void
jobQueueClient config cvs r = do
    control $ \restore ->
        withAsync (restore checker) $ \_ -> restore handleResponses
  where
    checker = checkHeartbeats r (clientHeartbeatCheckIvl config)
    handleResponses =
        subscribeToResponses r (clientBackchannelId config)
                               (clientSubscribed cvs)
            $ \rid -> do
                -- Lookup the handler before fetching / deleting the response,
                -- as the message may get delivered to multiple clients.
                let lookupAndRemove handler = return (handler, Remove)
                mhandler <- atomically $
                    SM.focus lookupAndRemove rid (clientDispatch cvs)
                forM_ mhandler $ \handler -> do
                    mresponse <- readResponse r rid
                    case mresponse of
                        Nothing -> throwM (ResponseMissingException rid)
                        Just response ->
                            decodeOrThrow "jobQueueClient" response >>=
                            handler

-- | Once a 'jobQueueClient' has been run with the 'ClientVars' value,
-- this function can be used to make requests and block on their
-- response.  It's up to the user of this and 'jobQueueWorker' to
-- ensure that the types of @request@ and @response@ match up.
-- Getting this wrong will cause a 'TypeMismatch' exception to be
-- thrown.
--
-- If the worker yields a 'DistributedJobQueueException', then this
-- function rethrows it.
jobQueueRequest
    :: (MonadCommand m, MonadLogger m, MonadThrow m, Sendable request, Sendable response)
    => ClientConfig
    -> ClientVars m response
    -> RedisInfo
    -> request
    -> m response
jobQueueRequest config cvs redis request = do
    (rid, mresponse) <- sendRequest config redis request
    case mresponse of
        Just response -> return response
        Nothing -> do
            resultVar <- newEmptyMVar
            registerResponseCallback cvs rid $ putMVar resultVar
            eres <- takeMVar resultVar
            either throwM return eres

-- | Sends a request to the workers.  This yields a 'RequestId' for use with
sendRequest
    :: forall m request response.
       ( MonadCommand m, MonadLogger m, MonadThrow m
       , Sendable request, Sendable response )
    => ClientConfig
    -> RedisInfo
    -> request
    -> m (RequestId, Maybe response)
sendRequest config r request = do
    let jrRequestType = fromTypeRep (typeRep (Nothing :: Maybe request))
        jrResponseType = fromTypeRep (typeRep (Nothing :: Maybe response))
        jrBody = toStrict (encode request)
        encoded = toStrict (encode JobRequest {..})
        expiry = clientRequestExpiry config
        ri = requestInfo (clientBackchannelId config) encoded
        k = riRequest ri
    mresponse <- pushRequest r expiry ri encoded
    case mresponse of
        Nothing -> do
            notifyRequestAvailable r
            return (k, Nothing)
        Just response -> do
            eres <- decodeOrThrow "sendRequest" response
            case eres of
                Left (_ :: DistributedJobQueueException) -> do
                    -- Cached exceptions are cleared.
                    clearResponse r k
                    return (k, Nothing)
                Right x -> return (k, Just x)

registerResponseCallback
    :: forall m response.
       (MonadCommand m, MonadLogger m, MonadThrow m , Sendable response)
    => ClientVars m response
    -> RequestId
    -> (Either DistributedJobQueueException response -> m ())
    -> m ()
registerResponseCallback cvs k handler = do
    atomically $ SM.focus addOrExtend k (clientDispatch cvs)
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
    :: MonadConnect m => RedisInfo -> WorkerId -> TVar Bool -> m void
sendHeartbeats r wid ready = do
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
        functioning <-
            run r (getset (heartbeatFunctioningKey r) (toStrict (encode False)))
        inactive <- if functioning == Just (toStrict (encode True))
            then do
                -- Fetch the list of inactive workers and move their
                -- jobs back to the requests queue.  If we re-enqueued
                -- some requests, then send out a notification about
                -- it.
                inactive <- run r $ smembers (heartbeatInactiveKey r)
                reenquedSome <- any id <$>
                    mapM (handleWorkerFailure r . WorkerId) inactive
                when reenquedSome $ notifyRequestAvailable r
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
-- for heartbeats.  It's used after a worker stops being a master.
--
-- It throws a 'WorkStillInProgress' exception if there is enqueued
-- work, so callers should ensure that this isn't the case.
--
-- The usage of this function in 'jobQueueWorker' is guaranteed to not
-- throw this exception, because it is called after 'sendResponse',
-- which removes the work from the active queue.
deactivateWorker :: (MonadCommand m, MonadThrow m)
                 => RedisInfo -> WorkerId -> m ()
deactivateWorker r wid = do
    activeCount <- run r $ llen (activeKey r wid)
    when (activeCount /= 0) $ throwM (WorkStillInProgress wid)
    run_ r $ srem (heartbeatActiveKey r) (unWorkerId wid :| [])

-- * Functions to compute Redis keys

heartbeatInactiveKey, heartbeatActiveKey :: RedisInfo -> SKey
-- A set of 'WorkerId' who have not yet removed their keys (indicating
-- that they're still alive and responding to heartbeats).
heartbeatInactiveKey r = SKey $ Key $ redisKeyPrefix r <> "heartbeat:inactive"
-- A set of 'WorkerId's that are currently thought to be running.
heartbeatActiveKey r = SKey $ Key $ redisKeyPrefix r <> "heartbeat:active"

-- Stores a "Data.Binary" encoded 'Bool'.
heartbeatFunctioningKey :: RedisInfo -> VKey
heartbeatFunctioningKey r = VKey $ Key $ redisKeyPrefix r <> "heartbeat:functioning"

-- Channel used for requesting that the workers remove their
-- 'WorkerId' from the set at 'heartbeatInactiveKey'.
heartbeatChannel :: RedisInfo -> Channel
heartbeatChannel r = Channel $ redisKeyPrefix r <> "heart]beat:channel"

-- Prefix used for the 'periodicActionWrapped' invocation, which is
-- used to share the responsibility of periodically checking
-- heartbeats.
heartbeatTimeKey :: RedisInfo -> PeriodicPrefix
heartbeatTimeKey r = PeriodicPrefix $ redisKeyPrefix r <> "heartbeat:time"

-- * Exceptions

-- | Exceptions which are returned to the client by work-queue.
data DistributedJobQueueException
    = WorkStillInProgress WorkerId
    -- ^ Thrown when the worker stops being a master but there's still
    -- work on its active queue.  This occuring indicates an error in
    -- the library.
    | RequestMissingException RequestId
    -- ^ Exception thrown when a worker can't find the request body.
    -- This means that the request body expired in redis
    -- (alternatively, it could indicate a bug in this library).
    | ResponseMissingException RequestId
    -- ^ Exception thrown when the client can't find the response
    -- body. This means that the response body expired in redis
    -- (alternatively, it could indicate a bug in this library).
    | TypeMismatch
        { expectedRequestType :: ConcreteTypeRep
        , actualRequestType :: ConcreteTypeRep
        , expectedResponseType :: ConcreteTypeRep
        , actualResponseType :: ConcreteTypeRep
        }
    -- ^ Thrown when the client makes a request with the wrong request
    -- / response types.
    | NetworkMessageException NetworkMessageException
    -- ^ Exceptions thrown by "Data.Streaming.NetworkMessage"
    | OtherException Text Text
    -- ^ This is used to return exceptions to the client, when
    -- exceptions occur while running the job.
    deriving (Eq, Show, Typeable, Generic)

instance Exception DistributedJobQueueException
instance Binary DistributedJobQueueException

wrapException :: SomeException -> DistributedJobQueueException
wrapException ex =
    case ex of
        (fromException -> Just err) -> err
        (fromException -> Just err) -> NetworkMessageException err
        (fromException -> Just (RequestMissing k)) -> RequestMissingException k
        _ -> OtherException (tshow (typeOf ex)) (tshow ex)

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
