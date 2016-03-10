{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE ViewPatterns #-}

-- | This module contains definitions used by
-- "Distributed.JobQueue.Worker" and "Distributed.JobQueue.Client"
module Distributed.JobQueue.Shared
    ( -- * Requests
      JobRequest(..)
    , notifyRequestAvailable
    , requestChannel
    , cancelKey
    , cancelValue
    , subscribeToRequests
    -- * Request events
    , RequestEvent(..)
    , addRequestEvent
    , addRequestEnqueuedEvent
    , getRequestEvents
    -- * Slave requests
    , MasterConnectInfo(..)
    , requestSlave
    , popSlaveRequest
    -- * Schema version
    , redisSchemaVersion
    , setRedisSchemaVersion
    , checkRedisSchemaVersion
    -- * Exceptions
    , DistributedJobQueueException(..)
    , wrapException
    , takeMVarE
    -- * Misc utilities
    , asyncLifted
    , withAsyncLifted
    -- * Exported for test
    , slaveRequestsKey
    ) where

import           ClassyPrelude
import           Control.Concurrent.Async (Async, async, withAsync)
import           Control.Exception (BlockedIndefinitelyOnMVar(..))
import           Control.Monad.Logger (MonadLogger, logInfo, logWarn)
import           Control.Monad.Trans.Control (MonadBaseControl, liftBaseWith)
import qualified Data.Aeson as Aeson
import qualified Data.ByteString.Lazy as LBS
import           Data.List.NonEmpty
import           Data.Serialize (Serialize, encode)
import           Data.Serialize.Orphans ()
import           Data.Streaming.NetworkMessage (NetworkMessageException)
import           Data.TypeFingerprint
import           Data.Typeable (typeOf)
import           Distributed.RedisQueue
import           Distributed.RedisQueue.Internal
import           FP.Redis
import           FP.ThreadFileLogger

-- * Requests

data JobRequest = JobRequest
    { jrRequestTypeFingerprint, jrResponseTypeFingerprint :: TypeFingerprint
    , jrSchema :: ByteString
    , jrBody :: ByteString
    } deriving (Generic, Show, Typeable)

instance Serialize JobRequest

notifyRequestAvailable :: MonadCommand m => RedisInfo -> m ()
notifyRequestAvailable r = run_ r $ publish (requestChannel r) ""

-- | 'Channel' which is used to notify idle workers that there is a new
-- client request or slave request available.
requestChannel :: RedisInfo -> Channel
requestChannel r = Channel $ redisKeyPrefix r <> "request-channel"

-- | Key which is filled with the value 'cancelValue' when the request gets
-- canceled.
--
-- This is essentially 'requestDataKey' with @:cancel@ added.
cancelKey :: RedisInfo -> RequestId -> VKey
cancelKey r k = VKey $ Key $ redisKeyPrefix r <> "request:" <> unRequestId k <> ":cancel"

-- | This gets placed at 'cancelKey' when things are cancelled. It's
-- just the string @"cancel"@.
cancelValue :: ByteString
cancelValue = "cancel"

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
    takeMVarE ready NoLongerWaitingForRequest
    -- 'notified' also gets filled by 'handleConnect', since this is
    -- needed when a reconnection occurs. We don't want it to be filled
    -- for the initial connection, though, so we take it.
    takeMVarE notified NoLongerWaitingForRequest
    -- Since we waited for ready to be filled, disconnectVar must no
    -- longer contains its error value.
    unsub <- readIORef disconnectVar
    return (notified, unsub)

-- * Request Events

data RequestEvent
    = RequestEnqueued
    | RequestWorkStarted WorkerId
    | RequestWorkFinished WorkerId
    | RequestResponseRead
    deriving (Generic, Show, Typeable)

instance Serialize RequestEvent
instance Aeson.ToJSON RequestEvent

data EventLogMessage
    = EventLogMessage
    { logTime :: String
    , logRequest :: RequestId
    , logEvent :: RequestEvent
    } deriving (Generic, Show, Typeable)

instance Aeson.ToJSON EventLogMessage

-- | Stores list of encoded '(UTCTime, RequestEvent)'.
requestEventsKey :: RedisInfo -> RequestId -> LKey
requestEventsKey r k = LKey $ Key $ redisKeyPrefix r <> "request:" <> unRequestId k <> ":events"

-- | Adds a 'RequestEvent', with the timestamp set to the current time.
addRequestEvent :: (MonadCommand m, MonadLogger m) => RedisInfo -> RequestId -> RequestEvent -> m ()
addRequestEvent r k x = do
    now <- liftIO getCurrentTime
    run_ r $ rpush (requestEventsKey r k) (encode (now, x) :| [])
    $logInfo $ decodeUtf8 $ LBS.toStrict $ Aeson.encode $ EventLogMessage
        { logTime = show now
        , logRequest = k
        , logEvent = x
        }

-- | Adds 'RequestEnqueued' event and sets expiry on the events list.
addRequestEnqueuedEvent :: (MonadCommand m, MonadLogger m) => RedisInfo -> RequestId -> Seconds -> m ()
addRequestEnqueuedEvent r k expiry = do
    addRequestEvent r k RequestEnqueued
    expirySet <- run r $ expire (unLKey (requestEventsKey r k)) expiry
    unless expirySet $ liftIO $ throwIO (InternalJobQueueException "Failed to set request events expiry")

-- | Gets all of the events which have been added for the specified request.
getRequestEvents :: MonadCommand m => RedisInfo -> RequestId -> m [(UTCTime, RequestEvent)]
getRequestEvents r k =
    run r (lrange (requestEventsKey r k) 0 (-1)) >>=
    mapM (decodeOrThrow "getRequestEvents")

-- * Slave Requests

-- | Hostname and port of the master the slave should connect to.  The
-- 'Serialize' instance for this is used to serialize this info to the
-- list stored at 'slaveRequestsKey'.
data MasterConnectInfo = MasterConnectInfo
    { mciHost :: ByteString
    , mciPort :: Int
    }
    deriving (Generic, Show, Typeable)

instance Serialize MasterConnectInfo

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
    let encoded = encode mci
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

-- * Schema version

redisSchemaVersion :: ByteString
redisSchemaVersion = "3"

redisSchemaKey :: RedisInfo -> VKey
redisSchemaKey r = VKey $ Key $ redisKeyPrefix r <> "version"

-- | Checks if the redis schema version is correct.  If not present, then the
-- key gets set.
setRedisSchemaVersion :: (MonadCommand m, MonadLogger m) => RedisInfo -> m ()
setRedisSchemaVersion r = do
    mv <- run r $ get (redisSchemaKey r)
    case mv of
        Just v | v /= redisSchemaVersion -> $logWarn $
            "Redis schema version changed from " <> tshow v <> " to " <> tshow redisSchemaVersion <>
            ".  This is only expected once after updating work-queue version."
        _ -> return ()
    run_ r $ set (redisSchemaKey r) redisSchemaVersion []

-- | Throws 'MismatchedRedisSchemaVersion' if it's wrong or unset.
checkRedisSchemaVersion :: MonadCommand m => RedisInfo -> m ()
checkRedisSchemaVersion r = do
    v <- fmap (fromMaybe "") $ run r $ get (redisSchemaKey r)
    when (v /= redisSchemaVersion) $ liftIO $ throwIO MismatchedRedisSchemaVersion
        { actualRedisSchemaVersion = v
        , expectedRedisSchemaVersion = redisSchemaVersion
        }

-- * Exceptions

-- | Exceptions which are returned to the client by the job-queue.
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
        { expectedRequestTypeFingerprint :: TypeFingerprint
        , actualRequestTypeFingerprint :: TypeFingerprint
        , expectedResponseTypeFingerprint :: TypeFingerprint
        , actualResponseTypeFingerprint :: TypeFingerprint
        }
    -- ^ Thrown when the client makes a request with the wrong request
    -- / response types.
    | RequestCanceledException RequestId
    -- ^ The request has been cancelled.
    | NoRequestForCallbackRegistration RequestId
    -- ^ Exception thrown when registering a callback for a non-existent
    -- request.
    | MismatchedRedisSchemaVersion
        { expectedRedisSchemaVersion :: ByteString
        , actualRedisSchemaVersion :: ByteString
        }
    -- ^ Exception thrown on initialization of work-queue
    | MismatchedRequestRedisSchemaVersion
        { expectedRequestRedisSchemaVersion :: ByteString
        , actualRequestRedisSchemaVersion :: ByteString
        , schemaMismatchRequestId :: RequestId
        }
    -- ^ Exception thrown when request is received with the wrong schema
    -- version.
    | NoLongerWaitingForResult
    -- ^ Exception thrown by job-queue client functions that block
    -- waiting for response.
    | NoLongerWaitingForRequest
    -- ^ Exception thrown by worker functions that block waiting for
    -- request.
    | NetworkMessageException NetworkMessageException
    -- ^ Exceptions thrown by "Data.Streaming.NetworkMessage"
    | InternalJobQueueException Text
    -- ^ Used for unexpected conditions.
    | OtherException Text Text
    -- ^ This is used to return exceptions to the client, when
    -- exceptions occur while running the job.
    deriving (Eq, Typeable, Generic)

instance Exception DistributedJobQueueException
instance Serialize DistributedJobQueueException

instance Show DistributedJobQueueException where
    show (WorkStillInProgress wid) =
        "WorkStillInProgress (" ++
        show wid ++
        ") {- This indicates a bug in the work queue library. -}"
    show (RequestMissingException wid) =
        "RequestMissingException (" ++
        show wid ++
        ") {- This likely means that the request body expired in redis. -}"
    show (ResponseMissingException rid) =
        "ResponseMissingException (" ++
        show rid ++
        ") {- This likely means that the response body expired in redis. -}"
    show (TypeMismatch {..}) =
        "TypeMismatch " ++
        "{ expectedResponseTypeFingerprint = " ++ show expectedResponseTypeFingerprint ++
        ", actualResponseTypeFingerprint = " ++ show actualResponseTypeFingerprint ++
        ", expectedRequestTypeFingerprint = " ++ show expectedRequestTypeFingerprint ++
        ", actualRequestTypeFingerprint = " ++ show actualRequestTypeFingerprint ++
        "}"
    show (RequestCanceledException rid) =
        "RequestCanceledException (" ++
        show rid ++
        ")"
    show (NoRequestForCallbackRegistration rid) =
        "NoRequestForCallbackRegistration (" ++
        show rid ++
        ")"
    show (MismatchedRedisSchemaVersion {..}) =
        "MismatchedRedisSchemaVersion " ++
        "{ expectedRedisSchemaVersion = " ++ show expectedRedisSchemaVersion ++
        ", actualRedisSchemaVersion = " ++ show actualRedisSchemaVersion ++
        "}"
    show (MismatchedRequestRedisSchemaVersion {..}) =
        "MismatchedRequestRedisSchemaVersion " ++
        "{ expectedRequestRedisSchemaVersion = " ++ show expectedRequestRedisSchemaVersion ++
        ", actualRequestRedisSchemaVersion = " ++ show actualRequestRedisSchemaVersion ++
        ", schemaMismatchRequestId = " ++ show schemaMismatchRequestId ++
        "}"
    show NoLongerWaitingForResult = concat
        [ "NoLongerWaitingForResult "
        , "{- This indicates that the jobQueueClient threads are no longer running.  "
        , "This is usually because lost the connection to redis, and couldn't reconnect. -}"
        ]
    show NoLongerWaitingForRequest = concat
        [ "NoLongerWaitingForRequest "
        , "{- This is usually because we lost the connection to redis, and couldn't reconnect. -}"
        ]
    show (NetworkMessageException nme) =
        "NetworkMessageException (" ++ show nme ++ ")"
    show (InternalJobQueueException txt) =
        "InternalJobQueueException " ++ show txt
    show (OtherException ty txt) =
        "OtherException " ++ show ty ++ " " ++ show txt

wrapException :: SomeException -> DistributedJobQueueException
wrapException ex =
    case ex of
        (fromException -> Just err) -> err
        (fromException -> Just err) -> NetworkMessageException err
        _ -> OtherException (tshow (typeOf ex)) (tshow ex)

-- | Like 'takeMVar', but convert wrap 'BlockedIndefinitelyOnMVar' exception in other exception.
takeMVarE :: (MonadBaseControl IO m, Exception ex) => MVar a -> ex -> m a
takeMVarE mvar exception = takeMVar mvar `catch` \BlockedIndefinitelyOnMVar -> throwIO exception

asyncLifted :: MonadBaseControl IO m => m () -> m (Async ())
asyncLifted f = liftBaseWith $ \restore -> async (void (restore f))

withAsyncLifted :: MonadBaseControl IO m => m () -> (Async () -> m ()) -> m ()
withAsyncLifted f g = liftBaseWith $ \restore -> withAsync (void (restore f)) (void . restore . g)
