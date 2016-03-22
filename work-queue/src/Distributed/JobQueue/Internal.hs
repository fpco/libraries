{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}

module Distributed.JobQueue.Internal where

import ClassyPrelude
import Control.Monad.Logger (MonadLogger, logWarn, logInfo)
import qualified Data.Aeson as Aeson
import Data.List.NonEmpty
import Data.Serialize (Serialize, encode)
import Distributed.Redis
import Distributed.Types
import FP.Redis

-- * Redis keys

-- | List of 'RequestId'. The client enqueues requests to this list,
-- and the workers pop them off.
requestsKey :: Redis -> LKey
requestsKey r = LKey $ Key $ redisKeyPrefix r <> "requests"

-- | Given a 'RequestId', computes the key for the request data.
requestDataKey :: Redis -> RequestId -> VKey
requestDataKey  r k = VKey $ Key $ redisKeyPrefix r <> "request:" <> unRequestId k

-- | 'Channel' which is used to notify idle workers that there is a new
-- client request or slave request available.
requestChannel :: Redis -> NotifyChannel
requestChannel r = NotifyChannel $ Channel $ redisKeyPrefix r <> "request-channel"

-- | Given a 'RequestId', stores when the request was enqueued.
requestTimeKey :: Redis -> RequestId -> VKey
requestTimeKey r k = VKey $ Key $ redisKeyPrefix r <> "request:" <> unRequestId k <> ":time"

-- | Given a 'RequestId', computes the key for the response data.
responseDataKey :: Redis -> RequestId -> VKey
responseDataKey r k = VKey $ Key $ redisKeyPrefix r <> "response:" <> unRequestId k

-- | Given a 'BackchannelId', computes the name of a 'Channel'.  This
-- 'Channel' is used to notify clients when responses are available.
responseChannel :: Redis -> Channel
responseChannel r = Channel $ redisKeyPrefix r <> "response-channel"

-- | Given a 'RequestId', stores when the request was enqueued.
responseTimeKey :: Redis -> RequestId -> VKey
responseTimeKey r k = VKey $ Key $ redisKeyPrefix r <> "response:" <> unRequestId k <> ":time"

-- | Given a 'WorkerId', computes the name of a redis key which usually
-- contains a list.  This list holds the items the worker is currently
-- working on.
--
-- See the documentation for 'Distributed.RedisQueue.HeartbeatFailure'
-- for more information on why this is not an 'LKey'.
activeKey :: Redis -> WorkerId -> Key
activeKey r k = Key $ redisKeyPrefix r <> "active:" <> unWorkerId k

-- | Key which is filled with the value 'cancelValue' when the request gets
-- canceled.
--
-- This is essentially 'requestDataKey' with @:cancel@ added.
cancelKey :: Redis -> RequestId -> VKey
cancelKey r k = VKey $ Key $ redisKeyPrefix r <> "request:" <> unRequestId k <> ":cancel"

-- | This gets placed at 'cancelKey' when things are cancelled. It's
-- just the string @"cancel"@.
cancelValue :: ByteString
cancelValue = "cancel"

-- * Schema version

redisSchemaVersion :: ByteString
redisSchemaVersion = "3"

redisSchemaKey :: Redis -> VKey
redisSchemaKey r = VKey $ Key $ redisKeyPrefix r <> "version"

-- | Checks if the redis schema version is correct.  If not present, then the
-- key gets set.
setRedisSchemaVersion :: (MonadCommand m, MonadLogger m) => Redis -> m ()
setRedisSchemaVersion r = do
    mv <- run r $ get (redisSchemaKey r)
    case mv of
        Just v | v /= redisSchemaVersion -> $logWarn $
            "Redis schema version changed from " <> tshow v <> " to " <> tshow redisSchemaVersion <>
            ".  This is only expected once after updating work-queue version."
        _ -> return ()
    run_ r $ set (redisSchemaKey r) redisSchemaVersion []

-- | Throws 'MismatchedRedisSchemaVersion' if it's wrong or unset.
checkRedisSchemaVersion :: MonadCommand m => Redis -> m ()
checkRedisSchemaVersion r = do
    v <- fmap (fromMaybe "") $ run r $ get (redisSchemaKey r)
    when (v /= redisSchemaVersion) $ liftIO $ throwIO MismatchedRedisSchemaVersion
        { actualRedisSchemaVersion = v
        , expectedRedisSchemaVersion = redisSchemaVersion
        }

-- * Request Events

data RequestEvent
    = RequestEnqueued
    | RequestWorkStarted !WorkerId
    | RequestWorkReenqueuedByWorker !WorkerId
    | RequestWorkFinished !WorkerId
    | RequestResponseRead
    deriving (Generic, Show, Typeable)

instance Serialize RequestEvent
instance Aeson.ToJSON RequestEvent

data EventLogMessage
    = EventLogMessage
    { logTime :: !String
    , logRequest :: !RequestId
    , logEvent :: !RequestEvent
    } deriving (Generic, Show, Typeable)

instance Aeson.ToJSON EventLogMessage

-- | Stores list of encoded '(UTCTime, RequestEvent)'.
requestEventsKey :: Redis -> RequestId -> LKey
requestEventsKey r k = LKey $ Key $ redisKeyPrefix r <> "request:" <> unRequestId k <> ":events"

-- | Adds a 'RequestEvent', with the timestamp set to the current time.
addRequestEvent :: (MonadCommand m, MonadLogger m) => Redis -> RequestId -> RequestEvent -> m ()
addRequestEvent r k x = do
    now <- liftIO getCurrentTime
    run_ r $ rpush (requestEventsKey r k) (encode (now, x) :| [])
    $logInfo $ decodeUtf8 $ toStrict $ Aeson.encode $ EventLogMessage
        { logTime = show now
        , logRequest = k
        , logEvent = x
        }

-- | Adds 'RequestEnqueued' event and sets expiry on the events list.
addRequestEnqueuedEvent :: (MonadCommand m, MonadLogger m) => JobQueueConfig -> Redis -> RequestId -> m ()
addRequestEnqueuedEvent config r k = do
    addRequestEvent r k RequestEnqueued
    expirySet <- run r $ expire (unLKey (requestEventsKey r k)) (jqcEventExpiry config)
    unless expirySet $ liftIO $ throwIO (InternalJobQueueException "Failed to set request events expiry")

-- | Gets all of the events which have been added for the specified request.
getRequestEvents :: MonadCommand m => Redis -> RequestId -> m [(UTCTime, RequestEvent)]
getRequestEvents r k =
    run r (lrange (requestEventsKey r k) 0 (-1)) >>=
    mapM (decodeOrThrow "getRequestEvents")
