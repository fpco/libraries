{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}

module Distributed.JobQueue.Internal where

import ClassyPrelude
import Control.Monad.Logger (logWarn, logInfo)
import qualified Data.Aeson as Aeson
import Data.List.NonEmpty
import Data.Serialize (Serialize, encode)
import Distributed.Redis
import Distributed.Types
import Distributed.Heartbeat
import FP.Redis
import Data.TypeFingerprint (TypeFingerprint)

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

-- | Contains the request a given worker is working on. Is either empty
-- or has one element.
activeKey :: Redis -> WorkerId -> LKey
activeKey r k = LKey $ Key $ redisKeyPrefix r <> "active:" <> unWorkerId k

-- | Channel to push cancellations.
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
setRedisSchemaVersion :: (MonadConnect m) => Redis -> m ()
setRedisSchemaVersion r = do
    mv <- run r $ get (redisSchemaKey r)
    case mv of
        Just v | v /= redisSchemaVersion -> $logWarn $
            "Redis schema version changed from " <> tshow v <> " to " <> tshow redisSchemaVersion <>
            ".  This is only expected once after updating work-queue version."
        _ -> return ()
    run_ r $ set (redisSchemaKey r) redisSchemaVersion []

-- | Throws 'MismatchedRedisSchemaVersion' if it's wrong or unset.
checkRedisSchemaVersion :: MonadConnect m => Redis -> m ()
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
addRequestEvent :: (MonadConnect m) => Redis -> RequestId -> RequestEvent -> m ()
addRequestEvent r k x = do
    now <- liftIO getCurrentTime
    run_ r $ rpush (requestEventsKey r k) (encode (now, x) :| [])
    $logInfo $ decodeUtf8 $ toStrict $ Aeson.encode $ EventLogMessage
        { logTime = show now
        , logRequest = k
        , logEvent = x
        }

-- | Adds 'RequestEnqueued' event and sets expiry on the events list.
addRequestEnqueuedEvent :: (MonadConnect m) => JobQueueConfig -> Redis -> RequestId -> m ()
addRequestEnqueuedEvent config r k = do
    addRequestEvent r k RequestEnqueued
    expirySet <- run r $ expire (unLKey (requestEventsKey r k)) (jqcEventExpiry config)
    unless expirySet $ liftIO $ throwIO (InternalJobQueueException "Failed to set request events expiry")

-- | Gets all of the events which have been added for the specified request.
getRequestEvents :: MonadConnect m => Redis -> RequestId -> m [(UTCTime, RequestEvent)]
getRequestEvents r k =
    run r (lrange (requestEventsKey r k) 0 (-1)) >>=
    mapM (decodeOrThrow "getRequestEvents")

-- * Config

-- | Configuration of job-queue, used by both the client and worker.
--
-- REVIEW TODO: Take a look if it's worth having just one type for client
-- and worker.
data JobQueueConfig = JobQueueConfig
    { jqcRedisConfig :: !RedisConfig
    -- ^ Configuration of communication with redis.
    , jqcHeartbeatConfig :: !HeartbeatConfig
    -- ^ Configuration for heartbeat checking.
    --
    -- REVIEW: This config is used by both the client and the worker to
    -- set up heartbeats. The documentation is elsewhere.
    , jqcHeartbeatFailureExpiry :: !Seconds
    -- ^ How long a heartbeat failure should stick around. This should
    -- be a quite large amount of time, as the worker might eventually
    -- reconnect, and it should know that it has been heartbeat-failure
    -- collected. Garbage collecting them to save resources doesn't
    -- matter very much. The main reason it matters is so that the UI
    -- doesn't end up with tons of heartbeat failure records.
    --
    -- REVIEW: With "garbage collection" above we mean collection of
    -- heartbeat-related data in redis.
    , jqcRequestExpiry :: !Seconds
    -- ^ The expiry time of the request data stored in redis. If it
    -- takes longer than this time for the worker to attempt to fetch
    -- the request data, it will fail and the request will be answered
    -- with 'RequestMissingException'.
    , jqcResponseExpiry :: !Seconds
    -- ^ How many seconds the response data should be kept in redis. The
    -- longer it's kept in redis, the more opportunity there is to
    -- eliminate redundant work, if identical requests are made. A
    -- longer expiry also allows more time between sending a response
    -- notification and the client reading its data. If the client finds
    -- that the response is missing, 'ResponseMissingException' is
    -- thrown.
    --
    -- REVIEW: With "identical" here we're only talking about the request
    -- id, so if we resubmit the request _with the same request id_, the
    -- response will be cached. The body of the request is irrelevant.
    , jqcEventExpiry :: !Seconds
    -- ^ How many seconds an 'EventLogMessage' remains in redis.
    , jqcCancelCheckIvl :: !Seconds
    -- ^ How often the worker should poll redis for an indication that
    -- the request has been cancelled.
    }

-- | Default settings for the job-queue:
--
-- * Uses 'defaultHeartbeatConfig'.
--
-- * Uses 'defaultRedisConfig'.
--
-- * Requests, responses, and heartbeat failures expire after an hour.
--
-- * 'EventLogMessage's expire after a day.
--
-- * Workers check for cancellation every 10 seconds.
defaultJobQueueConfig :: JobQueueConfig
defaultJobQueueConfig = JobQueueConfig
    { jqcRedisConfig = defaultRedisConfig
    , jqcHeartbeatConfig = defaultHeartbeatConfig
    , jqcHeartbeatFailureExpiry = Seconds 3600 -- 1 hour
    , jqcRequestExpiry = Seconds 3600
    , jqcResponseExpiry = Seconds 3600
    , jqcEventExpiry = Seconds (3600 * 24) -- 1 day
    , jqcCancelCheckIvl = Seconds 10
    }

data JobRequest = JobRequest
    { jrRequestTypeFingerprint, jrResponseTypeFingerprint :: !TypeFingerprint
    , jrSchema :: !ByteString
    -- REVIEW: This is a tag to detect if the deployment is compatible with the current
    -- code.
    , jrBody :: !ByteString
    } deriving (Generic, Show, Typeable)

instance Serialize JobRequest
