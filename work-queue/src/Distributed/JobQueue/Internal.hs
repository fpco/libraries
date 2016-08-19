{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}

module Distributed.JobQueue.Internal where

import ClassyPrelude
import Control.Monad.Logger.JSON.Extra (logWarnJ, logInfoJ
                                       , logWarnSJ)
import qualified Data.Aeson as Aeson
import qualified Data.ByteString as BS
import Data.List.NonEmpty hiding (unwords)
import Data.Store (Store, encode)
import Distributed.Redis
import Distributed.Types
import Distributed.Heartbeat
import FP.Redis
import Data.Store.TypeHash (TypeHash)
import qualified Data.ByteString.Char8 as B

-- * Redis keys

-- | List of 'RequestId'. The client enqueues requests to this list,
-- and the workers pop them off.
requestsKey :: Redis -> LKey
requestsKey r = LKey $ Key $ redisKeyPrefix r <> "requests"

-- | List of urgent 'RequestId's.
--
--Jobs in this queue take precedence over those in 'requestsKey'.
urgentRequestsKey :: Redis -> LKey
urgentRequestsKey r = LKey $ Key $ redisKeyPrefix r <> "requests-urgent"

-- | Key to the set of all 'RequestId's of urgent requests.
--
-- This holds all the 'RequestId's of urgent requests, regardless of
-- whether they are already taken by a worker.  This is used to make
-- sure that urgent requests are re-enqueued to the urgent queue.
urgentRequestsSetKey :: Redis -> SKey
urgentRequestsSetKey r = SKey $ Key $ redisKeyPrefix r <> "requests-urgent-set"

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

-- | Prefix of all the 'activeKey's.
allActiveKeyPrefix :: Redis -> ByteString
allActiveKeyPrefix r = redisKeyPrefix r <> "active:"

-- | Pattern that matches all the 'activeKey's.
allActiveKeyPattern :: Redis -> ByteString
allActiveKeyPattern r = allActiveKeyPrefix r <> "*"

-- | Given a worker's 'activeKey', get its 'WorkerId'.
workerIdFromActiveKey :: Redis -> Key -> Maybe WorkerId
workerIdFromActiveKey r k =
    if prefix `BS.isPrefixOf` bs
    then Just . WorkerId . BS.drop (BS.length prefix) $ bs
    else Nothing
  where
    prefix = allActiveKeyPrefix r
    bs = unKey k

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
        Just v | v /= redisSchemaVersion -> $logWarnJ $
            "Redis schema version changed from " <> tshow v <> " to " <> tshow redisSchemaVersion <>
            ".  This is only expected once after updating work-queue version."
        _ -> return ()
    run_ r $ set (redisSchemaKey r) redisSchemaVersion []

-- | Throws 'MismatchedRedisSchemaVersion' if it's wrong or unset.
checkRedisSchemaVersion :: MonadConnect m => Redis -> m ()
checkRedisSchemaVersion r = do
    mVersion <- run r $ get (redisSchemaKey r)
    case mVersion of
        Nothing -> $logInfoJ ("Redis schema not set.  This probably just means that no client is running yet."::Text)
        Just v -> when (v /= redisSchemaVersion) $ liftIO $ throwIO MismatchedRedisSchemaVersion
            { actualRedisSchemaVersion = v
            , expectedRedisSchemaVersion = redisSchemaVersion
            }

-- | Make sure that the 'activeKey' of a given worker does not have
-- more than one element.
--
-- This invariant should be checked very time an item is added to an
-- 'activeKey'.
checkActiveKey :: MonadConnect m => Redis -> WorkerId -> m ()
checkActiveKey r wid = do
    nRequests <- run r $ llen (activeKey r wid)
    unless (nRequests <= 1) $ liftIO . throwIO . InternalJobQueueException $
        unwords ["Request list should always have length 0 or 1, but has length"
                , pack . show $ nRequests
                ]

-- | Requests are re-enqueued in three situations:
data ReenqueueReason =
    ReenqueuedByWorker -- ^ The worker decided to re-enqueue the request.
    | ReenqueuedAfterHeartbeatFailure -- ^ The worker failed its heartbeat
    | ReenqueuedAsStale -- ^ A job is detected as stale, as in "Distributed.JobQueue.StaleKeys"
    deriving (Generic, Show, Typeable)
instance Store ReenqueueReason
instance Aeson.ToJSON ReenqueueReason

-- | Request Events
data RequestEvent
    = RequestEnqueued
    | RequestWorkStarted !WorkerId
    | RequestWorkReenqueued !ReenqueueReason !WorkerId
    | RequestWorkFinished !WorkerId
    | RequestResponseRead
    deriving (Generic, Show, Typeable)
instance Store RequestEvent
instance Aeson.ToJSON RequestEvent

-- | Re-enqueue the request of a given worker.
reenqueueRequest :: MonadConnect m => ReenqueueReason -> Redis -> WorkerId -> m (Maybe RequestId)
reenqueueRequest reason r wid = do
    mbRid <- run r reenqueue
    when (mbRid == Just "ERROR_ACTIVEKEY_NOT_EMPTY")
        (liftIO $ throwIO $ InternalJobQueueException $
         "activeKey of worker " ++ tshow wid ++ " was not empty after its job was re-enqueued.")
    checkActiveKey r wid
    case mbRid of
        Nothing -> failureLog
        Just rid -> do
            addRequestEvent r (RequestId rid) (RequestWorkReenqueued reason wid)
            successLog rid
    return $ RequestId <$> mbRid
    where
      -- re-enqueue the job to the appropriate queue (if the job is
      -- urgent, to urgentRequestsKey, otherwise to requestsKey).  We
      -- use a LUA script to save roundtrips, and to ensure atomicity.
      reenqueue :: CommandRequest (Maybe ByteString)
      reenqueue = eval
          (B.unlines [ "local rid = redis.call('LPOP', KEYS[1])"
                     , "if rid == false then"
                     , "  return false"
                     , "else"
                     , "  local isUrgent = redis.call('SISMEMBER', KEYS[2], rid)"
                     , "  if isUrgent then"
                     , "    redis.call('RPUSH', KEYS[3], rid)"
                     , "  else"
                     , "    redis.call('RPUSH', KEYS[4], rid)"
                     , "  end"
                     , "  if redis.call('LLEN', KEYS[1]) == 0 then"
                     , "    return rid"
                     , "  else"
                     , "    return 'ERROR_ACTIVEKEY_NOT_EMPTY'"
                     , "  end"
                     , "end"
                     ])
          [ unLKey $ activeKey r wid
          , unSKey $ urgentRequestsSetKey r
          , unLKey $ urgentRequestsKey r
          , unLKey $ requestsKey r
          ]
          []
      failureLog = case reason of
          ReenqueuedByWorker -> return () -- The log will be produced by 'checkPoppedActiveKey'.
          ReenqueuedAfterHeartbeatFailure ->
              $logWarnSJ "JobQueue" $ tshow wid <> " failed its heartbeat, but didn't have an item to re-enqueue."
          ReenqueuedAsStale ->
              $logWarnSJ "JobQueue" $ tshow wid <> " is not active anymore, and does not have a job."
      successLog rid = case reason of
          ReenqueuedByWorker -> return () -- The log will be produced by 'checkPoppedActiveKey'.
          ReenqueuedAfterHeartbeatFailure ->
              $logWarnSJ "JobQueue" $ tshow wid <> " failed its heartbeat, and " <> tshow rid <> " was re-enqueued."
          ReenqueuedAsStale ->
              $logWarnSJ "JobQueue" $ tshow wid <> " is not active anymore, and " <> tshow rid <> " was re-enqueued."

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
    $logInfoJ $ EventLogMessage
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


-- | Priority of a request.
--
-- Requests with priority 'PriorityUrgent' will be enqueued at the
-- front of the queue, so they will be served first.
data RequestPriority = PriorityUrgent
                     | PriorityNormal
                     deriving Eq
-- | Configuration of job-queue, used by both the client and worker.
--
-- REVIEW TODO: Take a look if it's worth having just one type for client
-- and worker.
data JobQueueConfig = JobQueueConfig
    { jqcRedisConfig :: !RedisConfig
    -- ^ Configuration of communication with redis.
    , jqcHeartbeatConfig :: !HeartbeatConfig
    -- ^ Configuration for heartbeat sending and checking.
    --
    -- REVIEW: This config is used by both the client and the worker to
    -- set up heartbeats. The documentation is elsewhere.
    , jqcRequestExpiry :: !Seconds
    -- ^ The expiry time of the request data stored in redis. If it
    -- takes longer than this time for the worker to attempt to fetch
    -- the request data, it will fail and the request will be answered
    -- with 'RequestMissingException'.
    , jqcResponseExpiry :: !Seconds
    -- ^ How many seconds the response data should be kept in
    -- redis. The longer it's kept in redis, the more opportunity
    -- there is to eliminate redundant work, if identical requests are
    -- made (where identical means that the same request id was used
    -- - the request body is not inspected for caching purposes).
    --
    -- A longer expiry also allows more time between sending a
    -- response notification and the client reading its data. If the
    -- client finds that the response is missing,
    -- 'ResponseMissingException' is thrown.
    , jqcEventExpiry :: !Seconds
    -- ^ How many seconds an 'EventLogMessage' remains in redis.
    , jqcCancelCheckIvl :: !Seconds
    -- ^ How often the worker should poll redis for an indication that
    -- the request has been cancelled.
    , jqcRequestNotificationFailsafeTimeout :: !Milliseconds
    , jqcSlaveRequestsNotificationFailsafeTimeout :: !Milliseconds
    , jqcWaitForResponseNotificationFailsafeTimeout :: !Milliseconds
    , jqcCheckStaleKeysInterval :: !Seconds
    -- ^ How often to check for jobs that have been taken by workers
    -- that are currently considered dead, see
    -- "Distributed.JobQueue.StaleKeys"
    } deriving (Eq, Show)

-- | Default settings for the job-queue:
--
-- * Uses 'defaultHeartbeatConfig'.
--
-- * Uses 'defaultRedisConfig'.
--
-- * Requests and responses expire after an hour.
--
-- * 'EventLogMessage's expire after a day.
--
-- * Workers check for cancellation every 10 seconds.
defaultJobQueueConfig :: ByteString -> JobQueueConfig
defaultJobQueueConfig prefix = JobQueueConfig
    { jqcRedisConfig = defaultRedisConfig prefix
    , jqcHeartbeatConfig = defaultHeartbeatConfig
    , jqcRequestExpiry = Seconds 3600
    , jqcResponseExpiry = Seconds 3600
    , jqcEventExpiry = Seconds (3600 * 24) -- 1 day
    , jqcCancelCheckIvl = Seconds 10
    , jqcRequestNotificationFailsafeTimeout = Milliseconds (1 * 1000) -- 1 secs
    , jqcSlaveRequestsNotificationFailsafeTimeout = Milliseconds (1 * 1000) -- 1 secs
    , jqcWaitForResponseNotificationFailsafeTimeout = Milliseconds 1000 -- 1 secs
    , jqcCheckStaleKeysInterval = Seconds . (*2) . unSeconds . hcCheckerIvl $ defaultHeartbeatConfig
      -- we use double the heartbeat check interval -- checking faster
      -- than the heartbeat check doesn't make sense, since workers
      -- will only be considered dead after a heartbeat check.
    }

data JobRequest = JobRequest
    { jrRequestTypeHash, jrResponseTypeHash :: !TypeHash
    , jrSchema :: !ByteString
    -- REVIEW: This is a tag to detect if the deployment is compatible with the current
    -- code.
    , jrBody :: !ByteString
    } deriving (Generic, Show, Typeable)

instance Store JobRequest
