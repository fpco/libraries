{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

module Distributed.Types where

import           ClassyPrelude hiding ((<>))
import           Control.Monad.Logger (LogSource, LogLevel, LogStr, Loc)
import           Control.Retry
import qualified Data.Aeson as Aeson
import qualified Data.ByteString.Char8 as BS8
import           Data.Serialize (Serialize)
import           Data.Serialize.Orphans ()
import           Data.Streaming.NetworkMessage (NetworkMessageException)
import qualified Data.Text as T
import           Data.TypeFingerprint
import           FP.Redis

-- * Logging

-- | Type of function used for logging.
type LogFunc = Loc -> LogSource -> LogLevel -> LogStr -> IO ()

-- * IDs used in Redis

-- | Every worker has a 'WorkerId' to uniquely identify it. It's needed
-- for the fault tolerance portion - in the event that a worker goes
-- down we need to be able to re-enqueue its work.
newtype WorkerId = WorkerId { unWorkerId :: ByteString }
    deriving (Eq, Ord, Show, Serialize, IsString, Typeable)

instance Aeson.ToJSON WorkerId where
    toJSON = Aeson.String . T.pack . BS8.unpack . unWorkerId

-- | This is the key used for enqueued requests, and, later, the
-- response associated with it. It's the hash of the request, which
-- allows responses to be cached.
newtype RequestId = RequestId { unRequestId :: ByteString }
    deriving (Eq, Ord, Show, Serialize, Hashable, Typeable)

instance Aeson.ToJSON RequestId where
    toJSON = Aeson.String . T.pack . BS8.unpack . unRequestId

-- * Job-queue

-- | Configuration of job-queue, used by both the client and worker.
data JobQueueConfig = JobQueueConfig
    { jqcRedisConfig :: !RedisConfig
    -- ^ Configuration of communication with redis.
    , jqcHeartbeatConfig :: !HeartbeatConfig
    -- ^ Configuration for heartbeat checking.
    , jqcHeartbeatFailureExpiry :: !Seconds
    -- ^ How long a heartbeat failure should stick around. This should
    -- be a quite large amount of time, as the worker might eventually
    -- reconnect, and it should know that it has been heartbeat-failure
    -- collected. Garbage collecting them to save resources doesn't
    -- matter very much. The main reason it matters is so that the UI
    -- doesn't end up with tons of heartbeat failure records.
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
    , jqcEventExpiry :: !Seconds
    -- ^ How many seconds an 'EventLogMessage' remains in redis.
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
defaultJobQueueConfig :: JobQueueConfig
defaultJobQueueConfig = JobQueueConfig
    { jqcRedisConfig = defaultRedisConfig
    , jqcHeartbeatConfig = defaultHeartbeatConfig
    , jqcHeartbeatFailureExpiry = Seconds 3600
    , jqcRequestExpiry = Seconds 3600
    , jqcResponseExpiry = Seconds 3600
    , jqcEventExpiry = Seconds (3600 * 24)
    }

data JobRequest = JobRequest
    { jrRequestTypeFingerprint, jrResponseTypeFingerprint :: !TypeFingerprint
    , jrSchema :: !ByteString
    , jrBody :: !ByteString
    } deriving (Generic, Show, Typeable)

instance Serialize JobRequest

-- * Redis

-- | Configuration of redis connection, along with a prefix for keys.
data RedisConfig = RedisConfig
    { rcConnectInfo :: !ConnectInfo
    -- ^ Redis host and port.
    , rcKeyPrefix :: !ByteString
    -- ^ Prefix to prepend to redis keys.
    }

-- | Default settingfs for connecting to redis:
--
-- * Use port 6379 (redis default port)
--
-- * Use 'defaultRetryPolicy' to determine redis reconnect behavior.
--
-- * It will use \"job-queue:\" as a key prefix in redis. This should
-- almost always get set to something else.
defaultRedisConfig :: RedisConfig
defaultRedisConfig = RedisConfig
    { rcConnectInfo = (connectInfo "localhost")
          { connectRetryPolicy = Just defaultRetryPolicy }
    , rcKeyPrefix = "job-queue:"
    }

-- | This is the retry policy used for 'withRedis' and 'withSubscription'
-- reconnects. If it fails 10 reconnects, with 1 second between each,
-- then it gives up.
defaultRetryPolicy :: RetryPolicy
defaultRetryPolicy = limitRetries 10 <> constantDelay (1000 * 1000)

-- | A connection to redis, along with a prefix for keys.
data Redis = Redis
    { redisConnection :: !Connection
    -- ^ Connection to redis.
    , redisKeyPrefix :: !ByteString
    -- ^ Prefix to prepend to redis keys.
    }

-- * Heartbeat checker

-- | Configuration of heartbeats, used by both the checker and sender.
data HeartbeatConfig = HeartbeatConfig
    { hcSenderIvl :: !Seconds
    -- ^ How frequently heartbeats should be sent.
    , hcCheckerIvl :: !Seconds
    -- ^ How frequently heartbeats should be checked. Should be
    -- substantially larger than 'hcSenderIvl'.
    }

-- | Default settings for the heartbeat checker:
--
-- * Heartbeats are checked every 30 seconds.
--
-- * Heartbeats are sent every 15 seconds.
defaultHeartbeatConfig :: HeartbeatConfig
defaultHeartbeatConfig = HeartbeatConfig
    { hcSenderIvl = Seconds 15
    , hcCheckerIvl = Seconds 30
    }

-- * Information for connecting to a worker

data WorkerConnectInfo = WorkerConnectInfo
    { wciHost :: !ByteString
    , wciPort :: !Int
    }
    deriving (Eq, Show, Ord, Generic, Typeable)

instance Serialize WorkerConnectInfo

-- * Exceptions

-- | Exceptions which are returned to the client by the job-queue.
data DistributedException
    = WorkStillInProgress !WorkerId
    -- ^ Thrown when the worker stops being a master but there's still
    -- work on its active queue.  This occuring indicates an error in
    -- the library.
    | RequestMissingException !RequestId
    -- ^ Exception thrown when a worker can't find the request body.
    -- This means that the request body expired in redis
    -- (alternatively, it could indicate a bug in this library).
    | ResponseMissingException !RequestId
    -- ^ Exception thrown when the client can't find the response
    -- body. This means that the response body expired in redis
    -- (alternatively, it could indicate a bug in this library).
    | TypeMismatch
        { expectedRequestTypeFingerprint :: !TypeFingerprint
        , actualRequestTypeFingerprint :: !TypeFingerprint
        , expectedResponseTypeFingerprint :: !TypeFingerprint
        , actualResponseTypeFingerprint :: !TypeFingerprint
        }
    -- ^ Thrown when the client makes a request with the wrong request
    -- / response types.
    | NoRequestForCallbackRegistration !RequestId
    -- ^ Exception thrown when registering a callback for a non-existent
    -- request.
    | MismatchedRedisSchemaVersion
        { expectedRedisSchemaVersion :: !ByteString
        , actualRedisSchemaVersion :: !ByteString
        }
    -- ^ Exception thrown on initialization of work-queue
    | MismatchedRequestRedisSchemaVersion
        { expectedRequestRedisSchemaVersion :: !ByteString
        , actualRequestRedisSchemaVersion :: !ByteString
        , schemaMismatchRequestId :: !RequestId
        }
    -- ^ Exception thrown when request is received with the wrong schema
    -- version.
    | NoLongerWaitingForResult
    -- ^ Exception thrown by job-queue client functions that block
    -- waiting for response.
    | NoLongerWaitingForRequest
    -- ^ Exception thrown by worker functions that block waiting for
    -- request.
    | NoLongerWaitingForWorkerRequest
    -- ^ Exception thrown by worker functions that block waiting for
    -- worker requests.
    | RequestCanceled !RequestId
    -- ^ Exception returned to the user when the request has been
    -- cancelled, and is no longer being worked on.
    | NetworkMessageException !NetworkMessageException
    -- ^ Exceptions thrown by "Data.Streaming.NetworkMessage"
    | InternalJobQueueException !Text
    -- ^ Used for unexpected conditions.
    | OtherException !Text !Text
    -- ^ This is used to return exceptions to the client, when
    -- exceptions occur while running the job.
    deriving (Eq, Typeable, Generic)

instance Exception DistributedException
instance Serialize DistributedException

instance Show DistributedException where
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
    show NoLongerWaitingForWorkerRequest = concat
        [ "NoLongerWaitingForWorkerRequest "
        , "{- This is usually because we lost the connection to redis, and couldn't reconnect. -}"
        ]
    show (RequestCanceled rid) =
        "RequestCanceled (" ++ show rid ++ ")"
    show (NetworkMessageException nme) =
        "NetworkMessageException (" ++ show nme ++ ")"
    show (InternalJobQueueException txt) =
        "InternalJobQueueException " ++ show txt
    show (OtherException ty txt) =
        "OtherException " ++ show ty ++ " " ++ show txt
