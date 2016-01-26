{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TemplateHaskell #-}

module Distributed.JobQueue.Client.NewApi
    ( JobClientConfig(..)
    , defaultJobClientConfig
    , JobClient
    , newJobClient
    , submitRequest
    , waitForResponse
    , checkForResponse
    , LogFunc
    ) where

import ClassyPrelude
import Control.Concurrent (forkIO)
import Control.Monad.Logger
import Control.Retry (RetryPolicy)
import Data.Streaming.NetworkMessage (Sendable)
import Data.Void (absurd)
import Distributed.JobQueue.Client hiding (checkForResponse)
import qualified Distributed.JobQueue.Client as Client
import Distributed.RedisQueue.Internal
import FP.Redis

data JobClientConfig = JobClientConfig
    { jccRedisHost :: ByteString
      -- ^ Host name of the redis server.
    , jccRedisPort :: Int
      -- ^ Port the redis server is listening on.
    , jccRedisPrefix :: ByteString
      -- ^ Prefix used for redis keys.  Should be unique to this job-queue.
    , jccRedisRetryPolicy :: RetryPolicy
      -- ^ How to try reconnecting to the redis server.
    , jccHeartbeatCheckIvl :: Seconds
      -- ^ How often to send heartbeat requests to the workers, and
      -- check for responses.  This value should be the same for all
      -- clients.
    , jccHeartbeatFailureExpiry :: Seconds
      -- ^ How long a heartbeat failure should stick around.  This should be a
      -- quite large amount of time, as the worker might eventually reconnect,
      -- and it should know that it has been heartbeat-failure collected.
      -- Garbage collecting them to save resources doesn't matter very much.
      -- The main reason it matters is so that the UI doesn't end up with tons
      -- of heartbeat failure records.
    , jccRequestExpiry :: Seconds
      -- ^ The expiry time of the request data stored in redis.
    }

-- | A default client configuration:
--
-- * It will attempt to connect to redis at localhost:6379
--
-- * It will use \"job-queue:\" as a key prefix in redis.  This should almost
--   always get set to something else.
--
-- * It will use 'defaultRetryPolicy' to determine redis reconnect behavior.
--
-- * Heartbeats are checked every 30 seconds
--
-- * Heartbeat failures expire after 1 day.
--
-- * Request bodies expire in redis after 1 hour
defaultJobClientConfig :: JobClientConfig
defaultJobClientConfig = JobClientConfig
    { jccRedisHost = "localhost"
    , jccRedisPort = 6379
    , jccRedisPrefix = "job-queue:"
    , jccRedisRetryPolicy = defaultRetryPolicy
    , jccHeartbeatCheckIvl = Seconds 30
    , jccHeartbeatFailureExpiry = Seconds (24 * 3600)
    , jccRequestExpiry = Seconds 3600
    }

type LogFunc = Loc -> LogSource -> LogLevel -> LogStr -> IO ()

-- | Start a new job queue client, which will run forever.  For most usecases,
-- this should only be invoked once for the process, usually on
-- initialization.
newJobClient
    :: (MonadIO m, Sendable response)
    => LogFunc
    -> JobClientConfig
    -> m (JobClient response)
newJobClient logFunc JobClientConfig{..} = liftIO $ do
    let config = ClientConfig
            { clientHeartbeatCheckIvl = jccHeartbeatCheckIvl
            , clientHeartbeatFailureExpiry = jccHeartbeatFailureExpiry
            , clientRequestExpiry = jccRequestExpiry
            }
        ci = (connectInfo jccRedisHost)
            { connectPort = jccRedisPort
            , connectRetryPolicy = Just jccRedisRetryPolicy
            }
    cvs <- newClientVars
    _ <- forkIO $ forever $ flip runLoggingT logFunc $ do
        eres <- tryAny $ withRedis jccRedisPrefix ci $ jobQueueClient config cvs
        case eres of
            Right x -> absurd x
            Left err -> do
                $logErrorS "JobClient" (pack (show err))
                $logInfoS "JobClient" "Restarting job client after exception."
    conn <- runLoggingT (connect ci) logFunc
    return JobClient
        { jcConfig = config
        , jcClientVars = cvs
        , jcRedis = RedisInfo conn ci jccRedisPrefix
        , jcLogFunc = logFunc
        }

data JobClient response = JobClient
    { jcConfig :: ClientConfig
    , jcClientVars :: ClientVars (LoggingT IO) response
    , jcRedis :: RedisInfo
    , jcLogFunc :: LogFunc
    }

-- | Submits a new request. Returns a 'Just' if the response to the request is already
-- ready (e.g. if a request with the given 'RequestId' was already submitted and processed).
submitRequest ::
       (MonadCommand m, Sendable request, Sendable response)
    => JobClient response
    -> RequestId
    -> request
    -> m (Maybe response)
submitRequest JobClient{..} reqId request =
    runLoggingT (sendRequestWithId jcConfig jcRedis reqId request) jcLogFunc

-- | Blocks until a response for the given 'RequestId' is available.
waitForResponse ::
       (MonadIO m, Sendable response)
    => JobClient response
    -> RequestId
    -> m (Either DistributedJobQueueException response)
waitForResponse JobClient{..} rid = liftIO $ do
    mv <- newEmptyMVar
    runLoggingT (registerResponseCallback jcClientVars jcRedis rid (putMVar mv)) jcLogFunc
    takeMVar mv

-- | Returns immediately with the request, if present.
checkForResponse ::
       (MonadIO m, Sendable response)
    => JobClient response
    -> RequestId
    -> m (Maybe (Either DistributedJobQueueException response))
checkForResponse JobClient{..} rid = liftIO (runLoggingT (Client.checkForResponse jcRedis rid) jcLogFunc)

{- TODO: implement something like this?  Then we won't get as much of a
guarantee that having a 'JobClient' means the job client threads are
running.  Could have *uses* of JobClient throw exceptions for this case,
though.

-- Unexported exception used to cancel jobQueueClient
data HaltJobClient = HaltJobClient
    deriving (Show, Typeable)

instance Exception HaltJobClient

haltJobClient :: JobClientMonad m => JobClient -> m ()
haltJobClient
-}
