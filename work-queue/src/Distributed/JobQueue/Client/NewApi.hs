{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TemplateHaskell #-}

module Distributed.JobQueue.Client.NewApi
    ( JobClientMonad
    , JobClientConfig(..)
    , defaultJobClientConfig
    , JobClient
    , newJobClient
    , submitRequest
    ) where

import ClassyPrelude
import Control.Concurrent.Lifted
import Control.Concurrent.STM hiding (atomically)
import Control.Monad.Logger
import Control.Monad.Trans.Control (MonadBaseControl)
import Control.Retry (RetryPolicy)
import Data.Streaming.NetworkMessage (Sendable)
import Data.Void (absurd)
import Distributed.JobQueue.Client
import Distributed.JobQueue.Shared
import Distributed.RedisQueue.Internal
import FP.Redis

type JobClientMonad m = (MonadIO m, MonadLogger m, MonadBaseControl IO m, MonadCatch m)

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
-- * Request bodies expire in redis after 1 hour
defaultJobClientConfig :: JobClientConfig
defaultJobClientConfig = JobClientConfig
    { jccRedisHost = "localhost"
    , jccRedisPort = 6379
    , jccRedisPrefix = "job-queue:"
    , jccRedisRetryPolicy = defaultRetryPolicy
    , jccHeartbeatCheckIvl = Seconds 30
    , jccRequestExpiry = Seconds 3600
    }

-- | Start a new job queue client, which will run forever.  For most usecases,
-- this should only be invoked once for the process, usually on
-- initialization.
newJobClient
    :: (JobClientMonad m, Sendable response)
    => JobClientConfig
    -> m (JobClient m response)
newJobClient JobClientConfig{..} = do
    let config = ClientConfig
            { clientHeartbeatCheckIvl = jccHeartbeatCheckIvl
            , clientRequestExpiry = jccRequestExpiry
            }
        ci = (connectInfo jccRedisHost)
            { connectPort = jccRedisPort
            , connectRetryPolicy = Just jccRedisRetryPolicy
            }
    cvs <- liftIO newClientVars
    _ <- fork $ forever $ do
        eres <- tryAny $ withRedis jccRedisPrefix ci $ jobQueueClient config cvs
        case eres of
            Right x -> absurd x
            Left err -> do
                $logErrorS "JobClient" (pack (show err))
                $logInfoS "JobClient" "Restarting job client after exception."
    conn <- connect ci
    return JobClient
        { jcConfig = config
        , jcClientVars = cvs
        , jcRedis = RedisInfo conn ci jccRedisPrefix
        }

data JobClient m response = JobClient
    { jcConfig :: ClientConfig
    , jcClientVars :: ClientVars m response
    , jcRedis :: RedisInfo
    }

submitRequest
    :: (JobClientMonad m, Sendable response, Sendable request)
    => JobClient m response
    -> RequestId
    -> request
    -> m (STM (Maybe (Either DistributedJobQueueException response)))
submitRequest JobClient{..} rid req = do
    -- FIXME: Avoid this re-wrapping of MVar as a TMVar.  Also avoid re-
    -- Either-ifying the exception.
    responseVar <- liftIO (newTVarIO Nothing)
    _ <- fork $ do
        response <- try $ jobQueueRequestWithId jcConfig jcClientVars jcRedis rid req
        atomically $ writeTVar responseVar (Just response)
    return (readTVar responseVar)

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
