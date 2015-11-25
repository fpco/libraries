{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts #-}

module Distributed.JobQueue.Status
    ( JobQueueStatus(..)
    , WorkerStatus(..)
    , RequestStatus(..)
    , getJobQueueStatus
    , getRequestStatus
    ) where

import ClassyPrelude hiding (keys)
import Control.Monad.Logger
import Control.Monad.Trans.Control (MonadBaseControl)
import Data.Time
import Data.Time.Clock.POSIX
import Distributed.JobQueue.Heartbeat (heartbeatActiveKey, heartbeatLastCheckKey)
import Distributed.RedisQueue
import Distributed.RedisQueue.Internal
import FP.Redis

data JobQueueStatus = JobQueueStatus
    { jqsLastHeartbeat :: !(Maybe NominalDiffTime)
    , jqsPending :: ![RequestId]
    , jqsWorkers :: ![WorkerStatus]
    } deriving Show

data WorkerStatus = WorkerStatus
    { wsWorker :: !WorkerId
    , wsHeartbeatFailure :: !Bool
    , wsLastHeartbeat :: !(Maybe NominalDiffTime)
    , wsRequests :: ![RequestId]
    } deriving Show

data RequestStatus = RequestStatus
    { rsId :: !RequestId
    , rsStart :: !(Maybe POSIXTime)
    } deriving Show

getJobQueueStatus :: (MonadIO m, MonadCatch m, MonadBaseControl IO m, MonadLogger m)
                  => RedisInfo -> m JobQueueStatus
getJobQueueStatus r = do
    start <- liftIO getCurrentTime
    let howLongAgo time = start `diffUTCTime` posixSecondsToUTCTime time
    mLastTime <- getRedisTime r (heartbeatLastCheckKey r)
    pending <- run r $ lrange (requestsKey r) 0 (-1)
    let activePrefix = redisKeyPrefix r <> "active:"
    activeKeys <- run r $ keys (activePrefix <> "*")
    workers <- forM activeKeys $ \active ->
        forM (fmap WorkerId (stripPrefix activePrefix (unKey active))) $ \wid -> do
            mtime <- fmap (fmap (howLongAgo . realToFrac)) $
                run r $ zscore (heartbeatActiveKey r) (unWorkerId wid)
            erequests <- try $ run r $ lrange (LKey active) 0 (-1)
            case erequests of
                -- Indicates heartbeat failure
                Left (CommandException (isPrefixOf "WRONGTYPE" -> True)) ->
                    return WorkerStatus
                        { wsWorker = wid
                        , wsHeartbeatFailure = True
                        , wsLastHeartbeat = mtime
                        , wsRequests = []
                        }
                Left ex -> throwM ex
                Right requests -> do
                    return WorkerStatus
                        { wsWorker = wid
                        , wsHeartbeatFailure = False
                        , wsLastHeartbeat = mtime
                        , wsRequests = map RequestId requests
                        }
    return JobQueueStatus
        { jqsLastHeartbeat = fmap howLongAgo mLastTime
        , jqsPending = map RequestId pending
        , jqsWorkers = catMaybes workers
        }

getRequestStatus :: MonadCommand m => RedisInfo -> RequestId -> m RequestStatus
getRequestStatus r rid = RequestStatus
    <$> pure rid
    <*> getRedisTime r (requestTimeKey r rid)
