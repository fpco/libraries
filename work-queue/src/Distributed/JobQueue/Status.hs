{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Distributed.JobQueue.Status
    ( JobQueueStatus(..)
    , WorkerStatus(..)
    , RequestStatus(..)
    , getJobQueueStatus
    -- * Stats
    , RequestStats(..)
    , getRequestStats
    , getAllRequestStats
    , getAllRequests
    , getActiveWorkers
    -- * Utilities for hpc-manager
    , clearHeartbeatFailure
    -- * Re-exports from Shared
    , RequestEvent(..)
    , getRequestEvents
    ) where

import ClassyPrelude hiding (keys)
import qualified Data.ByteString.Char8 as S8
import Data.Char (isAlphaNum)
import Data.List.NonEmpty (NonEmpty((:|)))
import Data.Time
import Data.Time.Clock.POSIX
import Distributed.JobQueue.Heartbeat (heartbeatActiveKey, heartbeatLastCheckKey)
import Distributed.JobQueue.Shared
import Distributed.RedisQueue
import Distributed.RedisQueue.Internal
import FP.Redis

data JobQueueStatus = JobQueueStatus
    { jqsLastHeartbeat :: !(Maybe UTCTime)
    , jqsPending :: ![RequestId]
    , jqsWorkers :: ![WorkerStatus]
    } deriving Show

data WorkerStatus = WorkerStatus
    { wsWorker :: !WorkerId
    , wsHeartbeatFailure :: !Bool
    , wsLastHeartbeat :: !(Maybe UTCTime)
    , wsRequests :: ![RequestId]
    } deriving Show

data RequestStatus = RequestStatus
    { rsId :: !RequestId
    , rsStart :: !(Maybe POSIXTime)
    } deriving Show

getJobQueueStatus :: MonadCommand m => RedisInfo -> m JobQueueStatus
getJobQueueStatus r = do
    checkRedisSchemaVersion r
    mLastTime <- getRedisTime r (heartbeatLastCheckKey r)
    pending <- run r $ lrange (requestsKey r) 0 (-1)
    wids <- getActiveWorkers r
    workers <- forM wids $ \wid -> do
        mtime <- fmap (fmap (posixSecondsToUTCTime . realToFrac)) <$>
            run r $ zscore (heartbeatActiveKey r) (unWorkerId wid)
        erequests <- try $ run r $ lrange (LKey (activeKey r wid)) 0 (-1)
        case erequests of
            -- Indicates heartbeat failure
            Left (CommandException (isPrefixOf "WRONGTYPE" -> True)) ->
                return WorkerStatus
                    { wsWorker = wid
                    , wsHeartbeatFailure = True
                    , wsLastHeartbeat = mtime
                    , wsRequests = []
                    }
            Left ex -> liftIO $ throwIO ex
            Right requests -> do
                return WorkerStatus
                    { wsWorker = wid
                    , wsHeartbeatFailure = False
                    , wsLastHeartbeat = mtime
                    , wsRequests = map RequestId requests
                    }
    return JobQueueStatus
        { jqsLastHeartbeat = fmap posixSecondsToUTCTime mLastTime
        , jqsPending = map RequestId pending
        , jqsWorkers = workers
        }

clearHeartbeatFailure :: MonadCommand m => RedisInfo -> WorkerId -> m ()
clearHeartbeatFailure r wid = do
     let k = activeKey r wid
     eres <- try $ run r $ get (VKey k)
     case eres of
         Left CommandException{}  -> return ()
         Left ex -> liftIO $ throwIO ex
         -- Indicates heartbeat failure
         Right _ -> void $ run r $ del (k :| [])

getActiveWorkers :: MonadCommand m => RedisInfo -> m [WorkerId]
getActiveWorkers r = do
    let activePrefix = redisKeyPrefix r <> "active:"
    activeKeys <- run r $ keys (activePrefix <> "*")
    return $ mapMaybe (fmap WorkerId . stripPrefix activePrefix . unKey) activeKeys

data RequestStats = RequestStats
    { rsEnqueueTime :: Maybe UTCTime -- ^ FIXME: figure out why this can be Nothing 0_0
    , rsReenqueueCount :: Int
    , rsComputeStartTime :: Maybe UTCTime
    , rsComputeFinishTime :: Maybe UTCTime
    , rsComputeTime :: Maybe NominalDiffTime
    , rsTotalTime :: Maybe NominalDiffTime
    , rsFetchCount :: Int
    }

getRequestStats :: MonadCommand m => RedisInfo -> RequestId -> m (Maybe RequestStats)
getRequestStats r k = do
    evs <- getRequestEvents r k
    case evs of
        [] -> return Nothing
        _ -> return $ Just RequestStats {..}
          where
            rsEnqueueTime = lastMay [x | (x, RequestEnqueued) <- evs]
            rsReenqueueCount = max 0 (length [() | (_, RequestWorkStarted _) <- evs] - 1)
            rsComputeStartTime = lastMay [x | (x, RequestWorkStarted _) <- evs]
            rsComputeFinishTime = lastMay [x | (x, RequestWorkFinished _) <- evs]
            rsComputeTime = diffUTCTime <$> rsComputeFinishTime <*> rsComputeStartTime
            rsTotalTime = diffUTCTime <$> rsComputeFinishTime <*> rsEnqueueTime
            rsFetchCount = length [() | (_, RequestResponseRead) <- evs]

getAllRequests :: MonadCommand m => RedisInfo -> m [RequestId]
getAllRequests r =
    fmap (mapMaybe (fmap RequestId . (stripSuffix ":events" =<<) . stripPrefix requestPrefix . unKey)) $
    run r (keys (requestPrefix <> "*"))
  where
    requestPrefix = redisKeyPrefix r <> "request:"
    isBase64 c = isAlphaNum c || c `elem` ['+','/']

getAllRequestStats :: MonadCommand m => RedisInfo -> m [(RequestId, RequestStats)]
getAllRequestStats r =
    fmap catMaybes . mapM (\k -> fmap (k,) <$> getRequestStats r k) =<< getAllRequests r
