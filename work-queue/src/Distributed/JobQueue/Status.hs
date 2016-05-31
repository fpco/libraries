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
    -- * Re-exports from Shared
    , RequestEvent(..)
    , getRequestEvents
    ) where

import           ClassyPrelude hiding (keys)
import qualified Data.Text as T
import           Data.Time
import           Data.Time.Clock.POSIX
import           Distributed.Heartbeat
import           Distributed.JobQueue.Internal
import           Distributed.Redis
import           Distributed.Types
import           FP.Redis

data JobQueueStatus = JobQueueStatus
    { jqsLastHeartbeat :: !(Maybe UTCTime)
    -- REVIEW: This is the last time a Client checked an heartbeat from a worker.
    , jqsPending :: ![RequestId]
    , jqsWorkers :: ![WorkerStatus]
    -- ^ All workers that have passed their last heartbeat check
    -- (those performing work, and those that are idle).
    , jqsHeartbeatFailures :: ![(UTCTime, WorkerId)]
    -- ^ Chronological list of hearbeat failures
    } deriving Show

data WorkerStatus = WorkerStatus
    { wsWorker :: !WorkerId
    , wsLastHeartbeat :: !(Maybe UTCTime)
    -- ^ REVIEW: This is the last heartbeat _sent_ by the worker (it might have not been
    -- received by anyone).
    , wsRequest :: !(Maybe RequestId)
    } deriving Show

data RequestStatus = RequestStatus
    { rsId :: !RequestId
    , rsStart :: !(Maybe POSIXTime)
    } deriving Show

getJobQueueStatus :: MonadConnect m => Redis -> m JobQueueStatus
getJobQueueStatus r = do
    checkRedisSchemaVersion r
    mLastTime <- lastHeartbeatCheck r
    pending <- run r $ lrange (requestsKey r) 0 (-1)
    wids <- activeOrUnhandledWorkers r
    inactiveWids <- deadWorkers r
    -- We could instead use zrange with WITHSCORES, to retrieve the
    -- scores together with the items (and parse the resulting
    -- 'ByteString's.
    heartbeatFailures <- liftM catMaybes $ mapM
        (\ wid -> do
                mfailure <- lastHeartbeatFailureForWorker r wid
                case mfailure of
                    Just t -> return (Just (t, wid))
                    Nothing -> return Nothing
        ) inactiveWids
    workers <- forM wids $ \wid -> do
        mtime <- lastHeartbeatForWorker r wid
        request <- getWorkerRequest r wid
        return WorkerStatus
            { wsWorker = wid
            , wsLastHeartbeat = mtime
            , wsRequest = request
            }
    return JobQueueStatus
        { jqsLastHeartbeat = mLastTime
        , jqsPending = map RequestId pending
        , jqsWorkers = workers
        , jqsHeartbeatFailures = heartbeatFailures
        }

getWorkerRequest :: MonadConnect m => Redis -> WorkerId -> m (Maybe RequestId)
getWorkerRequest r wid = do
    erequest <- try . run r $ lrange (activeKey r wid) 0 (-1)
    case erequest of
        Right [] -> return Nothing
        Right [request] -> return (Just (RequestId request))
        Right requests -> liftIO . throwIO . InternalJobQueueException $
            T.intercalate " " ["Illegal requests list:"
                              , T.pack . show $ requests
                              , "-- a request list should have zero or one element."]
        Left (ex :: SomeException) -> liftIO . throwIO $ ex

data RequestStats = RequestStats
    { rsEnqueueTime :: Maybe UTCTime
    , rsReenqueueCount :: Int
    , rsComputeStartTime :: Maybe UTCTime
    , rsComputeFinishTime :: Maybe UTCTime
    , rsComputeTime :: Maybe NominalDiffTime
    , rsTotalTime :: Maybe NominalDiffTime
    , rsFetchCount :: Int
    -- REVIEW: This is the number of times the request has been read by the client.
    }

getRequestStats :: MonadConnect m => Redis -> RequestId -> m (Maybe RequestStats)
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

getAllRequests :: MonadConnect m => Redis -> m [RequestId]
getAllRequests r =
    fmap (mapMaybe (fmap RequestId . (stripSuffix ":events" =<<) . stripPrefix requestPrefix . unKey)) $
    run r (keys (requestPrefix <> "*"))
  where
    requestPrefix = redisKeyPrefix r <> "request:"

getAllRequestStats :: MonadConnect m => Redis -> m [(RequestId, RequestStats)]
getAllRequestStats r =
    fmap catMaybes . mapM (\k -> fmap (k,) <$> getRequestStats r k) =<< getAllRequests r
