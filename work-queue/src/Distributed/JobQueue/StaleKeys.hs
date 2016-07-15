{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-|
Module: Distributed.JobQueue.StaleKeys
Description: Re-enqueue Jobs from workers that died after being considered dead by the heartbeat checker.

The heartbeat checker in "Distributed.Heartbeat" periodically checks
workers for their heartbeats, and makes sure that jobs are re-enqueued
whenever a worker fails its heartbeat check.

However, there is one mode if failure not handled by the heartbeat
checker (see github issue #133):

- If A worker fails its heartbeat check, but is actually still
  working, it will be considered dead by the heartbeat checker, and
  its heartbeats will no longer be checked.

- If the worker starts working on a new job after being declared dead,
  and then truly dies at a later time, the heartbeat checker will not
  re-enqueue the request: the heartbeats of the worker will no longer
  be checked, since it is already thought to be dead.

Under this condition, the job that the worker was handling at the time
it failed would stay in the queue indefinitely.


This module provides a mechanism for periodically checking for jobs
that are assigned to workers that are no longer alive, re-enqueuing as
necessary.
-}
module Distributed.JobQueue.StaleKeys where

import           ClassyPrelude hiding (keys)
import           Control.Concurrent.Lifted (threadDelay)
import           Control.Monad.Logger
import qualified Data.HashSet as HashSet
import           Distributed.Heartbeat
import           Distributed.JobQueue.Internal
import           Distributed.Redis
import           Distributed.Types
import           FP.Redis
import           FP.ThreadFileLogger

-- | Repeatedly check for workers that are considered dead, but are
-- listed as handling a job.
--
-- Re-enqueues jobs assigned to dead workers.
checkStaleKeys :: MonadConnect m
                  => JobQueueConfig
                  -> Redis
                  -> m void
checkStaleKeys config r = logNest "checkStaleKeys" $ forever $ do
    liveWorkers <- HashSet.fromList <$> activeOrUnhandledWorkers r
    let keyPrefix = allActiveKeyPattern r
    activeKeys <- run r $ keys keyPrefix
    workersWithJobs <- fmap HashSet.fromList $ forM activeKeys $ \k ->
        case workerIdFromActiveKey r k of
            Nothing -> liftIO . throwIO . InternalJobQueueException $
                "failed to convert activeKey " ++ pack (show k) ++ " to WorkerId."
            Just wid -> return wid
    let staleKeys = HashSet.toList $ workersWithJobs `HashSet.difference` liveWorkers
    forM_ staleKeys $ \wid -> do
        mbRid <- run r (rpoplpush (activeKey r wid) (requestsKey r))
        case mbRid of
            Nothing -> $logWarnS "JobQueue" $ tshow wid <> " is not active anymore, and does not have a job."
            Just rid -> do
                addRequestEvent r (RequestId rid) (RequestWorkReenqueuedAsStale wid)
                $logWarnS "JobQueue" $ tshow wid <> " is not active anymore, and " <> tshow rid <> " was re-enqueued."
    threadDelay (1000000 * (fromIntegral . unSeconds . jqcCheckStaleKeysInterval $ config))
    checkStaleKeys config r
