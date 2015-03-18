{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TemplateHaskell #-}

-- | This module handles the implementation of heartbeat check /
-- recovery for "Distributed.RedisQueue".
module Distributed.JobQueue.Heartbeat
    ( sendHeartbeats
    , checkHeartbeats
    , deactivateWorker
    ) where

import           ClassyPrelude
import           Control.Monad.Logger (MonadLogger, logErrorS)
import           Data.Binary (encode)
import           Data.List.NonEmpty (NonEmpty((:|)), nonEmpty)
import           Distributed.JobQueue.Shared
import           Distributed.RedisQueue
import           Distributed.RedisQueue.Internal
import           FP.Redis
import           FP.Redis.Mutex

-- | This listens for a notification telling the worker to send a
-- heartbeat.  In this case, that means the worker needs to remove its
-- key from a Redis set.  If this doesn't happen in a timely fashion,
-- then the worker will be considered to be dead, and its work items
-- get re-enqueued.
--
-- The @TVar Bool@ is changed to 'True' once the subscription is made
-- and the 'WorkerId' has been added to the list of active workers.
sendHeartbeats
    :: MonadConnect m => RedisInfo -> WorkerId -> TVar Bool -> m void
sendHeartbeats r wid ready = do
    let sub = subscribe (heartbeatChannel r :| [])
    withSubscriptionsWrapped (redisConnectInfo r) (sub :| []) $ \msg ->
        case msg of
            Subscribe {} -> do
                run_ r $ sadd (heartbeatActiveKey r) (unWorkerId wid :| [])
                atomically $ writeTVar ready True
            Unsubscribe {} ->
                atomically $ writeTVar ready False
            Message {} ->
                run_ r $ srem (heartbeatInactiveKey r) (unWorkerId wid :| [])

-- | Periodically check worker heartbeats.  This uses
-- 'periodicActionWrapped' to share the responsibility of checking the
-- heartbeats amongst multiple client servers.  All invocations of
-- this should use the same time interval.
checkHeartbeats
    :: MonadConnect m => RedisInfo -> Seconds -> m void
checkHeartbeats r ivl =
    periodicActionWrapped (redisConnection r) (heartbeatTimeKey r) ivl $ do
        -- Check if the last iteration of this heartbeat check ran
        -- successfully.  If it did, then we can use the contents of
        -- the inactive list.  The flag also gets set to False here,
        -- such that if a failure happens in the middle, the next run
        -- will know to not use the data.
        functioning <-
            run r (getset (heartbeatFunctioningKey r) (toStrict (encode False)))
        inactive <- if functioning == Just (toStrict (encode True))
            then do
                -- Fetch the list of inactive workers and move their
                -- jobs back to the requests queue.  If we re-enqueued
                -- some requests, then send out a notification about
                -- it.
                inactive <- run r $ smembers (heartbeatInactiveKey r)
                reenquedSome <- any id <$>
                    mapM (handleWorkerFailure r . WorkerId) inactive
                when reenquedSome $ notifyRequestAvailable r
                return inactive
            else do
                -- The reasoning here is that if the last heartbeat
                -- check failed, it might have enqueued requests.  We
                -- check if there are any, and if so, send a
                -- notification.
                requestsCount <- run r $ llen (requestsKey r)
                when (requestsCount > 0) $ notifyRequestAvailable r
                return []
        -- Remove the inactive workers from the list of workers.
        mapM_ (run_ r . srem (heartbeatActiveKey r)) (nonEmpty inactive)
        -- Populate the list of inactive workers for the next
        -- heartbeat.
        workers <- run r $ smembers (heartbeatActiveKey r)
        run_ r $ del (unSKey (heartbeatInactiveKey r) :| [])
        mapM_ (run_ r . sadd (heartbeatInactiveKey r)) (nonEmpty workers)
        -- Ask all of the workers to remove their IDs from the inactive
        -- list.
        run_ r $ publish (heartbeatChannel r) ""
        -- Record that the heartbeat check was successful.
        run_ r $ set (heartbeatFunctioningKey r) (toStrict (encode True)) []

handleWorkerFailure
    :: (MonadCommand m, MonadLogger m) => RedisInfo -> WorkerId -> m Bool
handleWorkerFailure r wid = do
    $logErrorS "JobQueue" $ tshow wid <>
        " failed to respond to heartbeat.  Re-enquing its items."
    let k = activeKey r wid
    requests <- run r $ lrange k 0 (-1)
    mapM_ (run_ r . rpush (requestsKey r)) (nonEmpty requests)
    -- Delete the active list after re-enquing is successful.
    -- This way, we can't lose data.
    run_ r $ del (unLKey (activeKey r wid) :| [])
    return $ isJust (nonEmpty requests)

-- | This is used to remove the worker from the set of workers checked
-- for heartbeats.  It's used after a worker stops being a master.
--
-- It throws a 'WorkStillInProgress' exception if there is enqueued
-- work, so callers should ensure that this isn't the case.
--
-- The usage of this function in 'jobQueueWorker' is guaranteed to not
-- throw this exception, because it is called after 'sendResponse',
-- which removes the work from the active queue.
deactivateWorker
    :: (MonadCommand m, MonadThrow m) => RedisInfo -> WorkerId -> m ()
deactivateWorker r wid = do
    activeCount <- run r $ llen (activeKey r wid)
    when (activeCount /= 0) $ throwM (WorkStillInProgress wid)
    run_ r $ srem (heartbeatActiveKey r) (unWorkerId wid :| [])

-- * Functions to compute Redis keys

heartbeatInactiveKey, heartbeatActiveKey :: RedisInfo -> SKey
-- A set of 'WorkerId' who have not yet removed their keys (indicating
-- that they're still alive and responding to heartbeats).
heartbeatInactiveKey r = SKey $ Key $ redisKeyPrefix r <> "heartbeat:inactive"
-- A set of 'WorkerId's that are currently thought to be running.
heartbeatActiveKey r = SKey $ Key $ redisKeyPrefix r <> "heartbeat:active"

-- Stores a "Data.Binary" encoded 'Bool'.
heartbeatFunctioningKey :: RedisInfo -> VKey
heartbeatFunctioningKey r = VKey $ Key $ redisKeyPrefix r <> "heartbeat:functioning"

-- Channel used for requesting that the workers remove their
-- 'WorkerId' from the set at 'heartbeatInactiveKey'.
heartbeatChannel :: RedisInfo -> Channel
heartbeatChannel r = Channel $ redisKeyPrefix r <> "heartbeat:channel"

-- Prefix used for the 'periodicActionWrapped' invocation, which is
-- used to share the responsibility of periodically checking
-- heartbeats.
heartbeatTimeKey :: RedisInfo -> PeriodicPrefix
heartbeatTimeKey r = PeriodicPrefix $ redisKeyPrefix r <> "heartbeat:time"
