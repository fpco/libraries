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
    ) where

import ClassyPrelude
import Control.Concurrent.Lifted (threadDelay)
import Control.Monad.Logger (MonadLogger, logWarnS)
import Data.Binary (encode)
import Data.List.NonEmpty (NonEmpty((:|)), nonEmpty)
import Distributed.JobQueue.Shared
import Distributed.RedisQueue
import Distributed.RedisQueue.Internal
import FP.Redis
import FP.Redis.Mutex
import FP.ThreadFileLogger

-- | This periodically removes the worker's key from the set of
-- inactive workers.  This set is periodically re-initialized and
-- checked by 'checkHeartbeats'.  Note that using the same 'Seconds'
-- time interval for both of these functions will be unreliable.
-- Instead, a lesser time interval should be passed to
-- 'sendHeartbeats', to be sure that the heartbeat is seen by every
-- iteration of 'checkHeartbeats'.
--
-- This function is cancellable.  When it receives an exception, it
-- removes the worker from the set which are actively checked via
-- heartbeats.  If there's active work, then it throws
-- 'WorkStillInProgress', but still halts the heartbeats.  When this
-- happens, the heartbeat checker will re-enqueue the items.  The
-- occurence of this error likely indicates misuse of sendHeartbeats,
-- where it gets cancelled before work is done.
sendHeartbeats
    :: MonadConnect m => RedisInfo -> Seconds -> WorkerId -> m void
sendHeartbeats r (Seconds ivl) wid = sender `finally` deactivate
  where
    sender = do
        run_ r $ sadd (heartbeatActiveKey r) (unWorkerId wid :| [])
        forever $ do
            liftIO $ threadDelay ((fromIntegral ivl `max` 1) * 1000 * 1000)
            run_ r $ srem (heartbeatInactiveKey r) (unWorkerId wid :| [])
    deactivate = do
        activeCount <- run r $ llen (activeKey r wid)
        when (activeCount /= 0) $ liftIO $ throwIO (WorkStillInProgress wid)
        run_ r $ srem (heartbeatActiveKey r) (unWorkerId wid :| [])

-- | Periodically check worker heartbeats.  This uses
-- 'periodicActionWrapped' to share the responsibility of checking the
-- heartbeats amongst multiple client servers.  All invocations of
-- this should use the same time interval.
checkHeartbeats
    :: MonadConnect m => RedisInfo -> Seconds -> m void
checkHeartbeats r ivl =
    periodicActionWrapped (redisConnection r) (heartbeatTimeKey r) ivl $ logNest "checkHeartbeats" $ do
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
                $logWarnS "JobQueue" $
                    "Last heartbeat failed (or this is the first)"
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
        -- Record that the heartbeat check was successful.
        run_ r $ set (heartbeatFunctioningKey r) (toStrict (encode True)) []

handleWorkerFailure
    :: (MonadCommand m, MonadLogger m) => RedisInfo -> WorkerId -> m Bool
handleWorkerFailure r wid = do
    let k = activeKey r wid
    requests <- run r $ lrange k 0 (-1)
    if null requests
        then $logWarnS "JobQueue" $ tshow wid <>
            " failed its heartbeat, but didn't have items to re-enqueue."
        else $logWarnS "JobQueue" $ tshow wid <>
            " failed its heartbeat.  Re-enqueuing its items."
    mapM_ (run_ r . rpush (requestsKey r)) (nonEmpty requests)
    -- Delete the active list after re-enquing is successful.
    -- This way, we can't lose data.
    run_ r $ del (unLKey (activeKey r wid) :| [])
    return $ isJust (nonEmpty requests)



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

-- Prefix used for the 'periodicActionWrapped' invocation, which is
-- used to share the responsibility of periodically checking
-- heartbeats.
heartbeatTimeKey :: RedisInfo -> PeriodicPrefix
heartbeatTimeKey r = PeriodicPrefix $ redisKeyPrefix r <> "heartbeat:time"
