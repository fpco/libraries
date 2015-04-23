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
    , deactivateHeartbeats
    , recoverFromHeartbeatFailure
    , checkHeartbeats
    ) where

import ClassyPrelude
import Control.Concurrent.Lifted (threadDelay)
import Control.Monad.Logger (MonadLogger, logInfoS, logWarnS, logErrorS)
import Data.Binary (encode)
import Data.List.NonEmpty (NonEmpty((:|)), nonEmpty)
import Distributed.JobQueue.Periodic (periodicWrapped)
import Distributed.JobQueue.Shared
import Distributed.RedisQueue
import Distributed.RedisQueue.Internal
import FP.Redis
import FP.Redis.Mutex
import FP.ThreadFileLogger
import qualified Data.ByteString.Char8 as BS8

-- | This periodically removes the worker's key from the set of
-- inactive workers.  This set is periodically re-initialized and
-- checked by 'checkHeartbeats'.
--
-- Note that using the same 'Seconds' time interval for both
-- 'checkHeartbeats' and 'sendHeartbeats' will be unreliable.
-- Instead, a lesser time interval should be passed to
-- 'sendHeartbeats', to be sure that the heartbeat is seen by every
-- iteration of 'checkHeartbeats'.
sendHeartbeats
    :: MonadConnect m => RedisInfo -> Seconds -> WorkerId -> m void
sendHeartbeats r (Seconds ivl) wid = do
    run_ r $ sadd (heartbeatActiveKey r) (unWorkerId wid :| [])
    forever $ do
        liftIO $ threadDelay ((fromIntegral ivl `max` 1) * 1000 * 1000)
        run_ r $ srem (heartbeatInactiveKey r) (unWorkerId wid :| [])

-- | This removes the worker from the set which are actively checked via
-- heartbeats.  If there's active work, then it throws
-- 'WorkStillInProgress', but still halts the heartbeats.  When this
-- happens, the heartbeat checker will re-enqueue the items.  The
-- occurence of this error indicates misuse of sendHeartbeats, where
-- it gets cancelled before work is done.
deactivateHeartbeats
    :: MonadConnect m => RedisInfo -> WorkerId -> m ()
deactivateHeartbeats r wid = do
    activeCount <- try $ run r $ llen (LKey (activeKey r wid))
    case activeCount :: Either RedisException Int64 of
        Right 0 -> return ()
        Right _ -> throwIO (WorkStillInProgress wid)
        _ -> return ()
    run_ r $ srem (heartbeatActiveKey r) (unWorkerId wid :| [])
    run_ r $ srem (heartbeatInactiveKey r) (unWorkerId wid :| [])

-- | This is called by a worker when it still functions, despite its
-- work items being re-enqueued after heartbeat failure.  It should
-- only be called when we can ensure that we won't enqueue any items
-- on the list stored at 'activeKey' until the heartbeats are being
-- sent.
recoverFromHeartbeatFailure :: MonadCommand m => RedisInfo -> WorkerId -> m ()
recoverFromHeartbeatFailure r wid = run_ r $ del (activeKey r wid :| [])

-- | Periodically check worker heartbeats.  This uses
-- 'periodicActionWrapped' to share the responsibility of checking the
-- heartbeats amongst multiple client servers.  All invocations of
-- this should use the same time interval.
checkHeartbeats
    :: MonadConnect m => RedisInfo -> Seconds -> m void
checkHeartbeats r (Seconds ivl) =
    periodicWrapped (redisConnection r)
                    (heartbeatTimeKey r)
                    -- Time interval between actions
                    (Seconds ivl)
                    -- Frequency that the mutex is checked
                    (Seconds (max 1 (ivl `div` 2)))
                    -- Mutex ttl while executing the action
                    (Seconds (max 2 (ivl `div` 2)))
    $ logNest "checkHeartbeats" $ do
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
                if isJust functioning
                    then $logWarnS "JobQueue" "Last heartbeat check failed"
                    else $logInfoS "JobQueue" "First heartbeat check"
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
    moved <- run r $ (eval script ks [] :: CommandRequest Int64)
    case moved of
        0 -> $logWarnS "JobQueue" $ tshow wid <>
            " failed its heartbeat, but didn't have items to re-enqueue."
        1 -> $logWarnS "JobQueue" $ tshow wid <>
            " failed its heartbeat.  Re-enqueuing its items."
        _ -> $logErrorS "JobQueue" $ unwords
            [ tshow wid
            , "failed its heartbeat.  Re-enqueing its items."
            , "It had more than one item on its work-queue.  This is"
            , "unexpected, and may indicate a bug."
            ]
    return (moved > 0)
  where
    ks = [activeKey r wid, unLKey (requestsKey r)]
    -- NOTE: In order to handle moving many requests, this script
    -- should probaly work around the limits of lua 'unpack'. This is
    -- fine for now, because we should only have at most one item in
    -- the active work queue.
    --
    -- See this older implementation, which I believe handles this,
    -- but is more complicated as a result:
    --
    -- https://github.com/fpco/libraries/blob/9b078aff00aab0a0ee30d33a3ffd9e3f5c869531/work-queue/src/Distributed/RedisQueue.hs#L349
    script = BS8.unlines
        [ "local xs = redis.pcall('lrange', KEYS[1], 0, -1)"
        -- This indicates that failure was already handled.
        , "if xs['err'] then"
        , "    return 0"
        , "else"
        , "    local len = table.getn(xs)"
        , "    if len > 0 then"
        , "        redis.call('rpush', KEYS[2], unpack(xs))"
        , "    end"
        , "    redis.call('del', KEYS[1])"
        , "    redis.call('set', KEYS[1], 'HeartbeatFailure')"
        , "    return len"
        , "end"
        ]

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
heartbeatTimeKey :: RedisInfo -> MutexKey
heartbeatTimeKey r = MutexKey $ Key $ redisKeyPrefix r <> "heartbeat:time"
