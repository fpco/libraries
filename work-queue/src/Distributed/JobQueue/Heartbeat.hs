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

import           ClassyPrelude
import           Control.Concurrent.Lifted (threadDelay)
import           Control.Monad.Logger (MonadLogger, logWarnS, logErrorS, logDebug)
import qualified Data.ByteString.Char8 as BS8
import           Data.List.NonEmpty (NonEmpty((:|)))
import           Data.Time.Clock.POSIX
import           Distributed.JobQueue.Shared
import           Distributed.RedisQueue
import           Distributed.RedisQueue.Internal
import           FP.Redis
import           FP.ThreadFileLogger

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
    sendHeartbeat
    run_ r $ sadd (heartbeatActiveKey r) (unWorkerId wid :| [])
    forever $ do
        liftIO $ threadDelay ((fromIntegral ivl `max` 1) * 1000 * 1000)
        sendHeartbeat
  where
    sendHeartbeat = setTime r (heartbeatKey r wid) =<< liftIO getPOSIXTime

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
checkHeartbeats r (Seconds ivl) = logNest "checkHeartbeats" $ forever $ do
    $logDebug "Checking if enough time has elapsed since last heartbeat check."
    startTime <- liftIO getPOSIXTime
    let oldTime = startTime - fromIntegral ivl
    mlastTime <- getTime r (heartbeatLastCheckKey r)
    -- Do the heartbeat check when enough time has elapsed since the last check,
    -- or when no check has ever happened.
    when (maybe True (< oldTime) mlastTime) $ do
        -- Blocks other checkers from starting, for the first half of the
        -- heartbeat interval.
        $logDebug "Trying to acquire heartbeat check mutex."
        let expiry = Seconds (max 1 (ivl `div` 2))
        gotMutex <- run r $ set (heartbeatMutexKey r) "" [NX, EX expiry]
        when gotMutex $ do
            $logDebug "Acquired heartbeat check mutex."
            workers <- run r $ smembers (heartbeatActiveKey r)
            reenqueuedSome <- fmap (any id) $ forM workers $ \widbs -> do
                let wid = WorkerId widbs
                mheartbeatTime <- getTime r (heartbeatKey r wid)
                -- If the heartbeat hasn't been updated within the check
                -- interval, re-enqueue its requests.
                if maybe True (< oldTime) mheartbeatTime
                    then handleWorkerFailure r wid
                    else return False
            when reenqueuedSome $ do
                $logDebug "Notifying that some requests were re-enqueued"
                notifyRequestAvailable r
            $logDebug "Setting timestamp for last heartbeat check"
            setTime r (heartbeatLastCheckKey r) startTime

handleWorkerFailure
    :: (MonadCommand m, MonadLogger m) => RedisInfo -> WorkerId -> m Bool
handleWorkerFailure r wid = do
    moved <- run r $ (eval script ks as :: CommandRequest Int64)
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
    as = [unWorkerId wid]
    ks = [ activeKey r wid
         , unLKey (requestsKey r)
         , unSKey (heartbeatActiveKey r)
         ]
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
        , "    redis.pcall('srem', KEYS[3], ARGV[1])"
        , "    return len"
        , "end"
        ]

-- * Utilities for reading and writing timestamps

-- Rounds time to the nearest millisecond.

setTime :: (MonadCommand m) => RedisInfo -> VKey -> POSIXTime -> m ()
setTime r k t = run_ r $ set k (BS8.pack (show (floor (t * 1000) :: Int))) []

getTime :: (MonadCommand m) => RedisInfo -> VKey -> m (Maybe POSIXTime)
getTime r k = do
    mbs <- run r $ get k
    case mbs of
        Nothing -> return Nothing
        Just bs ->
            case readMay (BS8.unpack bs) of
                Nothing -> fail $ "Failed to decode timestamp in key " ++ show k
                Just result -> return $ Just (fromInteger result / 1000)

-- * Functions to compute Redis keys

-- | Stores the timestamp of the last heartbeat from the given worker.
heartbeatKey :: RedisInfo -> WorkerId -> VKey
heartbeatKey r wid = VKey $ Key $ redisKeyPrefix r <> "heartbeat:worker:" <> unWorkerId wid

-- | A set of 'WorkerId's that are currently thought to be running.
heartbeatActiveKey :: RedisInfo -> SKey
heartbeatActiveKey r = SKey $ Key $ redisKeyPrefix r <> "heartbeat:active"

-- | Timestamp for the last time the heartbeat check successfully ran.
heartbeatLastCheckKey :: RedisInfo -> VKey
heartbeatLastCheckKey r = VKey $ Key $ redisKeyPrefix r <> "heartbeat:last-check"

-- | Key used as a simple mutex to signal that some server is currently doing
-- the heartbeat check. Note that this is just to avoid unnecessary work, and
-- correctness doesn't rely on it.
heartbeatMutexKey :: RedisInfo -> VKey
heartbeatMutexKey r = VKey $ Key $ redisKeyPrefix r <> "heartbeat:mutex"
