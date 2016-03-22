{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE ViewPatterns #-}

module Distributed.Heartbeat where

import ClassyPrelude
import Control.Concurrent.Lifted (threadDelay)
import Control.Monad.Logger (logDebug)
import Data.List.NonEmpty (NonEmpty((:|)))
import Data.Time.Clock.POSIX
import Distributed.Heartbeat.Internal
import Distributed.Redis
import Distributed.Types
import FP.Redis
import FP.ThreadFileLogger

-- FIXME: add warnings when the check rate is too low compared to send
-- rate.

-- | Periodically check worker heartbeats. See #78 for a description of
-- how this works.
checkHeartbeats
    :: MonadConnect m => HeartbeatConfig -> Redis -> ([WorkerId] -> m ()) -> m void
checkHeartbeats config r handleFailures = logNest "checkHeartbeats" $ forever $ do
    startTime <- liftIO getPOSIXTime
    let Seconds ivl = hcCheckerIvl config
        oldTime = startTime - fromIntegral ivl
    $logDebug $ "Checking if enough time has elapsed since last heartbeat check.  Looking for a time before " ++
        tshow oldTime
    mlastTime <- getRedisTime r (heartbeatLastCheckKey r)
    let setTimeAndWait = do
            setRedisTime r (heartbeatLastCheckKey r) startTime []
            liftIO $ threadDelay (fromIntegral ivl * 1000000)
    -- Do the heartbeat check when enough time has elapsed since the last check,
    -- or when no check has ever happened.
    case mlastTime of
        Just lastTime | lastTime >= oldTime -> do
            let usecs = (ceiling (lastTime - oldTime) `max` 1) * 1000000
            $logDebug $ "Heartbeat check: Not enough time has elapsed since the last check, delaying by: " ++
                        tshow usecs
            liftIO $ threadDelay usecs
        Just lastTime -> do
            -- Blocks other checkers from starting, for the first half of the
            -- heartbeat interval.
            $logDebug "Trying to acquire heartbeat check mutex."
            let mutexExpiry = Seconds (max 1 (ivl `div` 2))
            gotMutex <- run r $ set (heartbeatMutexKey r) "" [NX, EX mutexExpiry]
            when gotMutex $ do
                $logDebug "Acquired heartbeat check mutex."
                let ninf = -1 / 0
                inactive <- run r $ zrangebyscore (heartbeatActiveKey r) ninf (realToFrac lastTime) False
                handleFailures (map WorkerId inactive)
                $logDebug "Setting timestamp for last heartbeat check"
                setTimeAndWait
        Nothing -> setTimeAndWait

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
    :: MonadConnect m => HeartbeatConfig -> Redis -> WorkerId -> MVar () -> m void
sendHeartbeats config r wid heartbeatSentVar = do
    sendHeartbeat
    void $ tryPutMVar heartbeatSentVar ()
    forever $ do
        let Seconds ivl = hcSenderIvl config
        liftIO $ threadDelay ((fromIntegral ivl `max` 1) * 1000 * 1000)
        sendHeartbeat
  where
    sendHeartbeat = do
        now <- liftIO getPOSIXTime
        run_ r $ zadd (heartbeatActiveKey r) ((realToFrac now, unWorkerId wid) :| [])

{- FIXME: remove

-- | This removes the worker from the set which are actively checked via
-- heartbeats.  If there's active work, then it throws
-- 'WorkStillInProgress', but still halts the heartbeats.  When this
-- happens, the heartbeat checker will re-enqueue the items.  The
-- occurence of this error indicates misuse of sendHeartbeats, where
-- it gets cancelled before work is done.
deactivateHeartbeats
    :: MonadConnect m => Redis -> WorkerId -> m ()
deactivateHeartbeats r wid = do
    activeCount <- try $ run r $ llen (LKey (activeKey r wid))
    case activeCount :: Either RedisException Int64 of
        Right 0 -> return ()
        Right _ -> throwIO (WorkStillInProgress wid)
        _ -> return ()
    run_ r $ zrem (heartbeatActiveKey r) (unWorkerId wid :| [])
-}
