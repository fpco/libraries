{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Internal JobQueue module, exposed for testing purposes.
module Distributed.JobQueue.Periodic
    ( periodicEx
    , periodicWrapped
    ) where

import ClassyPrelude
import Control.Concurrent.Lifted (threadDelay)
import Control.Monad.Logger (logErrorNS)
import FP.Redis
import FP.Redis.Mutex

-- | A different approach to periodic functions than the functions in
-- FP.Redis.Mutex.  In particular, this uses the mutex ttl as a
-- mechanism to signal when the action needs to happen again.  This
-- results in:
--
--   * No reliance on synchronized clocks.
--
--   * Less traffic with redis, because we only refresh the mutex
--   frequently while executing the inner action.  Once the inner
--   action is known to have succeeded, we can refresh the mutex once
--   with a ttl as long as the wait interval.
--
--   * Also less traffic because we don't store timestamps in redis.
--
-- Another difference is that the interval is __between__ the end time
-- of the last action and the start time of the next.  FP.Redis.Mutex
-- has the interval be from the start time of the first to the start
-- time of the next, which isn't desired for heartbeats.
periodicEx
    :: MonadConnect m
    => Connection
    -> MutexKey
    -> Seconds
    -- ^ Time interval - minimum amount of time there should be
    -- between executions of the action.
    -> Seconds
    -- ^ Frequency that the mutex is checked.  You can expect that
    -- some runner of 'periodicEx' will attempt to check the mutex
    -- within @ivl + freq@ of the end of the last execution.
    -> Seconds
    -- ^ Mutex ttl while the action is executing.  This is a tradeoff:
    --
    --   * Smaller values make it so that that when a server or
    --   connection fails, other servers can run the action sooner.
    --
    --   * Larger values make it less likely that connection issues
    --   will cause the mutex to erroneously expire, which could lead
    --   to concurrent execution of the inner action.
    --
    -- This should be larger than 1, as that's the rate that the mutex
    -- is refreshed.
    -> m ()
    -> m void
periodicEx conn key ivl (Seconds freq) mutexTtl inner = forever $ do
    mtoken <- tryAcquireMutex conn mutexTtl key
    case mtoken of
        Nothing -> threadDelay (fromIntegral freq * 1000000)
        Just token -> flip onException (releaseMutex conn key token) $ do
            holdMutexDuring conn key token (Seconds 1) mutexTtl inner
            refreshMutex conn ivl key token

-- | This is the same as 'periodicEx' except it wraps the inner action
-- in an exception handler which throw.
--
-- Note: This isn't quite as wrapped as 'periodicActionWrapped', which
-- also has an outer exception wrapper.
periodicWrapped
    :: MonadConnect m
    => Connection
    -> MutexKey
    -> Seconds
    -> Seconds
    -> Seconds
    -> m ()
    -> m void
periodicWrapped conn key ivl freq mutexTtl =
    periodicEx conn key ivl freq mutexTtl . flip catchAny innerHandler
  where
    innerHandler ex =
        logErrorNS (connectLogSource (connectionInfo conn))
                   ("periodicWrapped innerHandler: " ++ tshow ex)
