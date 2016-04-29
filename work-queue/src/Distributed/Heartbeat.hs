{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE ViewPatterns #-}
module Distributed.Heartbeat
    ( HeartbeatConfig(..)
    , checkHeartbeats
    , withCheckHeartbeats
    , withHeartbeats

    -- * Testing/debugging
    , activeOrUnhandledWorkers
    ) where

import ClassyPrelude
import Control.Concurrent.Lifted (threadDelay)
import Control.Monad.Logger (logDebug, logInfo)
import Data.List.NonEmpty (NonEmpty((:|)))
import Data.Time.Clock.POSIX
import FP.Redis
import FP.ThreadFileLogger
import Control.Concurrent.Async.Lifted (race)
import Data.Void (absurd)

import Distributed.Redis
import Distributed.Types

-- TODO: add warnings when the check rate is too low compared to send
-- rate.

-- | Configuration of heartbeats, used by both the checker and sender.
data HeartbeatConfig = HeartbeatConfig
    { hcSenderIvl :: !Seconds
    -- ^ How frequently heartbeats should be sent.
    , hcCheckerIvl :: !Seconds
    -- ^ How frequently heartbeats should be checked. Should be
    -- substantially larger than 'hcSenderIvl'.
    }

-- | A sorted set of 'WorkerId's that are currently thought to be running. The
-- score of each is its timestamp.
heartbeatActiveKey :: Redis -> ZKey
heartbeatActiveKey r = ZKey $ Key $ redisKeyPrefix r <> "heartbeat:active"

-- | Timestamp for the last time the heartbeat check successfully ran.
heartbeatLastCheckKey :: Redis -> VKey
heartbeatLastCheckKey r = VKey $ Key $ redisKeyPrefix r <> "heartbeat:last-check"

-- | Returns all the heartbeats currently in redis. Should only be useful
-- for testing/debugging
activeOrUnhandledWorkers :: (MonadConnect m) => Redis -> m [WorkerId]
activeOrUnhandledWorkers r =
    map WorkerId <$> run r (zrange (heartbeatActiveKey r) 0 (-1) False)

-- | Periodically check worker heartbeats. See #78 for a description of
-- how this works.
--
-- Note that:
--
-- * @handleFailures@ could be called multiple times for a worker that has died only once,
-- even if the "cleanup" function is correctly called.
-- * If multiple 'checkHeartbeats' are waiting, each @handleFailures@ can be called with
-- the same workers.
checkHeartbeats
    :: (MonadConnect m)
    => HeartbeatConfig -> Redis
    -> ([WorkerId] -> m () -> m ())
    -- ^ The second function is a "cleanup" function. It should be called by the
    -- continuation when the worker failures have been handled. If it's not called,
    -- the 'WorkerId's will show up again.
    -> m void
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
            $logDebug ("Enough time has passed, checking if there are any inactive workers.")
            let ninf = -1 / 0
            inactives0 <- run r $ zrangebyscore (heartbeatActiveKey r) ninf (realToFrac lastTime) False
            case inactives0 of
                [] -> return ()
                inactive : inactives -> do
                    $logInfo ("Got inactive workers: " ++ tshow (inactive : inactives))
                    handleFailures
                        (map WorkerId (inactive : inactives))
                        (void (run r (zrem (heartbeatActiveKey r) (inactive :| inactives))))
            $logDebug "Setting timestamp for last heartbeat check"
            setTimeAndWait
        Nothing -> setTimeAndWait

withCheckHeartbeats
    :: (MonadConnect m)
    => HeartbeatConfig -> Redis
    -> ([WorkerId] -> m () -> m ())
    -> m a
    -> m a
withCheckHeartbeats conf redis handle cont =
    fmap (either id absurd) (race cont (checkHeartbeats conf redis handle))

-- | This periodically removes the worker's key from the set of
-- inactive workers.  This set is periodically re-initialized and
-- checked by 'checkHeartbeats'.
--
-- Note that using the same 'Seconds' time interval for both
-- 'checkHeartbeats' and 'sendHeartbeats' will be unreliable.
-- Instead, a lesser time interval should be passed to
-- 'sendHeartbeats', to be sure that the heartbeat is seen by every
-- iteration of 'checkHeartbeats'.
withHeartbeats
    :: MonadConnect m => HeartbeatConfig -> Redis -> WorkerId -> m a -> m a
withHeartbeats config r wid cont = do
    sendHeartbeat
    fmap (either id absurd) $ race cont $ forever $ do
        let Seconds ivl = hcSenderIvl config
        liftIO $ threadDelay ((fromIntegral ivl `max` 1) * 1000 * 1000)
        sendHeartbeat
  where
    sendHeartbeat = do
        now <- liftIO getPOSIXTime
        run_ r $ zadd (heartbeatActiveKey r) ((realToFrac now, unWorkerId wid) :| [])
