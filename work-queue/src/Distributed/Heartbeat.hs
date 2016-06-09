{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE ViewPatterns #-}
{-|
Module: Distributed.Heartbeat
Description: Keep track of failed workers by via heartbeats.

Workers are supposed to use 'withHeartbeats' to periodically send a
heartbeat to redis, wile performing some other action.

The function 'withCheckHeartbeats' will periodically check the
heartbeats of all workers that have sent at least one successful
heartbeat.

-}

module Distributed.Heartbeat
    ( -- * Configuration
      HeartbeatConfig(..)
    , defaultHeartbeatConfig
      -- * Sending and checking heartbeats
    , checkHeartbeats
    , withCheckHeartbeats
    , withHeartbeats
      -- * Query heartbeat status
    , deadWorkers
    , activeOrUnhandledWorkers
    , lastHeartbeatCheck
    , lastHeartbeatForWorker
    , lastHeartbeatFailureForWorker
      -- * Clear list of heartbeat failures
    , clearHeartbeatFailures
    ) where

import ClassyPrelude
import Control.Concurrent.Lifted (threadDelay)
import Control.Monad.Logger (logDebug, logInfo)
import Data.List.NonEmpty (NonEmpty((:|)))
import qualified Data.List.NonEmpty as NE
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
    } deriving (Eq, Show)

-- | Default 'HeartbeatConfig'.
--
-- Heartbeats are sent every 15 seconds, checked every 30 seconds.
defaultHeartbeatConfig :: HeartbeatConfig
defaultHeartbeatConfig = HeartbeatConfig
    { hcSenderIvl = Seconds 15
    , hcCheckerIvl = Seconds 30
    }

-- | Redis key used to collect the most recent heartbeat of nodes, in
-- a sorted set with the timestamp of the last heartbeat as key.
heartbeatActiveKey :: Redis -> ZKey
heartbeatActiveKey r = ZKey $ Key $ redisKeyPrefix r <> "heartbeat:active"

-- | A sorted set of 'WorkerId's that are considered dead, i.e., they
-- failed their last heartbeat check.  Score is the time of the
-- unsuccessful heartbeat check.
heartbeatDeadKey :: Redis -> ZKey
heartbeatDeadKey r = ZKey $ Key $ redisKeyPrefix r <> "heartbeat:dead"

-- | Redis ey to store the timestamp for the last time the heartbeat check successfully ran.
heartbeatLastCheckKey :: Redis -> VKey
heartbeatLastCheckKey r = VKey $ Key $ redisKeyPrefix r <> "heartbeat:last-check"

-- | Get a list of all workers currently connected to the system (that
-- did not fail their last heartbeat).
--
-- This includes workers that are currently working, and those waiting
-- for work.
activeOrUnhandledWorkers :: (MonadConnect m) => Redis -> m [WorkerId]
activeOrUnhandledWorkers r =
    map WorkerId <$> run r (zrange (heartbeatActiveKey r) 0 (-1) False)

-- | Returns all the workers that failed a heartbeat check.
--
-- This will also list workers that continued sending heartbeats after
-- failing one or more checks.
--
-- The list of heartbeat failures can be reset via 'clearHeartbeatFailures'.
deadWorkers :: MonadConnect m => Redis -> m [WorkerId]
deadWorkers r =
    map WorkerId <$> run r (zrange (heartbeatDeadKey r) 0 (-1) False)

-- | Timestamp for the last time the heartbeat check successfully ran.
lastHeartbeatCheck :: (MonadConnect m) => Redis -> m (Maybe UTCTime)
lastHeartbeatCheck r =
    fmap posixSecondsToUTCTime <$> getRedisTime r (heartbeatLastCheckKey r)

-- | Get the most recent time a given worker sent its hearbeat.
--
-- Will return nothing if the worker has not sent a heartbeat yet, or
-- if it failed the last heartbeat check.
lastHeartbeatForWorker :: (MonadConnect m) => Redis -> WorkerId -> m (Maybe UTCTime)
lastHeartbeatForWorker r wid =
    fmap (posixSecondsToUTCTime . realToFrac) <$>
    run r (zscore (heartbeatActiveKey r) (unWorkerId wid))

-- | Get the time of the most recent heartbeat failure for a specific worker.
lastHeartbeatFailureForWorker :: MonadConnect m => Redis -> WorkerId -> m (Maybe UTCTime)
lastHeartbeatFailureForWorker r wid =
    fmap (posixSecondsToUTCTime . realToFrac) <$>
    run r (zscore (heartbeatDeadKey r) (unWorkerId wid))

-- | Clear all heartbeat failures from the database.
clearHeartbeatFailures :: MonadConnect m => Redis -> m ()
clearHeartbeatFailures r = do
    items <- run r $ zrange (heartbeatDeadKey r) 0 (-1) False
    case items of
        [] -> return ()
        x:xs -> void $ run r $ zrem (heartbeatDeadKey r) (x :| xs)

-- | Periodically check worker heartbeats.
--
-- If enough time has passed since the last heartbeat check, look at
-- the latest heartbeat from every worker.  If it is too old, we have
-- a heartbeat failure of that worker.
--
-- After the heartbeat check, @handleFailures@ will be run on the list
-- of workers that failed their heartbeat.
--
-- See #78 for a detailed description of the design.
--
-- Note that:
--
-- * @handleFailures@ could be called multiple times for a worker that has died only once,
-- even if the "cleanup" function is correctly called.
-- * If multiple 'checkHeartbeats' are waiting, each @handleFailures@ can be called with
-- the same workers.
--
-- In other words, the continuation should be idempotent.
checkHeartbeats
    :: (MonadConnect m)
    => HeartbeatConfig -> Redis
    -> (NonEmpty WorkerId -> m () -> m ())
    -- ^ @handleFailures@: what to do in case some workers failed their heartbeats.
    -- The second argument is a "cleanup" action. It should be called by the
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
            -- Note that nothing prevents two checkers from checking up on
            -- the workers at the same time. This is fine and part of the contract of the
            -- function.
            $logDebug ("Enough time has passed, checking if there are any inactive workers.")
            let ninf = -1 / 0
            inactives00 <- run r $ zrangebyscore (heartbeatActiveKey r) ninf (realToFrac lastTime) False
            case inactives00 of
                [] -> return ()
                inactive : inactives0 -> do
                    let inactives = inactive :| inactives0
                    $logInfo ("Got inactive workers: " ++ tshow inactives)
                    deadones <- run r (zadd (heartbeatDeadKey r) (NE.zip (NE.repeat (realToFrac startTime)) inactives))
                    unless (deadones == fromIntegral (length inactives)) $
                        $logDebug ("Expecting to have registered " ++ tshow (length inactives) ++ " dead workers, but got " ++ tshow deadones ++ ", some other client probably took care of it already.")
                    -- TODO consider catching exceptins here
                    handleFailures
                        (map WorkerId inactives)
                        (do $logDebug ("Cleaning up inactive workers " ++ tshow inactives)
                            removed <- run r (zrem (heartbeatActiveKey r) inactives)
                            unless (removed == fromIntegral (length inactives)) $
                                $logDebug ("Expecting to have cleaned up " ++ tshow (length inactives) ++ " but got " ++ tshow removed ++ ", some other client probably took care of it already."))
            $logDebug "Setting timestamp for last heartbeat check"
            setTimeAndWait
        Nothing -> setTimeAndWait

-- | Periodically check for heartbeats while concurrently performing
-- another action.
--
-- See 'checkHeartbeats' for an explanation of how a single heartbeat
-- check works.
--
-- An invocation might look like this:
--
-- @
-- withCheckHeartbeats defaultHeartbeatConfig redis
--     (\wids markHandled -> do
--         doSomethingWithFailedWorkers wids
--         markHandled)
--     myAction
-- @
--
-- where @doSomethingWithFailedWorkers@ is some action you want to
-- perform for every failed worker, and @myAction@ is the action you
-- want to perform while checking heartbeats in the background.  Note
-- that unless you explicitly call @markHandled@ in the continuation,
-- workers that failed their heartbeat will not be removed from the
-- list of active workers and will still show up in the output of
-- 'activeOrUnhandledWorkers'.
withCheckHeartbeats
    :: (MonadConnect m)
    => HeartbeatConfig -> Redis
    -> (NonEmpty WorkerId -> m () -> m ())
    -- ^ Cleanup function, see 'checkHeartbeats'.
    -> m a
    -- ^ Action to perform while concurrently checking for heartbeats.
    -> m a
withCheckHeartbeats conf redis handle' cont =
    fmap (either id absurd) (race cont (checkHeartbeats conf redis handle'))

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
