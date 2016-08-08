{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE GADTs #-}
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
    , Heartbeating
    , heartbeatingWorkerId
    , checkHeartbeats
    , performHeartbeatCheck
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
      -- * WorkerId utils
    , uniqueWorkerId
    ) where


import ClassyPrelude
import Control.Concurrent.Lifted (threadDelay)
import Control.Monad.Extra (partitionM)
import Control.Monad.Logger.JSON.Extra (logDebugJ, logInfoJ)
import Data.List.NonEmpty (NonEmpty((:|)))
import qualified Data.List.NonEmpty as NE
import Data.Time.Clock.POSIX
import FP.Redis
import FP.ThreadFileLogger
import Control.Concurrent.Mesosync.Lifted.Safe (race)
import Data.Void (absurd)
import qualified Data.UUID as UUID
import qualified Data.UUID.V4 as UUID

import Distributed.Redis
import Distributed.Types

-- TODO: add warnings when the checkor timeout rate is too low
-- compared to send rate.

uniqueWorkerId :: MonadIO m => m WorkerId
uniqueWorkerId = liftIO $
    WorkerId . UUID.toASCIIBytes <$> UUID.nextRandom

newtype Heartbeating = Heartbeating WorkerId

heartbeatingWorkerId :: Heartbeating -> WorkerId
heartbeatingWorkerId (Heartbeating wid) = wid

-- | Configuration of heartbeats, used by both the checker and sender.
data HeartbeatConfig = HeartbeatConfig
    { hcSenderIvl :: !Seconds
    -- ^ How frequently heartbeats should be sent.
    , hcCheckerIvl :: !Seconds
    -- ^ How frequently heartbeats should be checked. Should be
    -- substantially larger than 'hcSenderIvl'.
    } deriving (Eq, Show)

-- | How long a heartbeat lasts in the database.
hcTimeoutIvl :: HeartbeatConfig -> Seconds
hcTimeoutIvl = hcCheckerIvl

-- | Default 'HeartbeatConfig'.
--
-- Heartbeats are sent every 15 seconds, last 30 seconds and are
-- checked every 30 seconds.
defaultHeartbeatConfig :: HeartbeatConfig
defaultHeartbeatConfig = HeartbeatConfig
    { hcSenderIvl = Seconds 15
    , hcCheckerIvl = Seconds 30
    }

-- | Redis key used to collect the 'WorkerId's of active workers.
heartbeatActiveKey :: Redis -> SKey
heartbeatActiveKey r = SKey $ Key $ redisKeyPrefix r <> "heartbeat:active"

-- | Redis key to hold the current heartbeat of a given 'WorkerId'.
-- Heartbeats expire automatically, and are refreshed byt he worker
-- periodically.  That way, each worker for which the
-- 'heartbeatWorkerKey' is set passes the heartbeat check, without the
-- need to compare the time of the last heartbeat against a clock.
heartbeatOfWorkerKey :: Redis -> WorkerId -> VKey
heartbeatOfWorkerKey r wid = VKey $ Key $ redisKeyPrefix r <> "heartbeat:heartbeat:" <> unWorkerId wid

-- | A sorted set of 'WorkerId's that have failed a heartbeat check in
-- the past.  Score is the time of the unsuccessful heartbeat check.
--
-- Note that workers that have failed a heartbeat check temporarily
-- are included here.
heartbeatDeadKey :: Redis -> ZKey
heartbeatDeadKey r = ZKey $ Key $ redisKeyPrefix r <> "heartbeat:dead"

-- | Redis key to store the timestamp for the last time the heartbeat check successfully ran.
heartbeatLastCheckKey :: Redis -> VKey
heartbeatLastCheckKey r = VKey $ Key $ redisKeyPrefix r <> "heartbeat:last-check"

-- | Get a list of all workers currently connected to the system (that
-- did not fail their last heartbeat).
--
-- This includes workers that are currently working, and those waiting
-- for work.
activeOrUnhandledWorkers :: (MonadConnect m) => Redis -> m [WorkerId]
activeOrUnhandledWorkers r =
    map WorkerId <$> run r (smembers (heartbeatActiveKey r))

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
-- if the last heartbeat has already expired.
lastHeartbeatForWorker :: (MonadConnect m) => Redis -> WorkerId -> m (Maybe UTCTime)
lastHeartbeatForWorker r wid =
    fmap posixSecondsToUTCTime <$> getRedisTime r (heartbeatOfWorkerKey r wid)

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
-- If enough time has passed since the last heartbeat check, check the
-- heartbeats of all workers that are currently believed to be live.
--
-- Workers periodically send heartbeats.  Each heartbeat expires
-- automatically, so workers that stop sending their heartbeats will
-- fail their heartbeat test after a short time.
--
-- After the heartbeat check, @handleFailures@ will be run on the list
-- of workers that failed their heartbeat.
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
    -- We use the current time here in order to reduce the total
    -- number of heartbeat checks in case many clients perform them:
    -- after every successfult heartbeat check, the time of the check
    -- is entered into the database.  Before performing a heartbeat
    -- check, we ensure that enough time has passed.
    startTime <- liftIO getPOSIXTime
    let Seconds ivl = hcCheckerIvl config
        oldTime = startTime - fromIntegral ivl
    $logDebugJ $ "Checking if enough time has elapsed since last heartbeat check.  Looking for a time before " ++
        tshow oldTime
    mlastTime <- getRedisTime r (heartbeatLastCheckKey r)
    let setTimeAndWait = do
            void $ setRedisTime r (heartbeatLastCheckKey r) startTime []
            liftIO $ threadDelay (fromIntegral ivl * 1000000)
    -- Do the heartbeat check when enough time has elapsed since the last check,
    -- or when no check has ever happened.
    case mlastTime of
        Just lastTime | lastTime >= oldTime -> do
            let usecs = (ceiling (lastTime - oldTime) `max` 1) * 1000000
            $logDebugJ $ "Heartbeat check: Not enough time has elapsed since the last check, delaying by: " ++
                        tshow usecs
            liftIO $ threadDelay usecs
        Just _lastTime -> do
            -- Note that nothing prevents two checkers from checking up on
            -- the workers at the same time. This is fine and part of the contract of the
            -- function.
            $logDebugJ ("Enough time has passed, checking if there are any inactive workers."::Text)
            performHeartbeatCheck r startTime handleFailures
            $logDebugJ ("Setting timestamp for last heartbeat check"::Text)
            setTimeAndWait
        Nothing -> setTimeAndWait

-- | Perform a single heartbeat check
performHeartbeatCheck
    :: MonadConnect m
    => Redis
    -> POSIXTime
    -> (NonEmpty WorkerId -> m () -> m ()) -> m ()
performHeartbeatCheck r startTime handleFailures = do
    workers <- activeOrUnhandledWorkers r
    (_passed, failed) <- partitionM (\wid -> isJust <$> lastHeartbeatForWorker r wid) workers
    case NE.nonEmpty failed of
        Nothing -> return ()
        Just inactives -> do
            $logInfoJ ("Got inactive workers: " ++ tshow inactives)
            deadones <- run r (zadd (heartbeatDeadKey r) (NE.zip (NE.repeat (realToFrac startTime)) (unWorkerId <$> inactives)))
            unless (deadones == fromIntegral (length inactives)) $
                $logDebugJ ("Expecting to have registered " ++ tshow (length inactives) ++ " dead workers, but got " ++ tshow deadones ++ ", some other client probably took care of it already.")
            -- TODO consider catching exceptins here
            handleFailures
                inactives
                (do $logDebugJ ("Cleaning up inactive workers " ++ tshow inactives)
                    removed <- run r $ srem (heartbeatActiveKey r) (unWorkerId <$> inactives)
                    unless (removed == fromIntegral (length inactives)) $
                        $logDebugJ ("Expecting to have cleaned up " ++ tshow (length inactives) ++ " but got " ++ tshow removed ++ ", some other client probably took care of it already.")
                )

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

-- | Periodically sends heartbeats, and makes sure that the worker is
-- in the set of active workers.
withHeartbeats
    :: MonadConnect m => HeartbeatConfig -> Redis -> (Heartbeating -> m a) -> m a
withHeartbeats config r cont = do
    $logInfoJ ("Starting heartbeats" :: Text)
    wid <- uniqueWorkerId
    sendHeartbeat wid
    $logInfoJ ("Initial heartbeat sent" :: Text)
    fmap (either id absurd) $ race (cont (Heartbeating wid)) $ forever $ do
        let Seconds ivl = hcSenderIvl config
        liftIO $ threadDelay ((fromIntegral ivl `max` 1) * 1000 * 1000)
        sendHeartbeat wid
  where
    sendHeartbeat wid = do
        -- We need the current time here only for displaying the time
        -- of the last heartbeat in the hpc-manager.  For the
        -- heartbeat mechanism itself, a fixed string would work just
        -- as well.
        curTime <- liftIO getPOSIXTime
        -- Update the heartbeat
        void $ setRedisTime r (heartbeatOfWorkerKey r wid) curTime [EX (hcTimeoutIvl config)]
        -- Make sure the worker appears in the set of active workers.
        -- Since it's a set, adding an element is idempotent, so it's
        -- safe to add every time we send a heartbeat.
        run_ r $ sadd (heartbeatActiveKey r) (unWorkerId wid :| [])
