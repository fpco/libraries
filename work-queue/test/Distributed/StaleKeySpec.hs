{-# OPTIONS_GHC -fno-warn-orphans #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE DataKinds #-}
module Distributed.StaleKeySpec (spec) where

import ClassyPrelude hiding (keys)
import Control.Concurrent.Mesosync.Lifted.Safe
import Control.Concurrent.Lifted (threadDelay)
import Data.Store.TypeHash (mkHasTypeHash)
import Data.Void
import Distributed.Heartbeat
import Distributed.JobQueue.Client
import Distributed.JobQueue.Status
import Distributed.JobQueue.Worker
import Distributed.Redis
import Distributed.Types
import FP.Redis (Seconds(..), MonadConnect)
import Test.Hspec (Spec)
import Test.Hspec.Expectations
import TestUtils

$(mkHasTypeHash =<< [t| ByteString |])

type Request = ByteString
type Response = ByteString

-- Configs/init
-----------------------------------------------------------------------

heartbeatConfig :: HeartbeatConfig
heartbeatConfig = HeartbeatConfig
    { hcSenderIvl = Seconds 3600 -- We don't want the second heartbeat to arrive, ever.
    , hcCheckerIvl = Seconds 1
    }

jqc :: JobQueueConfig
jqc = testJobQueueConfig { jqcHeartbeatConfig = heartbeatConfig
                         , jqcCheckStaleKeysInterval = Seconds 2
                         }

workerFunc :: MonadConnect m => RequestId -> Request -> m (Reenqueue Response)
workerFunc _ _ = do
    -- the worker should never finish in this test, for two reasons:
    --
    -- - We want to have enough time for the job to be rescheduled by
    --   'checkStaleKeys', and infinite is enough
    --
    -- - We need the client to run long enough (i.e., indefinitely),
    --   since the running client is what repeatedly calls
    --   'checkStaleKeys'.  If the worker doesn't finish, neither does
    --   the client, since it waits for the worker's result.
    _ <- forever $ threadDelay maxBound
    return $ DontReenqueue "done"

myWorker :: MonadConnect m => m void
myWorker = withRedis (jqcRedisConfig jqc) $ \r -> jobWorkerWithHeartbeats jqc r workerFunc

requestId :: RequestId
requestId = RequestId "myRequest"

myClient :: MonadConnect m => m void
myClient = withJobClient jqc $ \jq -> do
    let request = "some request" :: Request
    submitRequest jq requestId request
    mResponse :: Maybe Response <- waitForResponse_ jq requestId
    case mResponse of
        Nothing -> fail "myClient got 'Nothing' when waiting for the response!"
        Just _ -> fail "myClient got a response, which should never happen since the worker function never returns"

waitForHeartbeatFailure :: MonadConnect m => Redis -> m ()
waitForHeartbeatFailure redis = waitForHUnitPass upToAMinute $ do
    liveWorkers <- activeOrUnhandledWorkers redis
    liftIO $ liveWorkers `shouldBe` [] -- no active workers
    failedWorkers <- deadWorkers redis
    liftIO $ length failedWorkers `shouldBe` 1 -- 1 heartbeat failure

waitForJobStarted :: MonadConnect m => Redis -> m ()
waitForJobStarted redis = waitForHUnitPass upToAMinute $ do
    requestEvents <- getRequestEvents redis requestId
    liftIO $ requestEvents `shouldSatisfy`
        (isJust . find (\(_, rEvent) -> case rEvent of
                               RequestWorkStarted _ -> True
                               _ -> False))

waitForJobReenqueued :: MonadConnect m => Redis -> m ()
waitForJobReenqueued redis = waitForHUnitPass upToAMinute $ do
    requestEvents <- getRequestEvents redis requestId
    liftIO $ requestEvents `shouldSatisfy`
        (isJust . find (\(_, rEvent) -> case rEvent of
                               RequestWorkReenqueuedAsStale _ -> True
                               _ -> False))


-- | Test that `checkStaleKeys`, as run by a jobClientThread, actually works.
--
-- The timing is a bit tricky:
--
-- - We start a worker, and an explicit checkHeartbeats.
--
-- - The heartbeatConfig is such that the worker only sends one
--   heartbeat, so it will fail its heartbeat check very soon, and
--   `checkHeartbeats will remove it from the list of active workers. So
--   it is now in a state where it is idle, looking for jobs, but
--   considered to be dead by the heartbeat checker.
--
-- - Now, we start a client, submitting a job and starting checkStaleKeys.
--
-- - The worker will take the job, but it will still be considered dead
--   by the heartbeat checker. Without checkStaleKeys, the job would
--   stay in its activeKey. That would be the dangerous situation that
--   checkStaleKeys fixes: if the worker dies now, the heartbeat checker
--   would not re-schedule the job, since it thinks that the worker died
--   a long time ago (see issue #133).
--
-- - Finally, checkStaleKeys detects that a worker that is considered
--   dead is at the same time responsible for completing a job, and
--   re-schedules the job.
staleKeyTest :: MonadConnect m => Redis -> m ()
staleKeyTest redis =
    -- We'll need an explicit 'heartbeatChecker', since the client
    -- should pick a job _after_ failing its heartbeat.  If its
    -- heartbeat was checked by the client, it would already have
    -- taken the job.
    runConcurrently $
        Concurrently myWorker <|>
        Concurrently (checkHeartbeats (jqcHeartbeatConfig jqc) redis $ \_inactive cleanup -> cleanup) <|>
        Concurrently
            (do waitForHeartbeatFailure redis -- worker fails heartbeat, but is actually still alive.
                fmap (either absurd id) $ race myClient $ do
                    -- we expect the worker to take the job
                    waitForJobStarted redis
                    -- and the client to subsequently re-enqueue it, since
                    -- the worker is not in the list of active workers.
                    waitForJobReenqueued redis)

spec :: Spec
spec = redisIt "Re-enqueues jobs from dead workers that had already failed their heartbeat before working on the job." staleKeyTest
