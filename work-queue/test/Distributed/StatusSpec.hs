{-# OPTIONS_GHC -fno-warn-orphans #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Distributed.StatusSpec (spec) where

import qualified Control.Concurrent.Mesosync.Lifted.Safe as Async
import           Control.Concurrent.Lifted (threadDelay)
import           Control.Concurrent.MVar
import           Control.Monad
import           Control.Monad.IO.Class
import           Data.ByteString (ByteString)
import           Data.ByteString.Char8 (pack)
import           Data.Maybe
import           Data.Store.TypeHash (mkManyHasTypeHash)
import           Data.Void
import           Distributed.Heartbeat
import           Distributed.JobQueue.Client
import           Distributed.JobQueue.Status
import           Distributed.JobQueue.Worker
import           Distributed.Redis
import           Distributed.Types
import           FP.Redis (Seconds (..), MonadConnect)
import           Test.Hspec (Spec)
import           Test.Hspec.Expectations.Lifted
import           TestUtils

$(mkManyHasTypeHash [[t| ByteString |]])

type Request = ByteString
type Response = ByteString

-- | This worker completes as soon as the 'MVar' is not empty.
waitingWorker :: MonadConnect m => MVar () -> m void
waitingWorker mvar = withRedis (jqcRedisConfig testJobQueueConfig) $ \r -> jobWorkerWithHeartbeats testJobQueueConfig r $ waitingWorkerFunc mvar

waitingWorkerFunc :: MonadIO m => MVar () -> RequestId -> Request -> m (Reenqueue Response)
waitingWorkerFunc mvar _ _ = do
    _ <- liftIO $ takeMVar mvar
    return (DontReenqueue "Done")

-- | This 'JobQueueConfig' will cause heartbeat failures, since
-- 'hcSenderIvl' is larger than 'hcCheckerIvl'.
failingJqc :: JobQueueConfig
failingJqc = testJobQueueConfig
    { jqcHeartbeatConfig = HeartbeatConfig { hcSenderIvl = Seconds 3600
                                           , hcCheckerIvl = Seconds 2 }}

-- | A worker that will fail its heartbeat
failingWorker :: MonadConnect m => m void
failingWorker = withRedis (jqcRedisConfig failingJqc) $ \r -> jobWorkerWithHeartbeats failingJqc r failingWorkerFunc

-- | The worker function of the 'failingWorker'.
--
-- It simply does not terminate.
failingWorkerFunc :: MonadConnect m => RequestId -> Request -> m (Reenqueue Response)
failingWorkerFunc _ _ = forever (threadDelay maxBound) >> return (DontReenqueue "done")

-- | Simple client that submits a request and waits for the result.
startClient :: forall m . MonadConnect m => (RequestId, Request) -> m ()
startClient (rid, req) = withJobClient testJobQueueConfig $ \jc -> do
    submitRequest jc rid req
    void (waitForResponse_ jc rid :: m (Maybe Response))

-- | Some requests.
someRequests :: [(RequestId, ByteString)]
someRequests = [(RequestId . pack . show $ n, pack . show $ n) | n <- [0..10 :: Int]]

-- | Start some clients that each enqueue a request, wait until all
-- requests are in the database, and then perform a test.
--
-- All the tests require at least one active client for stuff like
-- heartbeat checks, as well as to set the redis schema.
withRequests :: (MonadConnect m) => Redis -> [(RequestId, Request)] -> m () -> m ()
withRequests redis reqs cont =
    foldl (\x req -> either id id <$> Async.race x (startClient req))
        (waitForHUnitPass upToAMinute $ do
                reqs' <- getAllRequests redis
                reqs' `shouldMatchList` map fst reqs
                cont)
        reqs

queueRequestsTest :: MonadConnect m => Redis -> m ()
queueRequestsTest redis = withRequests redis someRequests $
    waitForHUnitPass upToAMinute $ do
    -- check that the list of pending requests match the submitted jobs
        jqs <- getJobQueueStatus redis
        jqsPending jqs `shouldMatchList` map fst someRequests

addWorkerTest :: MonadConnect m => Redis -> m ()
addWorkerTest redis = withRequests redis someRequests $ do
    mvar <- liftIO newEmptyMVar
    either absurd id <$> Async.race
        (waitingWorker mvar)
        (do waitForHUnitPass upToAMinute $ do
                jqs' <- getJobQueueStatus redis
                length (jqsWorkers jqs') `shouldBe` 1
                length (jqsPending jqs') `shouldBe` length someRequests - 1
            liftIO $ putMVar mvar ())

heartbeatFailureTest :: MonadConnect m => Redis -> m ()
heartbeatFailureTest redis = withRequests redis someRequests $
    fmap (either absurd id) $ Async.race failingWorker $
        waitForHUnitPass upToAMinute $ do -- wait for a heartbeat failure
            jqs <- getJobQueueStatus redis
            length (jqsHeartbeatFailures jqs) `shouldBe` 1
            length (jqsWorkers jqs) `shouldBe` 0

completeJobTest :: MonadConnect m => Redis -> m ()
completeJobTest redis = withRequests redis someRequests $ do
    mvar <- liftIO newEmptyMVar
    either absurd id <$> Async.race
        (waitingWorker mvar)
        (do liftIO $ putMVar mvar ()  -- the worker should finish the job now.
            waitForHUnitPass upToAMinute $ do
                stats' <- getAllRequestStats redis
                length (filter (isJust . rsComputeFinishTime . snd) stats')
                    `shouldBe` 1)

clearFailuresTest :: MonadConnect m => Redis -> m ()
clearFailuresTest redis = withRequests redis someRequests $
    -- get a heartbeat failure, so that 'clearHeartbeatFailures' actually does something.
    fmap (either absurd id) $ Async.race failingWorker $ do
        waitForHUnitPass upToAMinute $ do -- wait for a heartbeat failure
            jqs <- getJobQueueStatus redis
            length (jqsHeartbeatFailures jqs) `shouldBe` 1
        clearHeartbeatFailures redis
        waitForHUnitPass upToAMinute $ do
            jqs <- getJobQueueStatus redis
            length (jqsHeartbeatFailures jqs) `shouldBe` 0

spec :: Spec
spec = do
    redisIt "reports queued requests" queueRequestsTest
    redisIt "reports added worker and enqueued job" addWorkerTest
    redisIt "reports heartbeat failure" heartbeatFailureTest
    redisIt "reports completed jobs" completeJobTest
    redisIt "clearing the list of heartbeat failures works" clearFailuresTest
