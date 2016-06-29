{-# OPTIONS_GHC -fno-warn-orphans #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Distributed.StatusSpec (spec) where

import qualified Control.Concurrent.Async.Lifted.Safe as Async
import           Control.Concurrent.MVar
import           Control.Monad
import           Control.Monad.IO.Class
import           Control.Monad.Logger
import           Data.ByteString (ByteString)
import           Data.ByteString.Char8 (pack)
import           Data.Maybe
import           Data.Store.TypeHash (mkManyHasTypeHash)
import           Distributed.Heartbeat
import           Distributed.JobQueue.Client
import           Distributed.JobQueue.Status
import           Distributed.JobQueue.Worker
import           Distributed.Redis
import           Distributed.Types
import           FP.Redis (MonadConnect)
import           Test.Hspec (Spec)
import           Test.Hspec.Expectations.Lifted
import           TestUtils

$(mkManyHasTypeHash [[t| ByteString |]])

type Request = ByteString
type Response = ByteString

startClient :: forall m . (MonadConnect m, MonadLogger m) => (RequestId, Request) -> m (Async.Async ())
startClient (rid, req) = Async.async $ withJobClient testJobQueueConfig $ \jc -> do
    submitRequest jc rid req
    void (waitForResponse_ jc rid :: m (Maybe Response))

-- | This worker completes as soon as the 'MVar' is not empty.
waitingWorker :: MonadConnect m => MVar () -> m void
waitingWorker mvar = jobWorkerWithHeartbeats testJobQueueConfig $ waitingWorkerFunc mvar

waitingWorkerFunc :: MonadIO m => MVar () -> Redis -> RequestId -> Request -> m (Reenqueue Response)
waitingWorkerFunc mvar _ _ _ = do
    _ <- liftIO $ takeMVar mvar
    return (DontReenqueue "Done")

someRequests :: [(RequestId, ByteString)]
someRequests = [(RequestId . pack . show $ n, pack . show $ n) | n <- [0..10 :: Int]]

queueReqs :: (MonadLogger m, MonadConnect m) => Redis -> [(RequestId, Request)] -> m ()
queueReqs redis reqs = do
    mapM_ startClient reqs
    waitFor upToTenSeconds $ do
        reqs' <- getAllRequests redis
        reqs' `shouldMatchList` map fst reqs

queueRequestsTest :: MonadConnect m => Redis -> m ()
queueRequestsTest redis = do
    queueReqs redis someRequests
    waitFor upToTenSeconds $ do
    -- check that both getAllRequests and the list of pending requests match the submitted jobs
        jqs <- getJobQueueStatus redis
        jqsPending jqs `shouldMatchList` map fst someRequests
        allReqs <- getAllRequests redis
        allReqs `shouldMatchList` map fst someRequests

addWorkerTest :: MonadConnect m => Redis -> m ()
addWorkerTest redis = do
    queueReqs redis someRequests
    mvar <- liftIO newEmptyMVar
    Async.race_
        (waitingWorker mvar)
        (waitFor upToTenSeconds $ do
                jqs' <- getJobQueueStatus redis
                length (jqsWorkers jqs') `shouldBe` 1
                length (jqsPending jqs') `shouldBe` length someRequests - 1
                liftIO $ putMVar mvar ()
        )

heartbeatFailureTest :: MonadConnect m => Redis -> m ()
heartbeatFailureTest redis = do
    queueReqs redis someRequests
    mvar <- liftIO newEmptyMVar
    worker <- Async.async $ waitingWorker mvar
    waitFor upToTenSeconds $ do -- make sure the worker has sent its first heartbeat
        jqs <- getJobQueueStatus redis
        length (jqsWorkers jqs) `shouldBe` 1
    liftIO $ Async.cancel worker
    waitFor upToTenSeconds $ do -- wait for a heartbeat failure
        jqs <- getJobQueueStatus redis
        length (jqsHeartbeatFailures jqs) `shouldBe` 1
    liftIO $ putMVar mvar ()

completeJobTest :: MonadConnect m => Redis -> m ()
completeJobTest redis = do
    queueReqs redis someRequests
    mvar <- liftIO newEmptyMVar
    Async.race_
        (waitingWorker mvar)
        (do liftIO $ putMVar mvar ()  -- the worker should finish the job now.
            waitFor upToTenSeconds $ do
                stats' <- getAllRequestStats redis
                length (filter (isJust . rsComputeFinishTime . snd) stats')
                    `shouldBe` 1
        )

clearFailuresTest :: MonadConnect m => Redis -> m ()
clearFailuresTest redis = do
    queueReqs redis someRequests
    clearHeartbeatFailures redis
    jqs <- getJobQueueStatus redis
    length (jqsHeartbeatFailures jqs) `shouldBe` 0

spec :: Spec
spec = do
    redisIt "reports queued requests" queueRequestsTest
    redisIt "reports added worker and enqueued job" addWorkerTest
    redisIt "reports heartbeat failure" heartbeatFailureTest
    redisIt "reports completed jobs" completeJobTest
    redisIt "clearing the list of heartbeat failures works" clearFailuresTest
