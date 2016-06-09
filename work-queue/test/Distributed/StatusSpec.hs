{-# OPTIONS_GHC -fno-warn-orphans #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE DataKinds #-}
module Distributed.StatusSpec (spec) where

import           Control.Concurrent (threadDelay)
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
import           FP.Redis (MonadConnect, Seconds (..))
import           Test.Hspec (Spec)
import           Test.Hspec.Expectations.Lifted
import           TestUtils

$(mkManyHasTypeHash [[t| ByteString |]])

type Request = ByteString
type Response = ByteString

startClient :: (RequestId, Request) -> IO (Async.Async ())
startClient (rid, req) = Async.async . runWithoutLogs . withJobClient testJobQueueConfig $ \jc -> do
    submitRequest jc rid req
    void (waitForResponse_ jc rid :: LoggingT IO (Maybe Response))

-- | This worker completes as soon as the 'MVar' is not empty.
waitingWorker :: MVar () -> IO (Async.Async ())
waitingWorker mvar = Async.async . runWithoutLogs . jobWorker testJobQueueConfig $ waitingWorkerFunc mvar

waitingWorkerFunc :: MVar () -> Redis -> RequestId -> Request -> (LoggingT IO) (Reenqueue Response)
waitingWorkerFunc mvar _ _ _ = do
    _ <- liftIO $ takeMVar mvar
    return (DontReenqueue "Done")

runWithoutLogs :: LoggingT IO a -> IO a
runWithoutLogs = runStdoutLoggingT . filterLogger (\_ _ -> False)

waitForHeartbeat :: IO ()
waitForHeartbeat = threadDelay $
    3 * 1000 * 1000 * (fromIntegral . unSeconds $ testHeartbeatCheckIvl)
waitShortInterval :: IO ()
waitShortInterval = threadDelay $ 2 * 1000 * 1000

someRequests :: [(RequestId, ByteString)]
someRequests = [(RequestId . pack . show $ n, pack . show $ n) | n <- [0..10 :: Int]]

queueReqs :: MonadConnect m => [(RequestId, Request)] -> m ()
queueReqs reqs = liftIO $ do
    mapM_ startClient reqs
    waitShortInterval

failHeartbeat :: MonadConnect m => m ()
failHeartbeat = do
    mvar <- liftIO newEmptyMVar
    worker <- liftIO $ waitingWorker mvar
    liftIO waitForHeartbeat
    liftIO $ Async.cancel worker
    liftIO waitForHeartbeat

queueRequestsTest :: MonadConnect m => Redis -> m ()
queueRequestsTest redis = do
    queueReqs someRequests
    -- check that both getAllRequests and the list of pending requests match the submitted jobs
    jqs <- getJobQueueStatus redis
    jqsPending jqs `shouldMatchList` map fst someRequests
    allReqs <- getAllRequests redis
    allReqs `shouldMatchList` map fst someRequests

addWorkerTest :: MonadConnect m => Redis -> m ()
addWorkerTest redis = do
    queueReqs someRequests
    mvar <- liftIO newEmptyMVar
    _ <- liftIO $ waitingWorker mvar
    liftIO waitShortInterval
    jqs' <- getJobQueueStatus redis
    length (jqsWorkers jqs') `shouldBe` 1
    length (jqsPending jqs') `shouldBe` length someRequests - 1

heartbeatFailureTest :: MonadConnect m => Redis -> m ()
heartbeatFailureTest redis = do
    queueReqs someRequests
    failHeartbeat
    jqs' <- getJobQueueStatus redis
    length (jqsHeartbeatFailures jqs') `shouldBe` 1

completeJobTest :: MonadConnect m => Redis -> m ()
completeJobTest redis = do
    queueReqs someRequests
    mvar <- liftIO newEmptyMVar
    _ <- liftIO $ waitingWorker mvar
    liftIO $ putMVar mvar () -- the worker should finish the job now.
    liftIO waitShortInterval
    stats' <- getAllRequestStats redis
    length (filter (isJust . rsComputeFinishTime . snd) stats')
        `shouldBe` 1

clearFailuresTest :: MonadConnect m => Redis -> m ()
clearFailuresTest redis = do
    queueReqs someRequests
    clearHeartbeatFailures redis
    jqs <- getJobQueueStatus redis
    length (jqsHeartbeatFailures jqs) `shouldBe` 0

spec :: Spec
spec = do
    flakyTest $ redisIt "reports queued requests" queueRequestsTest
    flakyTest $ redisIt "reports added worker and enqueued job" addWorkerTest
    flakyTest $ redisIt "reports heartbeat failure" heartbeatFailureTest
    flakyTest $ redisIt "reports completed jobs" completeJobTest
    flakyTest $ redisIt "clearing the list of heartbeat failures works" clearFailuresTest
