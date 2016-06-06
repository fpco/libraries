{-# OPTIONS_GHC -fno-warn-orphans #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TemplateHaskell #-}
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
import           Data.TypeFingerprint (mkHasTypeFingerprint)
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

$(mkHasTypeFingerprint =<< [t| ByteString |])

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
waitShortInterval = threadDelay $ 500 * 1000

    
queueRequests :: MonadConnect m => Redis -> m ()
queueRequests redis = do
    -- send requests
    _ <- liftIO $ mapM startClient reqs
    liftIO waitShortInterval
    -- check that both getAllRequests and the list of pending requests match the submitted jobs
    jqs <- getJobQueueStatus redis
    jqsPending jqs `shouldMatchList` map fst reqs
    allReqs <- getAllRequests redis
    allReqs `shouldMatchList` map fst reqs
  where reqs = [(RequestId . pack . show $ n, pack . show $ n) | n <- [0..10 :: Int]]

addWorker :: MonadConnect m => Redis -> m ()
addWorker redis = do
    jqs <- getJobQueueStatus redis
    mvar <- liftIO newEmptyMVar
    _ <- liftIO $ waitingWorker mvar
    liftIO waitShortInterval
    jqs' <- getJobQueueStatus redis
    length (jqsWorkers jqs') `shouldBe` length (jqsWorkers jqs) + 1
    length (jqsPending jqs') `shouldBe` max 0 (length (jqsPending jqs) - 1)

heartbeatFailure :: MonadConnect m => Redis -> m ()
heartbeatFailure redis = do
    jqs <- getJobQueueStatus redis
    mvar <- liftIO newEmptyMVar
    worker <- liftIO $ waitingWorker mvar
    liftIO waitShortInterval
    liftIO $ Async.cancel worker
    liftIO waitForHeartbeat
    -- we should now have a heartbeat failure
    jqs' <- getJobQueueStatus redis
    length (jqsHeartbeatFailures jqs') `shouldBe` length (jqsHeartbeatFailures jqs) + 1

completeJob :: MonadConnect m => Redis -> m ()
completeJob redis = do
    stats <- getAllRequestStats redis
    mvar <- liftIO newEmptyMVar
    _ <- liftIO $ waitingWorker mvar
    liftIO $ putMVar mvar ()
    liftIO waitShortInterval
    stats' <- getAllRequestStats redis
    length (filter (isJust . rsComputeFinishTime . snd) stats')
        `shouldBe`
        length (filter (isJust . rsComputeFinishTime . snd) stats) + 1

clearFailures :: MonadConnect m => Redis -> m ()
clearFailures redis = do
    clearHeartbeatFailures redis
    jqs <- getJobQueueStatus redis
    length (jqsHeartbeatFailures jqs) `shouldBe` 0

-- | Version of 'redisIt' that does not clear the database.
redisIt' :: String -> (forall m. (MonadConnect m) => Redis -> m ()) -> Spec
redisIt' msg cont = loggingIt msg $
    withRedis testRedisConfig cont

spec :: Spec
spec = do
    redisIt "reports queued requests" queueRequests
    redisIt' "reports added worker and enqueued job" addWorker
    redisIt' "reports heartbeat failure" heartbeatFailure
    redisIt' "reports completed jobs" completeJob
    redisIt' "clearing the list of heartbeat failures works" clearFailures
