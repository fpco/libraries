{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RecordWildCards #-}
module Distributed.HeartbeatSpec (spec) where

import           ClassyPrelude hiding (keys)
import           FP.Redis (Seconds(..))
import qualified Control.Concurrent.Async.Lifted.Safe as Async
import           FP.Redis (MonadConnect)
import qualified Control.Concurrent.STM as STM
import           Control.Concurrent (threadDelay)
import           Data.List.NonEmpty (NonEmpty)
import           Test.Hspec (Spec)

import           Distributed.Heartbeat
import           Distributed.JobQueue.Status
import           Distributed.JobQueue.Internal (setRedisSchemaVersion)
import           Distributed.Types
import           Distributed.Redis
import           TestUtils

-- Configs/init
-----------------------------------------------------------------------

heartbeatConfig :: HeartbeatConfig
heartbeatConfig = HeartbeatConfig
    { hcSenderIvl = Seconds 2
    , hcCheckerIvl = Seconds 1
    }

withHeartbeats_ :: (MonadConnect m) => Redis -> (Heartbeating -> m a) -> m a
withHeartbeats_ = withHeartbeats heartbeatConfig

withCheckHeartbeats_ :: (MonadConnect m) => Redis -> (NonEmpty WorkerId -> m () -> m ()) -> m a -> m a
withCheckHeartbeats_ = withCheckHeartbeats heartbeatConfig

-- Utilities
-----------------------------------------------------------------------

expectWorkers :: (Monad m) => [WorkerId] -> [WorkerId] -> m ()
expectWorkers expected got = do
    unless (expected == got) $
        fail ("Expected workers " ++ show (map unWorkerId expected) ++ ", got " ++ show (map unWorkerId got))

checkNoWorkers :: (MonadConnect m) => Redis -> m ()
checkNoWorkers r = do
    wids <- activeOrUnhandledWorkers r
    unless (null wids) $
        fail ("Expected no workers, but got " ++ show (map unWorkerId wids))

checkDeadWorker :: MonadConnect m => Redis -> WorkerId -> m ()
checkDeadWorker r wid = do
    bodies <- deadWorkers r
    unless (bodies == [wid]) $
        fail "Expected a dead worker, found none."

checkNoWorkersStatus :: MonadConnect m => Redis -> m ()
checkNoWorkersStatus r = do
    JobQueueStatus{..} <- getJobQueueStatus r
    unless (null jqsWorkers)$
        fail ("Expected no workers in status, but found" ++ show jqsWorkers)

checkDeadWorkerStatus :: MonadConnect m => Redis -> WorkerId -> m ()
checkDeadWorkerStatus r wid = do
    JobQueueStatus{..} <- getJobQueueStatus r
    case find ((==wid) . snd) jqsHeartbeatFailures of
        Nothing -> fail "Expected Heartbeat failure in status, but did not find it."
        Just _ -> return ()



-- Tests
-----------------------------------------------------------------------

detectDeadWorker :: (MonadConnect m) => Redis -> Int -> m ()
detectDeadWorker redis dieAfter = do
    stopChecking :: MVar () <- newEmptyMVar
    widVar :: MVar WorkerId <- newEmptyMVar
    Async.withAsync
        (withHeartbeats_ redis (\hbting -> putMVar widVar (heartbeatingWorkerId hbting) >> liftIO (threadDelay dieAfter)))
        (\worker -> do
            wid <- readMVar widVar
            withCheckHeartbeats_ redis
                (\wids markHandled -> do
                    expectWorkers [wid] (toList wids)
                    Async.cancel worker
                    markHandled
                    putMVar stopChecking ())
                (takeMVar stopChecking))
    wid <- readMVar widVar
    checkNoWorkers redis
    checkDeadWorker redis wid
    setRedisSchemaVersion redis -- we don't have a client that sets the scheme
    checkNoWorkersStatus redis
    checkDeadWorkerStatus redis wid

spec :: Spec
spec = do
    redisIt "Detects dead worker" (\redis -> detectDeadWorker redis 0)
    -- Delay for 5 secs, so that the second 'sendHeartbeat' in 'withHeartbeats'
    -- will be triggered. Even if more than 1 secs ought to be enough, I found
    -- that it'll often fail with less than 5 secs...
    redisIt "Detects dead worker (long)" (\redis -> detectDeadWorker redis (5 * 1000 * 1000))
    redisIt "Keeps detecting dead workers if they're not handled" $ \redis -> do
        widVar :: MVar WorkerId <- newEmptyMVar
        handleCalls :: TVar Int <- liftIO (newTVarIO 0)
        withHeartbeats_ redis (putMVar widVar . heartbeatingWorkerId)
        wid <- takeMVar widVar
        withCheckHeartbeats_ redis
            (\wids markHandled -> do
                expectWorkers [wid] (toList wids)
                calls <- atomically $ do
                    modifyTVar handleCalls (+1)
                    readTVar handleCalls
                when (calls == 3) $ do
                    markHandled
                    -- We need this too to ensure that the entire action exits
                    -- after 'markHandled' finished
                    atomically (modifyTVar handleCalls (+1)))
            (atomically $ do
                calls <- readTVar handleCalls
                unless (calls == 4) STM.retry)
        checkNoWorkers redis
        setRedisSchemaVersion redis
        checkNoWorkersStatus redis
