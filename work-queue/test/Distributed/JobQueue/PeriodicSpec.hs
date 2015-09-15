{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE CPP #-}

module Distributed.JobQueue.PeriodicSpec where

import ClassyPrelude
import Control.Concurrent.Async (Async)
import Control.Concurrent.Lifted (threadDelay, myThreadId, throwTo, killThread)
import Control.Exception (ErrorCall(..))
import Control.Monad.Logger (runStdoutLoggingT)
import Control.Monad.Trans.Resource (ResourceT, resourceForkIO)
import Distributed.JobQueue.Periodic (periodicEx)
import Distributed.JobQueueSpec (jqit, allocateAsync, randomDelay)
import Distributed.WorkQueueSpec (redisTestPrefix)
import FP.Redis
import FP.Redis.Mutex hiding (mutexKey)
import Test.Hspec (Spec, shouldSatisfy)

spec :: Spec
spec = do
#ifndef LONG_TESTS
    -- Always run a basic sanity test
    jqit "short actions work (one thread)" $
        testPeriodic 1 2 2 0 500 3 0 $ \run -> void run
#else
    jqit "short actions work (one thread)" $
        testPeriodic 1 2 2 0 500 10 0 $ \run -> void run
    jqit "short actions work (2 threads)" $
        testPeriodic 1 2 2 0 500 30 0 $ \run ->
            replicateM_ 2 (randomDelay 0 1000 >> run)
    jqit "short actions work (50 threads)" $
        testPeriodic 1 2 2 0 500 30 0 $ \run ->
            replicateM_ 50 (randomDelay 0 1000 >> run)
    jqit "long actions work (2 threads)" $
        testPeriodic 1 2 2 5000 5000 30 0 $ \run ->
            replicateM_ 2 (randomDelay 0 1000 >> run)
    jqit "long actions work (50 threads)" $
        testPeriodic 1 2 2 5000 5000 30 0 $ \run ->
            replicateM_ 50 (randomDelay 0 1000 >> run)
    jqit "long periods work (50 threads)" $
        testPeriodic 5 2 2 0 500 30 0 $ \run ->
            replicateM_ 50 (randomDelay 0 1000 >> run)
    -- TODO: In order to reinstate this test, we need a way to rudely
    -- quit the thread / process without releasing the mutex.
    --
    -- jqit "if the action dies, it recovers after the ttl" $
    --     testPeriodic 4 1 10 1000 1000 30 2 $ \run -> do
    --         a <- run
    --         threadDelay (1000 * 1000)
    --         lift $ cancel a
    --         void run
    --         replicateM_ 2 (randomDelay 0 1000 >> run)
#endif

type M = ResourceT IO

testPeriodic
    :: Int -> Int -> Int -> Int -> Int -> Int -> Int
    -> (M (Async ()) -> M ())
    -> M ()
testPeriodic ivl freq mutexTtl delayLower delayUpper runtime delta f = do
    counter <- newIORef 0
    running <- newIORef False
    tid <- myThreadId
    forked <- resourceForkIO $ f $
        runPeriodic (toSeconds ivl) (toSeconds freq) (toSeconds mutexTtl) $ do
            conc <- readIORef running
            if conc
                then throwTo tid $ ErrorCall "Concurrent executions of periodic action"
                else flip finally (atomicWriteIORef running False) $ do
                    atomicWriteIORef running True
                    randomDelay delayLower delayUpper
                    atomicModifyIORef' counter (\n -> (n + 1, ()))
    threadDelay (1000 * 1000 * runtime)
    killThread forked
    n <- readIORef counter
    let delayLower', delayUpper' :: Double
        delayLower' = fromIntegral delayLower / 1000
        delayUpper' = fromIntegral delayUpper / 1000
        lower, upper :: Int
        lower = floor (fromIntegral runtime / (delayUpper' + fromIntegral (ivl + freq)))
        upper = ceiling (fromIntegral runtime / (delayLower' + fromIntegral ivl))
    lift $ (lower, n + delta, upper) `shouldSatisfy` (\(l, x, u) -> x >= l && x <= u)

toSeconds :: Int -> Seconds
toSeconds = Seconds . fromIntegral

runPeriodic
    :: Seconds -> Seconds -> Seconds -> IO () -> M (Async ())
runPeriodic ivl freq mutexTtl f =
    allocateAsync $ runStdoutLoggingT $ withConnection (connectInfo "localhost") $ \conn ->
        periodicEx conn mutexKey' ivl freq mutexTtl (lift f)

mutexKey' :: MutexKey
mutexKey' = MutexKey $ Key $ redisTestPrefix ++ ".periodic-mutex"
