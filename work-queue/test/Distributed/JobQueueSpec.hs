{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE NoImplicitPrelude #-}

module Distributed.JobQueueSpec where

import ClassyPrelude
import Control.Concurrent.Async.Lifted (withAsync)
import Control.Concurrent.Lifted (threadDelay, fork, killThread)
import Control.Monad.Logger (runStdoutLoggingT)
import Data.Binary (decode)
import Data.List.Split (chunksOf)
import Distributed.JobQueue
import Distributed.RedisQueue
import Distributed.WorkQueueSpec (forkMasterSlaveNoBlock, redisTestPrefix)
import FP.Redis
import System.Timeout (timeout)
import Test.Hspec (Spec, it, shouldBe)

spec :: Spec
spec = do
    it "Runs enqueued computations" $ do
        killMaster <- forkMasterSlaveNoBlock "redis1"
        resultVar <- newEmptyMVar
        tid <- fork $ runDispatcher resultVar
        putStrLn "Blocking"
        result <- timeout (5 * 1000 * 1000) $ takeMVar resultVar
        (result `shouldBe` Just 0)
            `finally` killMaster >> killThread tid
        killMaster
    it "Doesn't lose data when master fails" $ do
        killMaster0 <- forkMasterSlaveNoBlock "redis0"
        resultVar <- newEmptyMVar
        tid <- fork $ runDispatcher resultVar
        threadDelay (100 * 1000)
        -- Expect no results, because there are no master or slave jobs.
        noResult <- tryTakeMVar resultVar
        (noResult `shouldBe` Nothing)
            `finally` killMaster0
            `onException` killThread tid
        killMaster1 <- forkMasterSlaveNoBlock "redis1"
        result <- timeout (5 * 1000 * 1000) $ takeMVar resultVar
        (result `shouldBe` Just 0)
            `finally` killMaster1 >> killThread tid

runDispatcher :: MVar Int -> IO ()
runDispatcher resultVar = do
    let localhost = connectInfo "localhost"
    runStdoutLoggingT $ withRedisInfo redisTestPrefix localhost $ \redis -> do
        client <- liftIO (newClientVars (Seconds 2) (Seconds 3600))
        withAsync (jobQueueClient client redis) $ \_ -> do
            -- Push a single set of work requests.
            let workItems = fromList (chunksOf 100 [1..(2^(8 :: Int))-1]) :: Vector [Int]
            response <- jobQueueRequest client redis workItems
            putStrLn "Putting"
            putMVar resultVar (decode (fromStrict response))
