{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE NoImplicitPrelude #-}

module Distributed.JobQueueSpec where

import ClassyPrelude
import Control.Concurrent.Async (withAsync)
import Control.Concurrent.Lifted (threadDelay, fork, killThread)
import Control.Monad.Logger (runStdoutLoggingT)
import Control.Monad.Trans.Control (control)
import Control.Monad.Trans.Resource (ResourceT, runResourceT, allocate)
import Data.Binary (encode)
import Data.List.NonEmpty (nonEmpty, NonEmpty(..))
import Data.List.Split (chunksOf)
import Distributed.JobQueue
import Distributed.RedisQueue
import Distributed.WorkQueueSpec (redisTestPrefix, forkWorker, cancelProcess)
import FP.Redis
import System.Timeout.Lifted (timeout)
import Test.Hspec (Spec, it, shouldBe)

spec :: Spec
spec = do
    it "Runs enqueued computations" $ do
        clearRedisKeys
        runResourceT $ do
            _ <- forkWorker "redis" 0
            _ <- forkWorker "redis" 0
            resultVar <- forkDispatcher
            checkResult resultVar 5 (Just 0)
    it "Doesn't lose data when master fails" $ do
        clearRedisKeys
        runResourceT $ do
            -- This worker will take the request and become a master.
            master <- forkWorker "redis" 0
            resultVar <- forkDispatcher
            threadDelay (100 * 1000)
            -- Expect no results, because there are no slaves.
            noResult <- tryTakeMVar resultVar
            liftIO $ do
                noResult `shouldBe` Nothing
                cancelProcess master
                putStrLn "=================================="
                putStrLn "Master cancelled"
                putStrLn "=================================="
            -- One of these will become a master and one will become a
            -- slave.
            --
            -- This also tests the property that if there are stale
            -- slave requests, they get ignored because the connection
            -- fails.
            _ <- forkWorker "redis" 0
            _ <- forkWorker "redis" 0
            --FIXME: once "Non-existent slave request is ignored" is
            --fixed, this fork worker can be removed.  The issue is
            --that one of the workers above takes a stale slave
            --request.
            _ <- forkWorker "redis" 0
            checkResult resultVar 10 (Just 0)
    it "Long tasks complete" $ do
        clearRedisKeys
        runResourceT $ do
            _ <- forkWorker "redis-long" 0
            _ <- forkWorker "redis-long" 0
            resultVar <- forkDispatcher
            checkResult resultVar 15 (Just 0)
    it "Non-existent slave request is ignored" $ do
        clearRedisKeys
        runResourceT $ do
            -- Enqueue an erroneous slave request
            enqueueSlaveRequest (SlaveRequest "localhost" 1337)
            -- One worker will become a master, the other will get the
            -- erroneous slave request.
            _ <- forkWorker "redis" 0
            _ <- forkWorker "redis" 0
            resultVar <- forkDispatcher
            checkResult resultVar 5 (Just 0)

forkDispatcher :: ResourceT IO (MVar Int)
forkDispatcher = do
    resultVar <- newEmptyMVar
    void $ allocate (fork $ runDispatcher resultVar) killThread
    return resultVar

runDispatcher :: MVar Int -> IO ()
runDispatcher resultVar =
    runStdoutLoggingT $ withRedis redisTestPrefix localhost $ \redis -> do
        client <- liftIO (newClientVars (Seconds 2) (Seconds 3600))
        control $ \restore ->
            withAsync (restore $ jobQueueClient client redis) $ \_ -> restore $ do
                -- Push a single set of work requests.
                let workItems = fromList (chunksOf 100 [1..(2^(8 :: Int))-1]) :: Vector [Int]
                response <- jobQueueRequest client redis workItems
                putStrLn "Putting"
                putMVar resultVar response

checkResult :: MonadIO m => MVar Int -> Int -> Maybe Int -> m ()
checkResult resultVar seconds expected = liftIO $ do
    result <- timeout (seconds * 1000 * 1000) $ takeMVar resultVar
    result `shouldBe` expected

clearRedisKeys :: MonadIO m => m ()
clearRedisKeys =
    liftIO $ runStdoutLoggingT $ withConnection localhost $ \redis -> do
        matches <- runCommand redis $ keys (redisTestPrefix <> "*")
        mapM_ (runCommand_ redis . del) (nonEmpty matches)

clearSlaveRequests :: MonadIO m => m ()
clearSlaveRequests =
    liftIO $ runStdoutLoggingT $ withRedis redisTestPrefix localhost $ \r -> do
        runCommand_ (redisConnection r) $ del (unLKey (slaveRequestsKey r) :| [])

enqueueSlaveRequest :: MonadIO m => SlaveRequest -> m ()
enqueueSlaveRequest sr =
    liftIO $ runStdoutLoggingT $ withRedis redisTestPrefix localhost $ \r -> do
        let encoded = toStrict (encode sr)
        runCommand_ (redisConnection r) $ lpush (slaveRequestsKey r) (encoded :| [])

localhost :: ConnectInfo
localhost = connectInfo "localhost"
