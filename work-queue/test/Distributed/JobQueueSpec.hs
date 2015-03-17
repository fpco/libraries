{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE ConstraintKinds #-}

module Distributed.JobQueueSpec where

import ClassyPrelude hiding (keys)
import Control.Concurrent.Async (race)
import Control.Concurrent.Lifted (threadDelay, fork, killThread)
import Control.Monad.Logger (runStdoutLoggingT)
import Control.Monad.Trans.Resource (ResourceT, runResourceT, allocate)
import Data.Binary (encode)
import Data.List.NonEmpty (nonEmpty, NonEmpty(..))
import Data.List.Split (chunksOf)
import Data.Streaming.NetworkMessage (Sendable)
import Distributed.JobQueue
import Distributed.RedisQueue
import Distributed.WorkQueueSpec (redisTestPrefix, forkWorker, cancelProcess)
import FP.Redis
import System.Random (randomRIO)
import System.Timeout.Lifted (timeout)
import Test.Hspec (Spec, it, shouldBe)

spec :: Spec
spec = do
    jqit "Runs enqueued computations" $ do
        _ <- forkWorker "redis" 0
        _ <- forkWorker "redis" 0
        resultVar <- forkDispatcher
        checkResult 5 resultVar 0
    jqit "Doesn't lose data when master fails" $ do
        -- This worker will take the request and become a master.
        master <- forkWorker "redis" 0
        resultVar <- forkDispatcher
        threadDelay (100 * 1000)
        -- Expect no results, because there are no slaves.
        noResult <- tryTakeMVar resultVar
        liftIO $ do
            isNothing noResult `shouldBe` True
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
        checkResult 10 resultVar 0
    jqit "Long tasks complete" $ do
        _ <- forkWorker "redis-long" 0
        _ <- forkWorker "redis-long" 0
        resultVar <- forkDispatcher
        checkResult 15 resultVar 0
    jqit "Non-existent slave request is ignored" $ do
        -- Enqueue an erroneous slave request
        enqueueSlaveRequest (SlaveRequest "localhost" 1337)
        -- One worker will become a master, the other will get the
        -- erroneous slave request.
        _ <- forkWorker "redis" 0
        _ <- forkWorker "redis" 0
        resultVar <- forkDispatcher
        checkResult 5 resultVar 0
    jqit "Preserves data despite slaves being started and killed periodically" $ do
        resultVar <- forkDispatcher
        let randomSlaveSpawner = runResourceT $ forever $ do
                pid <- forkWorker "redis" 0
                ms <- liftIO $ randomRIO (0, 200)
                threadDelay (1000 * ms)
                cancelProcess pid
            -- TODO: determine why this is needed.
            eventuallySpawnWithoutKilling = runResourceT $ do
                threadDelay (1000 * 1000 * 5)
                forkWorker "redis" 0
        liftIO $ void $
            randomSlaveSpawner `race`
            randomSlaveSpawner `race`
            eventuallySpawnWithoutKilling `race`
            eventuallySpawnWithoutKilling `race`
            checkResult 10 resultVar 0
    jqit "Sends an exception to the client on type mismatch" $ do
        resultVar <- forkDispatcher'
        _ <- forkWorker "redis" 0
        _ <- forkWorker "redis" 0
        ex <- getException 5 (resultVar :: MVar (Either DistributedJobQueueException Bool))
        case ex of
            TypeMismatch {} -> return ()
            _ -> fail $ "Expected TypeMismatch, but got " <> show ex

jqit :: String -> ResourceT IO () -> Spec
jqit name f = it name $ clearRedisKeys >> runResourceT f

forkDispatcher :: ResourceT IO (MVar (Either DistributedJobQueueException Int))
forkDispatcher = forkDispatcher'

forkDispatcher' :: Sendable a => ResourceT IO (MVar (Either DistributedJobQueueException a))
forkDispatcher' = do
    resultVar <- newEmptyMVar
    void $ allocate (fork $ runDispatcher resultVar) killThread
    return resultVar

runDispatcher :: Sendable a => MVar (Either DistributedJobQueueException a) -> IO ()
runDispatcher resultVar =
    runStdoutLoggingT $ withRedis redisTestPrefix localhost $ \redis ->
        withJobQueueClient config redis $ \cvs -> do
            -- Push a single set of work requests.
            let workItems = fromList (chunksOf 100 [1..(2^(8 :: Int))-1]) :: Vector [Int]
            result <- try $ jobQueueRequest config cvs redis workItems
            putMVar resultVar result
  where
    config = defaultClientConfig
        { clientHeartbeatCheckIvl = Seconds 2
        }

checkResult :: MonadIO m => Int -> MVar (Either DistributedJobQueueException Int) -> Int -> m ()
checkResult seconds resultVar expected = liftIO $ do
    result <- timeout (seconds * 1000 * 1000) $ takeMVar resultVar
    case result of
        Just (Left ex) -> throwM ex
        Just (Right x) -> x `shouldBe` expected
        Nothing -> fail "Timed out waiting for value"

getException :: (Show r, MonadIO m) => Int -> MVar (Either DistributedJobQueueException r) -> m DistributedJobQueueException
getException seconds resultVar = liftIO $ do
    result <- timeout (seconds * 1000 * 1000) $ takeMVar resultVar
    case result of
        Nothing -> fail "Timed out waiting for exception"
        Just (Right n) -> fail $ "Expected exception, but got " ++ show n
        Just (Left err) -> return err

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
