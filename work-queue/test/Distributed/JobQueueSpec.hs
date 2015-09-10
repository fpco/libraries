{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TemplateHaskell #-}

module Distributed.JobQueueSpec
    ( spec
    -- Used by PeriodicSpec
    , jqit
    , allocateAsync
    , randomDelay
    ) where

import ClassyPrelude hiding (keys)
import Control.Concurrent (forkIO, myThreadId, ThreadId)
import Control.Concurrent.Async (Async, race, async, cancel, waitCatch)
import Control.Concurrent.Lifted (threadDelay)
import Control.Exception (AsyncException(ThreadKilled), throwTo)
import Control.Monad.Logger
import Control.Monad.Trans.Resource (ResourceT, runResourceT, allocate)
import Data.Binary (encode)
import Data.List.NonEmpty (nonEmpty, NonEmpty(..))
import Data.List.Split (chunksOf)
import Data.Proxy (Proxy(..))
import Data.Streaming.NetworkMessage (Sendable)
import Distributed.JobQueue
import Distributed.RedisQueue
import Distributed.RedisQueue.Internal (run_)
import Distributed.WorkQueueSpec (redisTestPrefix, forkWorker, cancelProcess)
import FP.Redis
import FP.ThreadFileLogger
import System.Random (randomRIO)
import System.Timeout.Lifted (timeout)
import Test.Hspec (Spec, it, shouldBe)
import Data.Bits (shiftL)
import qualified Data.Set as S
import Control.Monad.Base (MonadBase)
import Control.Monad.Trans.Control (MonadBaseControl, liftBaseWith)

spec :: Spec
spec = do
    jqit "Runs enqueued computations" $ do
        _ <- forkWorker "redis" 0
        _ <- forkWorker "redis" 0
        forM_ [0..3] $ \n -> do
            resultVar <- forkDispatcher' (mkRequest n)
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
        resultVar <- forkDispatcher' (fromList [[5 * 1000]] :: Vector [Int])
        checkResult 15 resultVar 0
    jqit "Non-existent slave request is ignored" $ do
        -- Enqueue an erroneous slave request
        enqueueSlaveRequest (MasterConnectInfo "localhost" 1337)
        -- One worker will become a master, the other will get the
        -- erroneous slave request.
        _ <- forkWorker "redis" 0
        _ <- forkWorker "redis" 0
        resultVar <- forkDispatcher
        checkResult 5 resultVar 0
    jqit "Preserves data despite slaves being started and killed periodically" $ do
        resultVar <- forkDispatcher
        liftIO $ void $
            randomSlaveSpawner "redis" `race`
            randomSlaveSpawner "redis" `race`
            randomSlaveSpawner "redis" `race`
            checkResult 60 resultVar 0
    jqit "Works despite clients and workers being started and killed periodically" $ do
        (sets :: RequestSets Int) <- mkRequestSets
        liftIO $ void $
            randomSlaveSpawner "redis-long" `race`
            randomSlaveSpawner "redis-long" `race`
            randomSlaveSpawner "redis-long" `race`
            randomSlaveSpawner "redis-long" `race`
            randomWaiterSpawner "waiter-1" sets `race`
            randomWaiterSpawner "waiter-2" sets `race`
            -- Run job requesters for 2 seconds, then check that all
            -- the responses eventually came back.
            (do void $ timeout (1000 * 1000 * 2) $
                    randomJobRequester sets 254 `race`
                    randomJobRequester sets 255
                checkRequestsAnswered (== 0) sets 60)
    jqit "Sends an exception to the client on type mismatch" $ do
        resultVar <- forkDispatcher' defaultRequest
        _ <- forkWorker "redis" 0
        _ <- forkWorker "redis" 0
        ex <- getException 5 (resultVar :: MVar (Either DistributedJobQueueException Bool))
        case ex of
            TypeMismatch {} -> return ()
            _ -> fail $ "Expected TypeMismatch, but got " <> show ex
    jqit "Multiple clients can make requests to multiple workers" $ do
        let workerCount = 4
            requestCount = 20
        -- At least one worker needs a local slave, so that progress
        -- is made when they all initially become masters.
        _ <- forkWorker "redis" 1
        replicateM_ (workerCount - 1) $ void $ forkWorker "redis" 0
        resultVars <- mapM (forkDispatcher' . mkRequest) [1..requestCount]
        eresults <- timeout (30 * 1000 * 1000) $ mapM takeMVar resultVars
        case eresults of
            Just (partitionEithers -> ([], xs)) -> liftIO $ xs `shouldBe` (replicate requestCount 0 :: [Int])
            Just (partitionEithers -> (errs, _)) -> fail $ "Got errors: " ++ show errs
            _ -> fail "Timed out waiting for values"

jqit :: String -> ResourceT IO () -> Spec
jqit name f = it name $ clearRedisKeys >> runResourceT f

forkDispatcher :: ResourceT IO (MVar (Either DistributedJobQueueException Int))
forkDispatcher = forkDispatcher' defaultRequest

defaultRequest :: Vector [Int]
defaultRequest = mkRequest 0

-- These lists always xor together to 0
mkRequest :: Int -> Vector [Int]
mkRequest offset = fromList $ chunksOf 10 $ map (`shiftL` offset) [1..(2^(8 :: Int))-1]

forkDispatcher'
    :: (Sendable request, Sendable response)
    => request -> ResourceT IO (MVar (Either DistributedJobQueueException response))
forkDispatcher' request = do
    resultVar <- newEmptyMVar
    void $ allocateAsync $ runDispatcher request resultVar
    return resultVar

runDispatcher
    :: (Sendable request, Sendable response)
    => request -> MVar (Either DistributedJobQueueException response) -> IO ()
runDispatcher request resultVar =
    runThreadFileLoggingT $ logNest "dispatcher" $ withRedis redisTestPrefix localhost $ \redis ->
        withJobQueueClient clientConfig redis $ \cvs -> do
            -- Push a single set of work requests.
            result <- try $ jobQueueRequest clientConfig cvs redis request
            putMVar resultVar result

sendJobRequest
    :: forall request response. (Sendable request, Sendable response)
    => RequestSets response -> request -> IO RequestId
sendJobRequest sets request =
    runThreadFileLoggingT $ logNest "sendJobRequest" $ withRedis redisTestPrefix localhost $ \redis -> do
        mresult <- timeout (1000 * 1000) $ sendRequest clientConfig redis request
        let (ri, _) = prepareRequest clientConfig request (Proxy :: Proxy response)
        case mresult of
            Nothing -> do
                $logError $ "Timed out waiting for request to be sent: " <> tshow ri
                fail "sendJobRequest failed"
            Just (_, Just (_ :: response)) -> do
                $logError $ "Didn't expect to find a cached result: " <> tshow ri
                fail "sendJobRequest failed"
            Just (rid, Nothing) -> do
                $logDebug $ "Successfully sent job request " ++ tshow rid
                atomicInsert rid (sentRequests sets)
                atomicInsert rid (unwatchedRequests sets)
                return rid

forkResponseWaiter
    :: forall response. (Sendable response, Ord response)
    => LogTag
    -> RequestSets response
    -> ResourceT IO (Async ())
forkResponseWaiter tag sets = allocateAsync responseWaiter
  where
    -- Takes some items from the unwatchedRequests set and waits on it.
    responseWaiter :: IO ()
    responseWaiter = do
        runThreadFileLoggingT $ logNest tag $ withRedis redisTestPrefix localhost $ \redis -> do
            withJobQueueClient clientConfig redis $ \cvs ->
                foldl' raceLifted
                       (forever $ threadDelay maxBound)
                       (replicate 5 (waitForResponse cvs redis) :: [LoggingT IO ()])
    waitForResponse :: ClientVars (LoggingT IO) response -> RedisInfo -> LoggingT IO ()
    waitForResponse cvs redis = forever $ do
        withItem (unwatchedRequests sets) $ \rid -> do
            resultVar <- newEmptyMVar
            registerResponseCallback cvs redis rid $
                void . tryPutMVar resultVar
            result <- takeMVar resultVar
            case result of
                Left ex -> liftIO $ throwIO ex
                Right x -> atomicInsert (rid, x) (receivedResponses sets)

clientConfig :: ClientConfig
clientConfig = defaultClientConfig
    { clientHeartbeatCheckIvl = Seconds 2
    }

randomSlaveSpawner :: String -> IO ()
randomSlaveSpawner which = runResourceT $ runThreadFileLoggingT $ withLogTag "randomSlaveSpawner" $ forM_ [0..] $ \n -> do
    startTime <- liftIO getCurrentTime
    $logInfo $ "Forking worker at " ++ tshow startTime
    pid <- lift $ forkWorker which 0
    randomDelay (500 * n) (500 + 500 * n)
    $logInfo $ "Cancelling worker started at " ++ tshow startTime
    cancelProcess pid

randomWaiterSpawner :: (Sendable response, Ord response)
                    => LogTag -> RequestSets response -> IO ()
randomWaiterSpawner tag sets = runResourceT $ runThreadFileLoggingT $ withLogTag tag $ forM_ [0..] $ \n -> do
    startTime <- liftIO getCurrentTime
    $logInfo $ "Forking waiter at " ++ tshow startTime
    tid <- lift $ forkResponseWaiter tag sets
    randomDelay (500 * n) (2500 + 500 * n)
    $logInfo $ "Cancelling waiter started at " ++ tshow startTime
    liftIO $ cancel tid

randomJobRequester :: Sendable response => RequestSets response -> Int -> IO ()
randomJobRequester sets cnt = runResourceT $ runThreadFileLoggingT $ withLogTag "randomJobRequester" $ forM_ [0..] $ \(n :: Int) -> do
    -- "redis-long" interprets this as a number of ms to delay.
    ms <- liftIO $ randomRIO (0, 200)
    let request :: Vector [Int]
        request = fromList $ map (ms:) $ chunksOf 100 [n..n+cnt]
    lift $ void $ allocateAsync (sendJobRequest sets request)
    randomDelay 100 200

checkRequestsAnswered :: (MonadIO m, MonadBaseControl IO m, Show response) => (response -> Bool) -> RequestSets response -> Int -> m ()
checkRequestsAnswered correct sets seconds = do
    lastSummaryRef <- newIORef (error "impossible: lastSummaryRef")
    let loop = do
            unwatched <- readIORef (unwatchedRequests sets)
            sent <- readIORef (sentRequests sets)
            received <- readIORef (receivedResponses sets)
            let wrongResults = S.filter (not . correct . snd) received
            unless (S.null wrongResults) $
                fail $ "Received wrong results: " ++ show wrongResults
            let unreceived = sent `S.difference` (S.map fst received)
            writeIORef lastSummaryRef (unwatched, sent, unreceived)
            if S.null unwatched && S.null unreceived
                then return ()
                else threadDelay (10 * 1000) >> loop
    result <- timeout (seconds * 1000 * 1000) loop
    case result of
        Nothing -> do
            (unwatched, sent, unreceived) <- readIORef lastSummaryRef
            fail $ "Didn't receive all requests:" ++
                "\nsent = " ++ show sent ++
                "\nunwatched = " ++ show unwatched ++
                "\nunreceived = " ++ show unreceived ++
                "\nunreceived - unwatched = " ++ show (unreceived `S.difference` unwatched)
        Just () -> do
            sent <- readIORef (sentRequests sets)
            when (S.null sent) $ fail "Didn't send any requests."

randomDelay :: (MonadIO m, MonadBase IO m) => Int -> Int -> m ()
randomDelay minMillis maxMillis = do
    ms <- liftIO $ randomRIO (minMillis, maxMillis)
    threadDelay (1000 * ms)

checkResult :: MonadIO m => Int -> MVar (Either DistributedJobQueueException Int) -> Int -> m ()
checkResult seconds resultVar expected = liftIO $ do
    result <- timeout (seconds * 1000 * 1000) $ takeMVar resultVar
    case result of
        Just (Left ex) -> liftIO $ throwIO ex
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
    liftIO $ runThreadFileLoggingT $ logNest "clearRedisKeys" $ withConnection localhost $ \redis -> do
        matches <- runCommand redis $ keys (redisTestPrefix <> "*")
        mapM_ (runCommand_ redis . del) (nonEmpty matches)

{-
clearSlaveRequests :: MonadIO m => m ()
clearSlaveRequests =
    liftIO $ runThreadFileLoggingT $ logNest "clearSlaveRequests" $ withRedis redisTestPrefix localhost $ \r -> do
        run_ r $ del (unLKey (slaveRequestsKey r) :| [])
-}

enqueueSlaveRequest :: MonadIO m => MasterConnectInfo -> m ()
enqueueSlaveRequest mci =
    liftIO $ runThreadFileLoggingT $ logNest "enqueueSlaveRequest" $ withRedis redisTestPrefix localhost $ \r -> do
        let encoded = toStrict (encode mci)
        run_ r $ lpush (slaveRequestsKey r) (encoded :| [])

localhost :: ConnectInfo
localhost = connectInfo "localhost"

-- Keeping track of requests which have been sent, haven't yet been
-- watched, and have been received.

data RequestSets response = RequestSets
    { unwatchedRequests, sentRequests :: IORef (S.Set RequestId)
    , receivedResponses :: IORef (S.Set (RequestId, response))
    }

mkRequestSets :: (MonadBase IO m, Applicative m) => m (RequestSets response)
mkRequestSets = RequestSets <$>
    newIORef S.empty <*>
    newIORef S.empty <*>
    newIORef S.empty

atomicInsert :: (MonadBase IO m, Ord a) => a -> IORef (S.Set a) -> m ()
atomicInsert x ref = atomicModifyIORef' ref ((,()) . S.insert x)

atomicTake :: (MonadBase IO m, Ord a) => IORef (S.Set a) -> m (Maybe a)
atomicTake ref = atomicModifyIORef' ref $ \s ->
    case S.minView s of
        Nothing -> (s, Nothing)
        Just (x, s') -> (s', Just x)

withItem :: (MonadBaseControl IO m, Ord a) => IORef (S.Set a) -> (a -> m ()) -> m ()
withItem ref f = do
    mask $ \restore -> do
        mx <- atomicTake ref
        case mx of
            Nothing -> return ()
            Just x -> restore (f x) `onException` atomicInsert x ref

allocateAsync :: IO a -> ResourceT IO (Async a)
allocateAsync f = fmap snd $ allocate linkedAsync cancel
  where
    linkedAsync = do
        thread <- async f
        linkIgnoreThreadKilled thread
        return thread

-- Something that gives a capability like this should be in the async
-- library...
linkIgnoreThreadKilled :: Async a -> IO ()
linkIgnoreThreadKilled a = do
    me <- myThreadId
    void $ forkRepeat $ do
        eres <- waitCatch a
        case eres of
            Left (fromException -> Just ThreadKilled) -> return ()
            Left err -> throwTo me err
            Right _ -> return ()

raceLifted :: MonadBaseControl IO m => m a -> m b -> m ()
raceLifted f g =
    liftBaseWith $ \restore ->
        void $ restore f `race` restore g

-- Copied from async library

-- | Fork a thread that runs the supplied action, and if it raises an
-- exception, re-runs the action.  The thread terminates only when the
-- action runs to completion without raising an exception.
forkRepeat :: IO a -> IO ThreadId
forkRepeat action =
  mask $ \restore ->
    let go = do r <- try (restore action)
                case r of
                  Left (_ :: SomeException) -> go
                  _                         -> return ()
    in forkIO go
