{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

module Distributed.JobQueueSpec (spec) where

import           ClassyPrelude hiding (keys)
import           Control.Concurrent (forkIO, myThreadId, ThreadId)
import           Control.Concurrent.Async (Async, race, async, cancel, waitCatch, cancel, race_)
import           Control.Concurrent.Lifted (threadDelay)
import           Control.Exception (AsyncException(ThreadKilled), throwTo)
import           Control.Monad.Base (MonadBase)
import           Control.Monad.Logger
import           Control.Monad.Trans.Control (MonadBaseControl, liftBaseWith)
import           Control.Monad.Trans.Resource (MonadResource, ResourceT, runResourceT, register, allocate)
import           Data.Bits (shiftL, xor, zeroBits)
import           Data.List.NonEmpty (NonEmpty(..))
import           Data.List.Split (chunksOf)
import           Data.Proxy (Proxy(..))
import           Data.Serialize (Serialize)
import           Data.Serialize (encode)
import qualified Data.Set as S
import           Data.Streaming.NetworkMessage (Sendable)
import           Data.Time.Clock (diffUTCTime)
import           Data.TypeFingerprint
import qualified Distributed.JobQueue.Client as JQ
import           Distributed.JobQueue.Worker
import           Distributed.TestUtil
import           Distributed.Types
import           FP.Redis hiding (Response)
import           FP.ThreadFileLogger
import           System.Mem.Weak
import           System.Random (randomRIO)
import           System.Timeout.Lifted (timeout)
import           Test.Hspec (Spec, it)
import           Test.Hspec.Expectations.Lifted (shouldBe, shouldReturn)
import qualified Control.Concurrent.STM as STM
import qualified Data.UUID as UUID
import qualified Data.UUID.V4 as UUID.V4
import           Control.Monad.Trans.Control (control)
import qualified Crypto.Hash.SHA256 as SHA256

import           Distributed.ConnectRequest
import           Distributed.Redis (withRedis)
import qualified Distributed.WorkQueue as WQ
import qualified Data.WorkQueue as WQ
import qualified Distributed.JobQueue.WorkQueue as JQ
import           Data.TypeFingerprintSpec ()
import qualified Distributed.JobQueue.Status as JQ

data Request = Request (Vector [Int])
    deriving (Eq, Show, Generic, Typeable)

instance Serialize Request

data Response = Response Int
    deriving (Eq, Show, Generic, Typeable)

instance Serialize Response

$(mkManyHasTypeFingerprint [ [t| Request |], [t| Response |] ])

spec :: Spec
spec = do
    testcase "Runs an enqueued computation" $ do
        (_, respStm) <- runJobClientAndRequest
        _ <- runXorWorker
        JQ.atomicallyReturnOrThrow respStm `shouldReturn` Response 0
    testcase "Doesn't yield a value when there are no workers" $ do
        (_, respStm) <- runJobClientAndRequest
        threadDelay (200 * 1000)
        liftIO (atomically respStm) `shouldReturn` (Nothing :: Maybe (Either DistributedException Response))
    testcase "Doesn't lose data when worker fails" $ do
        (_, respStm) <- runJobClientAndRequest
        worker <- runDelayWorker
        threadDelay (100 * 1000)
        liftIO $ cancel worker
        threadDelay (500 * 1000)
        liftIO (atomically respStm) `shouldReturn` (Nothing :: Maybe (Either DistributedException Response))
        worker <- runXorWorker
        JQ.atomicallyReturnOrThrow respStm `shouldReturn` Response 0
    testcase "Sends an exception to the client on type mismatch" $ do
        (_, respStm) <- runJobClientAndRequest
        worker <- runWorker $ \_ _ () -> return ()
        JQ.atomicallyReturnOrThrow respStm `shouldThrow` \ex ->
            case ex of
                TypeMismatch {} -> True
                _ -> False
    -- FIXME: return to this part of the test-suite.
    --
    -- In the old implementation, there's some pretty complicated sanity
    -- checking. I think it makes sense to turn as much of that as
    -- possible into a library feature. The library should record enough
    -- information that various invariants can be checked. Then, the
    -- clients can periodically run sanity checks.
{-
    testcase "Preserves data despite slaves being started and killed periodically (all using the same request)" $ do
        resultVars <- replicateM 10 runJobClientAndRequest
        liftIO $ void $
            randomSlaveSpawner "redis" `race`
            randomSlaveSpawner "redis" `race`
            randomSlaveSpawner "redis" `race`
            checkResults 60 resultVars (repeat 0)
    testcase "Multiple clients can make requests to multiple workers" $ do
        fail "FIXME: implement"
    testcase "Can cancel active requests" $ do
        fail "FIXME: implement"
-}
    jqit "Preserves data despite slaves being started and killed periodically (all using the same request)" $ do
        resultVars <- replicateM 10 forkDispatcher
        liftIO $ void $
            randomSlaveSpawner runJQWorker_ `race`
            randomSlaveSpawner runJQWorker_ `race`
            randomSlaveSpawner runJQWorker_ `race`
            checkResults 60 resultVars (repeat 0)
    jqit "Preserves data despite slaves being started and killed periodically (different requests)" $ do
        resultVars <- forM [1..3] (forkDispatcher' . mkRequest)
        liftIO $ void $
            randomSlaveSpawner runJQWorker_ `race`
            randomSlaveSpawner runJQWorker_ `race`
            randomSlaveSpawner runJQWorker_ `race`
            checkResults 100 resultVars (repeat 0)
    jqit "Works despite clients and workers being started and killed periodically" $ do
        (sets :: RequestSets Int) <- mkRequestSets
        liftIO $ void $
            randomSlaveSpawner runJQWorkerLong `race`
            randomSlaveSpawner runJQWorkerLong `race`
            randomSlaveSpawner runJQWorkerLong `race`
            randomSlaveSpawner runJQWorkerLong `race`
            randomWaiterSpawner "waiter-1" sets `race`
            randomWaiterSpawner "waiter-2" sets `race`
            -- Run job requesters for 10 seconds, then check that all
            -- the responses eventually came back.
            (do void $ timeout (1000 * 1000 * 10) $
                    randomJobRequester "job-requester-1" sets 254 `race`
                    randomJobRequester "job-requester-2" sets 255
                -- Give some more time for the job requesters to do their thing.
                threadDelay (1000 * 1000 * 20))
        putStrLn "Will now check the answers"
        checkRequestsAnswered (== 0) sets 120
    jqit "Sends an exception to the client on type mismatch" $ do
        resultVar <- forkDispatcher' defaultRequest
        pid1 <- forkJQWorker runJQWorker_ 0
        pid2 <- forkJQWorker runJQWorker_ 0
        ex <- getException 5 (resultVar :: MVar (Either DistributedException Bool))
        cancelJQWorker pid1
        cancelJQWorker pid2
        case ex of
            TypeMismatch {} -> return ()
            _ -> fail $ "Expected TypeMismatch, but got " <> show ex
    jqit "Multiple clients can make requests to multiple workers" $ do
        let workerCount = 4
            requestCount = 20
        -- At least one worker needs a local slave, so that progress
        -- is made when they all initially become masters.
        pid0 <- forkJQWorker runJQWorker_ 1
        pids <- replicateM (workerCount - 1) (forkJQWorker runJQWorker_ 0)
        resultVars <- mapM (forkDispatcher' . mkRequest) [1..requestCount]
        eresults <- timeout (30 * 1000 * 1000) $ mapM takeMVar resultVars
        mapM cancelJQWorker (pid0 : pids)
        case eresults of
            Just (partitionEithers -> ([], xs)) -> liftIO $ xs `shouldBe` (replicate requestCount 0 :: [Int])
            Just (partitionEithers -> (errs, _)) -> fail $ "Got errors: " ++ show errs
            _ -> fail "Timed out waiting for values"
    jqit "Can cancel active requests" $ do
        resultVar <- forkDispatcher' (fromList [[1000 * 8]] :: Vector [Int])
        pid <- forkJQWorker runJQWorkerLong 1
        threadDelay (1000 * 1000)
        let withRedis' = runThreadFileLoggingT . withRedis redisConfig
            getWorkerStatus = do
                mworker <- listToMaybe . JQ.jqsWorkers <$> withRedis' JQ.getJobQueueStatus
                case mworker of
                    Nothing -> fail "Couldn't find worker"
                    Just worker -> return worker
        worker <- getWorkerStatus
        when (null $ JQ.wsRequests worker) $
            fail "Worker didn't get request"
        forM_ (JQ.wsRequests worker) $ \req -> do
            success <- withRedis' $ \redis ->
                JQ.cancelRequest (Seconds 60) redis req (Just (JQ.wsWorker worker))
            liftIO $ success `shouldBe` True
        threadDelay (1000 * 1000)
        -- Test that the worker is indeed now available.
        resultVar' <- forkDispatcher' (fromList [[1000]] :: Vector [Int])
        threadDelay (1000 * 1000 * 3)
        mresult <- tryTakeMVar resultVar'
        liftIO $ mresult `shouldBe` Just (Right (0 :: Int))
        -- And that the other result comes in with a canceled exception.
        mresult' <- takeMVar resultVar
        cancelJQWorker pid
        case mresult' of
            Left (RequestCanceled {}) -> return ()
            _ -> fail $ "Instead of canceled exception, got " ++
                show (mresult' :: Either DistributedException Int)

-- Maximum test time of 2 minutes.  Individual tests can of course further restrict this
testcase :: String -> ResourceT IO () -> Spec
testcase = testcaseTimeout (Seconds 120)

testcaseTimeout :: Seconds -> String -> ResourceT IO () -> Spec
testcaseTimeout (Seconds s) name f = it name $ do
    mres <- timeout (fromIntegral s * 1000 * 1000) $ clearRedisKeys >> runResourceT f
    case mres of
        Nothing -> fail "Timed out waiting for test to finish"
        Just _ -> return ()

runJobClientAndRequest
    :: (MonadCommand m, MonadResource m, Sendable response)
    => m (JQ.JobClient response, STM (Maybe (Either DistributedException response)))
runJobClientAndRequest = do
    jc <- newJobClient config
    respStm <- JQ.submitRequestAndWaitForResponse jc r0 (mkRequest 0)
    return (jc, respStm)

newJobClient
    :: (MonadCommand m, MonadResource m, Sendable response)
    => JobQueueConfig -> m (JQ.JobClient response)
newJobClient config = do
    logFunc <- runThreadFileLoggingT $ withLogTag "jobClient" askLoggerIO
    jc <- JQ.newJobClient logFunc config
    wp <- liftIO $ mkWeakPtr jc Nothing
    _ <- register $ do
        let loop ix | ix <= 0 = fail "Timed out waiting for job client to be garbage collected"
            loop ix = do
                mjc <- deRefWeak wp
                case mjc of
                    Nothing -> return ()
                    Just _ -> do
                        threadDelay (100 * 1000)
                        loop (ix - 1)
        loop 100
    return jc

runXorWorker :: ResourceT IO (Async ())
runXorWorker = runWorker $ \_ _ (Request xs) -> return $ Response $ foldl' (foldl' xor) zeroBits xs

-- Delays half a second and returns 42
runDelayWorker :: ResourceT IO (Async ())
runDelayWorker = runWorker $ \_ _ (Request _) -> do
    liftIO $ threadDelay (1000 * 500)
    return $ Response 42

runWorker
    :: (Sendable request, Sendable response)
    => (Redis -> RequestId -> request -> LoggingT IO response)
    -> ResourceT IO (Async ())
runWorker = allocateAsync . runThreadFileLoggingT . jobWorker config

redisConfig :: RedisConfig
redisConfig = defaultRedisConfig { rcKeyPrefix = redisTestPrefix }

config :: JobQueueConfig
config = defaultJobQueueConfig
    { jqcRedisConfig = redisConfig
    , jqcHeartbeatConfig = HeartbeatConfig
        { hcSenderIvl = Seconds 1
        , hcCheckerIvl = heartbeatCheckIvl
        }
    }

r0 :: RequestId
r0 = RequestId "0"

-- These lists always xor together to 0
mkRequest :: Int -> Request
mkRequest offset = Request $ fromList $ chunksOf 10 $ map (`shiftL` offset) [1..(2^(8 :: Int))-1]

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

jqit :: String -> ResourceT IO () -> Spec
jqit name f = it name $ clearRedisKeys >> runResourceT f

forkDispatcher :: ResourceT IO (MVar (Either DistributedException Int))
forkDispatcher = forkDispatcher' defaultRequest

defaultRequest :: Request
defaultRequest = mkRequest 0

{-
-- These lists always xor together to 0
mkRequest :: Int -> Vector [Int]
mkRequest offset = fromList $ chunksOf 10 $ map (`shiftL` offset) [1..(2^(8 :: Int))-1]
-}

forkDispatcher'
    :: (Sendable request, Sendable response)
    => request -> ResourceT IO (MVar (Either DistributedException response))
forkDispatcher' request = do
    resultVar <- newEmptyMVar
    void $ allocateAsync $ runDispatcher request resultVar
    return resultVar

runDispatcher
    :: (Sendable request, Sendable response)
    => request -> MVar (Either DistributedException response) -> IO ()
runDispatcher request resultVar = do
    logFunc <- runThreadFileLoggingT $ withLogTag "dispatcher" askLoggerIO
    client <- JQ.newJobClient logFunc config
    let k = RequestId (SHA256.hash (encode request))
    result <- liftIO $
        waitForSTMMaybe =<< JQ.submitRequestAndWaitForResponse client k request
    putMVar resultVar result
  where
    waitForSTMMaybe m = atomically $ do
        mbRes <- m
        case mbRes of
            Nothing -> STM.retry
            Just res -> return res

sendJobRequest
    :: forall request response. (Sendable request, Sendable response)
    => LogTag -> RequestSets response -> request -> IO RequestId
sendJobRequest tag sets request = runResourceT $ runThreadFileLoggingT $ withLogTag tag $ do
    logFunc <- askLoggerIO
    jc <- JQ.newJobClient logFunc config
    let rid = RequestId (SHA256.hash (encode request))
    mresult <- JQ.submitRequest jc rid request
    case mresult of
        Just (_ :: response) -> do
            $logError $ "Didn't expect to find a cached result: " <> tshow rid
            fail "sendJobRequest failed"
        Nothing -> do
            $logInfo $ "Sent job request " ++ tshow rid
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
        logFunc <- runThreadFileLoggingT $ withLogTag tag askLoggerIO
        jc <- JQ.newJobClient logFunc config
        foldl' race_
               (forever $ threadDelay maxBound)
               (replicate 5 (waitForResponse jc) :: [IO ()])
    waitForResponse :: JQ.JobClient response -> IO ()
    waitForResponse cvs = forever $ do
        withItem (unwatchedRequests sets) $ \rid -> do
            act <- JQ.waitForResponse cvs rid
            result <- liftIO $ atomically $ do
                mb <- act
                case mb of
                    Nothing -> STM.retry
                    Just res -> return res
            case result of
                Left ex -> liftIO $ throwIO ex
                Right x -> do
                    runThreadFileLoggingT $ withLogTag tag ($logInfo $ "Got result for " <> tshow rid)
                    atomicInsert (rid, x) (receivedResponses sets)

{-
clientConfig :: ClientConfig
clientConfig = defaultClientConfig
    { clientHeartbeatCheckIvl = heartbeatCheckIvl
    }
-}

heartbeatCheckIvl :: Seconds
heartbeatCheckIvl = Seconds 2


runJQWorker_ :: LogFunc -> Int -> IO ()
runJQWorker_ logFunc lslaves = do
    let calc :: [Int] -> IO Int
        calc input = return $ foldl' xor zeroBits input
        inner :: Redis -> WorkerConnectInfo -> RequestId -> Request -> WQ.WorkQueue [Int] Int -> IO Int
        inner redis mci _requestId (Request request) queue = do
            WQ.withLocalSlaves queue lslaves calc $ do
                requestWorker redis mci
                subresults <- WQ.mapQueue queue request
                liftIO $ calc (otoList (subresults :: Vector Int))
    masterConf <- WQ.defaultMasterConfig
    JQ.workQueueJQWorker logFunc config masterConf calc inner

runJQWorkerLong :: LogFunc -> Int -> IO ()
runJQWorkerLong logFunc lslaves = do
    let calc :: [Int] -> IO Int
        calc (x : _) = do
            threadDelay (x * 1000)
            return 0
        calc _ = return 0
        inner :: Redis -> WorkerConnectInfo -> RequestId -> Request -> WQ.WorkQueue [Int] Int -> IO Int
        inner redis mci _requestId (Request request) queue = do
            WQ.withLocalSlaves queue lslaves calc $ do
                requestWorker redis mci
                results <- WQ.mapQueue queue request
                if results == fmap (\_ -> 0) request
                    then return 0
                    else return 1
    masterConf <- WQ.defaultMasterConfig
    JQ.workQueueJQWorker logFunc config masterConf calc inner

forkJQWorker :: (MonadIO m) => (LogFunc -> Int -> IO ()) -> Int -> m ThreadId
forkJQWorker workerFunc lslaves = liftIO $ do
    logFunc <- runThreadFileLoggingT $ withLogTag "randomSlaveSpawner" askLoggerIO
    forkIO (workerFunc logFunc lslaves)

cancelJQWorker :: (MonadIO m) => ThreadId -> m ()
cancelJQWorker pid = liftIO (throwTo pid ThreadKilled)

randomSlaveSpawner :: (LogFunc -> Int -> IO ()) -> IO ()
randomSlaveSpawner workerFunc = runResourceT $ runThreadFileLoggingT $ withLogTag "randomSlaveSpawner" $ forM_ [0..] $ \n -> do
    startTime <- liftIO getCurrentTime
    $logInfo $ "Forking worker at " ++ tshow startTime
    logFunc <- askLoggerIO
    pid <- forkJQWorker workerFunc 0
    randomDelay (500 * n) (500 + 500 * n)    
    $logInfo $ "Cancelling worker started at " ++ tshow startTime
    cancelJQWorker pid

randomWaiterSpawner :: (Sendable response, Ord response)
                    => LogTag -> RequestSets response -> IO ()
randomWaiterSpawner tag sets = runResourceT $ runThreadFileLoggingT $ withLogTag tag $ forM_ [0..] $ \n -> do
    startTime <- liftIO getCurrentTime
    $logInfo $ "Forking waiter at " ++ tshow startTime
    tid <- lift $ forkResponseWaiter tag sets
    randomDelay (500 * n) (2500 + 500 * n)
    $logInfo $ "Cancelling waiter started at " ++ tshow startTime
    liftIO $ cancel tid

randomJobRequester :: Sendable response => Text -> RequestSets response -> Int -> IO ()
randomJobRequester tag sets cnt = runResourceT $ runThreadFileLoggingT $ withLogTag (LogTag tag) $ forM_ [0..] $ \(n :: Int) -> do
    -- "redis-long" interprets this as a number of ms to delay.
    ms <- liftIO $ randomRIO (0, 200)
    let request :: Vector [Int]
        request = fromList $ map (ms:) $ chunksOf 100 [n..n+cnt]
    lift $ void $ allocateAsync (sendJobRequest (LogTag (tag ++ "-" ++ tshow n)) sets (Request request))
    randomDelay 100 200

checkRequestsAnswered :: (MonadIO m, MonadBaseControl IO m, Show response) => (response -> Bool) -> RequestSets response -> Int -> m ()
checkRequestsAnswered correct sets seconds = runThreadFileLoggingT $ withLogTag "checkRequestsAnswered" $ do
    lastSummaryRef <- newIORef (error "impossible: lastSummaryRef")
    let updateLastSummaryRef :: IO (Set RequestId, Set RequestId, Set RequestId)
        updateLastSummaryRef = do
            unwatched <- readIORef (unwatchedRequests sets)
            sent <- readIORef (sentRequests sets)
            received <- readIORef (receivedResponses sets)
            let wrongResults = S.filter (not . correct . snd) received
            unless (S.null wrongResults) $
                fail $ "Received wrong results: " ++ show wrongResults
            let unreceived = sent `S.difference` S.map fst received
            writeIORef lastSummaryRef (unwatched, sent, unreceived)            
            return (unwatched, sent, unreceived)
        loop = do
            (unwatched, sent, unreceived) <- updateLastSummaryRef
            -- NOTE: uncomment this for lots more debug info
            -- mapM_ ($logDebug . pack) . lines =<< getSummary
            if S.null unwatched && S.null unreceived
                then return ()
                else threadDelay (1000 * 1000) >> loop
        getSummary = liftIO $ do
            (unwatched, sent, unreceived) <- updateLastSummaryRef
            JQ.JobQueueStatus{..} <- runThreadFileLoggingT $
                withRedis redisConfig JQ.getJobQueueStatus
            -- TODO: May also make sense to check that requests we have received
            -- aren't in the pending / active lists.
            start <- getCurrentTime
            let summary :: String
                summary = intercalate ", " $ do
                    let errs =
                            (guard (not (null unwatched)) >> ["Some requests weren't watched"]) ++
                            (guard (null unreceived) >> ["All responses received"]) ++
                            (guard (not (null requestsLost)) >> ["Some request was lost! Argh!"]) ++
                            (guard (not (null oldHeartbeats)) >> ["Heartbeat checker failed to re-enqueue items"])
                    if null errs
                        then ["Test timed out too early, but no there was no data loss, and heartbeats seem to function."]
                        else errs
                requestsLost =
                    map fst $
                    filter (isNothing . snd) $
                    map (\x -> (x, find (\y -> x `elem` JQ.wsRequests y) jqsWorkers)) $
                    filter (`onotElem` jqsPending) (S.toList unreceived)
                ivl = fromIntegral (unSeconds heartbeatCheckIvl)
                oldHeartbeats = filter (maybe True (> ivl) . fmap (start `diffUTCTime`) . JQ.wsLastHeartbeat) jqsWorkers
            return $
                "\nsent = " ++ show sent ++
                "\nunwatched = " ++ show unwatched ++
                "\nunreceived = " ++ show unreceived ++
                "\npending = " ++ show jqsPending ++
                "\nactive = " ++ show jqsWorkers ++
                "\noldHeartbeats = " ++ show oldHeartbeats ++
                "\nrequestsLost = " ++ show requestsLost ++
                "\ntimeSinceLastHeartbeat = " ++ show jqsLastHeartbeat ++
                "\nsummary = " ++ show summary
    result <- liftIO (timeout (seconds * 1000 * 1000) loop)
    case result of
        Nothing -> do
            summary <- getSummary
            fail $ "Didn't receive all requests:" ++ summary
        Just () -> do
            sent <- readIORef (sentRequests sets)
            when (S.null sent) $ fail "Didn't send any requests."

randomDelay :: (MonadIO m, MonadBase IO m) => Int -> Int -> m ()
randomDelay minMillis maxMillis = do
    ms <- liftIO $ randomRIO (minMillis, maxMillis)
    threadDelay (1000 * ms)

checkResult :: MonadIO m => Int -> MVar (Either DistributedException Int) -> Int -> m ()
checkResult seconds resultVar expected = checkResults seconds [resultVar] [expected]

checkResults :: MonadIO m => Int -> [MVar (Either DistributedException Int)] -> [Int] -> m ()
checkResults seconds resultVars expecteds = liftIO $ do
    mresults <- timeout (seconds * 1000 * 1000) $ mapM takeMVar resultVars
    case mresults of
        Nothing -> fail "Timed out waiting for value(s)"
        Just results ->
            forM_ (zip results expecteds) $ \(result, expected) ->
                case result of
                    Left ex -> liftIO $ throwIO ex
                    Right x -> x `shouldBe` expected

getException :: (Show r, MonadIO m) => Int -> MVar (Either DistributedException r) -> m DistributedException
getException seconds resultVar = liftIO $ do
    result <- timeout (seconds * 1000 * 1000) $ takeMVar resultVar
    case result of
        Nothing -> fail "Timed out waiting for exception"
        Just (Right n) -> fail $ "Expected exception, but got " ++ show n
        Just (Left err) -> return err

{-
enqueueSlaveRequest :: MonadIO m => MasterConnectInfo -> m ()
enqueueSlaveRequest mci =
    liftIO $ runThreadFileLoggingT $ logNest "enqueueSlaveRequest" $ withRedis redisTestPrefix localhost $ \r -> do
        let encoded = encode mci
        run_ r $ lpush (slaveRequestsKey r) (encoded :| [])
-}

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
