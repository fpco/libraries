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
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE DataKinds #-}

module Distributed.JobQueueSpec (spec) where

import ClassyPrelude
import Data.Store (Store)
import qualified Control.Concurrent.Async.Lifted.Safe as Async
import           Test.Hspec hiding (shouldBe)
import qualified Test.Hspec
import FP.Redis (MonadConnect, Seconds(..))
import Control.Concurrent (threadDelay)
import qualified Data.UUID as UUID
import qualified Data.UUID.V4 as UUID.V4
import Data.Void (absurd)
import qualified Data.ByteString.Char8 as BSC8
import qualified Data.HashSet as HS
import qualified Control.Concurrent.STM as STM

import Data.Store.TypeHash (mkManyHasTypeHash)
import Distributed.JobQueue.Client
import Distributed.JobQueue.Worker
import TestUtils
import Distributed.Types
import Distributed.Redis (withRedis)

-- * Utils
-----------------------------------------------------------------------

-- | Contains how many
data Request = Request
    { requestDelay :: !Int
    , requestResponse :: !(Reenqueue Response)
    } deriving (Eq, Show, Generic, Typeable)
instance Store Request

newtype Response = Response
    { _responseEcho :: ByteString
    } deriving (Eq, Ord, Show, Generic, Typeable)
instance Store Response

$(mkManyHasTypeHash [[t|Request|], [t|Response|]])

jobWorker_ :: (MonadConnect m) => (Request -> m (Reenqueue Response)) -> m void
jobWorker_ work = withRedis (jqcRedisConfig testJobQueueConfig) $ \r -> jobWorkerWithHeartbeats testJobQueueConfig r (\_wid -> work)

processRequest :: (MonadConnect m) => Request -> m (Reenqueue Response)
processRequest Request{..} = do
    liftIO (threadDelay requestDelay)
    return requestResponse

testJobWorkerOnStartWork :: (MonadConnect m) => m () -> m void
testJobWorkerOnStartWork onStartWork = jobWorker_ $ \req -> do
    onStartWork
    processRequest req

testJobWorker :: (MonadConnect m) => m void
testJobWorker = withRedis (jqcRedisConfig testJobQueueConfig) $ \r -> jobWorkerWithHeartbeats testJobQueueConfig r $ \_wid Request{..} -> do
    liftIO (threadDelay requestDelay)
    return requestResponse

withTestJobClient :: (MonadConnect m) => (JobClient Response -> m a) -> m a
withTestJobClient = withJobClient testJobQueueConfig

getRequestId :: (MonadIO m) => m RequestId
getRequestId = liftIO (RequestId . UUID.toASCIIBytes <$> UUID.V4.nextRandom)

submitTestRequest :: (MonadConnect m) => JobClient Response -> Request -> m RequestId
submitTestRequest jc req = do
    rid <- getRequestId
    submitRequest jc rid req
    return rid

shouldBe :: (Eq a, Show a, MonadIO m) => a -> a -> m ()
shouldBe x y = liftIO (Test.Hspec.shouldBe x y)

runWorkerAndClient :: (MonadConnect m) => (JobClient Response -> m a) -> m a
runWorkerAndClient cont =
    fmap (either absurd id) (Async.race testJobWorker (withTestJobClient cont))

timeout :: (MonadConnect m) => Int -> m a -> m (Maybe a)
timeout n m = fmap (either (const Nothing) Just) (Async.race (liftIO (threadDelay n)) m)

mapConcurrently_ :: (MonadConnect m, Traversable t) => (a -> m ()) -> t a -> m ()
mapConcurrently_ f x = void (Async.mapConcurrently f x)

-- * Tests
-----------------------------------------------------------------------

spec :: Spec
spec = do
    -- We use redisIt_ here because we want to flush the keys anyway.
    redisIt_ "Runs an enqueued computation" $ do
        let resp = Response "test"
        let req = Request
                { requestDelay = 0
                , requestResponse = DontReenqueue resp
                }
        resp' <- runWorkerAndClient (\jc -> waitForResponse_ jc =<< submitTestRequest jc req)
        resp' `shouldBe` Just resp
    redisIt_ "Doesn't yield a value when there are no workers" $ do
        let resp = Response "test"
        let req = Request
                { requestDelay = 0
                , requestResponse = DontReenqueue resp
                }
        mbResp :: Maybe (Maybe Response) <-
            withTestJobClient $ \jc -> do
                timeout (1 * 1000 * 1000) (waitForResponse_ jc =<< submitTestRequest jc req)
        mbResp `shouldBe` Nothing
    redisIt_ "Don't lose data when worker fails" $ do
        let resp = Response "test"
        let req = Request
                { requestDelay = 5 * 1000 * 1000
                , requestResponse = DontReenqueue resp
                }
        workCountRef :: IORef Int <- newIORef 0
        let onStartWork = modifyIORef workCountRef (+1)
        resp' :: Maybe Response <- fmap (either absurd id) $ Async.race
            (do maybe () absurd <$> timeout (2 * 1000 * 1000) (testJobWorkerOnStartWork onStartWork)
                testJobWorkerOnStartWork onStartWork)
            (withTestJobClient (\jc -> waitForResponse_ jc =<< submitTestRequest jc req))
        resp' `shouldBe` Just resp
        workStarted <- readIORef workCountRef
        workStarted `shouldBe` 2
    redisIt_ "Reenqueue requests" $ do
        let resp = Response "test"
        let req = Request
                { requestDelay = 0
                , requestResponse = DontReenqueue resp
                }
        workCountRef :: IORef Int <- newIORef 0
        resp' :: Maybe Response <- fmap (either absurd id) $ Async.race
            (jobWorker_ $ \req' -> do
                workCount <- readIORef workCountRef
                res <- if
                    | workCount == 0 -> return Reenqueue
                    | workCount == 1 -> processRequest req'
                    | True -> fail ("Unexpected work count " ++ show workCount)
                modifyIORef workCountRef (+1)
                return res)
            (withTestJobClient (\jc -> waitForResponse_ jc =<< submitTestRequest jc req))
        resp' `shouldBe` Just resp
        workCount <- readIORef workCountRef
        workCount `shouldBe` 2
    redisIt_ "Can cancel request" $ do
        let resp = Response "test"
        let req = Request
                { requestDelay = 120 * 1000 * 1000
                , requestResponse = DontReenqueue resp
                }
        workStartedRef :: IORef Int <- newIORef 0
        workEndedRef :: IORef Int <- newIORef 0
        fmap (either absurd id) $ Async.race
            (jobWorker_ $ \req' -> do
                atomicModifyIORef' workStartedRef (\c -> (c + 1, ()))
                res <- processRequest req'
                atomicModifyIORef' workEndedRef (\c -> (c + 1, ()))
                return res)
            -- It's important that we do the check inside the async, so that
            -- the worker does not get killed because we terminate here, which
            -- will lead to the work being stopped even if the cancel doesn't work.
            (do resp' :: Maybe Response <- withTestJobClient $ \jc -> do
                    rid <- submitTestRequest jc req
                    -- wait for the worker to actually start the job
                    waitForHUnitPass upToAMinute $ do
                        workStarted <- readIORef workStartedRef
                        workStarted `shouldBe` 1
                    -- now, cancel the request
                    cancelRequest (Seconds 50) (jcRedis jc) rid
                    waitForResponse_ jc rid
                -- we should get an empty response
                resp' `shouldBe` Nothing
                -- and the worker should have abandoned the work
                -- before reaching the second 'atomicModifyIORef'.
                workEnded <- readIORef workEndedRef
                workEnded `shouldBe` 0)
    -- Type mismatch
    -- Submitting the same req twice computes once
    -- Waiting on non-existant response gets you nothing
    stressfulTest $ redisIt_ "Fullfills all requests" $ do
        let requestsPerClient :: Int = 10
        let clients :: Int = 1000
        let workers :: Int = 100
        let client :: (MonadConnect m) => Int -> m ()
            client n = withTestJobClient $ \jc -> forM_ [1..requestsPerClient] $ \m -> do
                let resp = Response (BSC8.pack (show n ++ "-" ++ show m))
                let req = Request{requestDelay = 10, requestResponse = DontReenqueue resp}
                rid <- submitTestRequest jc req
                resp' <- waitForResponse_ jc rid
                resp' `shouldBe` Just resp
        raceAgainstVoids
            (mapConcurrently_ client [1..clients])
            (replicate workers testJobWorker)
    stressfulTest $ redisIt_ "Withstands chaos" chaosTest

chaosTest :: forall m. (MonadConnect m) => m ()
chaosTest = do
    -- We send and consume many request from many clients.
    -- In the first phase the workers and clients are killed at a rate that
    -- doesn't allow completion of requests. In the second phase they are killed
    -- at a rate that allows completion but after a bit.
    workStartedCountRef <- newIORef 0
    workCompletedCountRef <- newIORef 0
    fmap (either absurd id) $ Async.race
        (runChaosWorkers workStartedCountRef workCompletedCountRef)
        runChaosClients
    (workStartedCount, workCompletedCount) <-
        (,) <$> readIORef workStartedCountRef <*> readIORef workCompletedCountRef
    unless (workStartedCount > workCompletedCount * 3) $
        fail "Not retrying enough"
  where
    numClients :: Int
    numClients = 10

    reqsPerClient :: Int
    reqsPerClient = 10

    numWorkers :: Int
    numWorkers = 10

    reqDuration :: Int
    reqDuration = 5 * 1000 * 1000

    maxPause :: Int
    maxPause = 100

    phaseOneLifespan :: Int
    phaseOneLifespan = 1 * 1000 * 1000

    phaseTwoKillRandomly :: KillRandomly
    phaseTwoKillRandomly = KillRandomly
        { krMaxPause = 100
        , krRetries = 10000
        , krMaxTimeout = 10  * 1000
        }

    workersPhase1Len :: Int
    workersPhase1Len = 10

    clientsPhase1Len :: Int
    clientsPhase1Len = 10

    runChaosWorkers :: IORef Int -> IORef Int -> m void
    runChaosWorkers workStartedCountRef workCompletedCountRef = do
        let jw :: m void
            jw = jobWorker_ $ \req -> do
                atomicModifyIORef' workStartedCountRef (\c -> (c+1, ()))
                x <- processRequest req
                atomicModifyIORef' workCompletedCountRef (\c -> (c+1, ()))
                return x
        let runChaosWorker :: Int -> m void
            runChaosWorker _ = do
                replicateM_ workersPhase1Len $ do
                    maybe () absurd <$> timeout phaseOneLifespan jw
                    randomThreadDelay (maxPause * 1000)
                killRandomly phaseTwoKillRandomly jw
        raceAgainstVoids (runChaosWorker 1) [runChaosWorker i | i <- [2..numWorkers]]

    runChaosClients :: m ()
    runChaosClients = do
        allReqIdsVar :: TVar (HS.HashSet RequestId) <- liftIO (newTVarIO mempty)
        let runChaosClient :: Int -> m ()
            runChaosClient clientId = do
                -- First submit the requests
                reqIds :: [RequestId] <- forM [1..reqsPerClient] $ \(reqN :: Int) -> do
                    let resp = Response (BSC8.pack (show clientId ++ "-" ++ show reqN))
                    let req = Request
                            { requestDelay = reqDuration
                            , requestResponse = DontReenqueue resp
                            }
                    withTestJobClient (\jc -> submitTestRequest jc req)
                -- Then insert the reqids
                atomically (modifyTVar allReqIdsVar (HS.union (HS.fromList reqIds)))
                -- Wait for everything to be submitted
                allReqIds0 <- atomically $ do
                    allReqIds <- readTVar allReqIdsVar
                    unless (HS.size allReqIds == numClients * reqsPerClient) STM.retry
                    return (HS.toList allReqIds)
                let waitAllResponses :: m [Response]
                    waitAllResponses = withTestJobClient $ \jc -> do
                        let go :: [STM (Either DistributedException Response)] -> [RequestId] -> m [Response]
                            go wait = \case
                                [] -> mapM (either throwIO return) =<< atomically (sequence wait)
                                reqId : allReqIds -> do
                                    Just resps <- waitForResponse jc reqId (\waitReqId -> go (waitReqId : wait) allReqIds)
                                    return resps
                        go [] allReqIds0
                let phase2 = killRandomly phaseTwoKillRandomly waitAllResponses
                let phase1 :: Int -> m [Response]
                    phase1 n = if n < 0
                        then phase2
                        else do
                            mbDone <- timeout phaseOneLifespan waitAllResponses
                            case mbDone of
                                Nothing -> do
                                    randomThreadDelay (maxPause * 1000)
                                    phase1 (n-1)
                                Just _ -> fail ("Done in phase1!")
                resps <- phase1 clientsPhase1Len
                let expectedResps =
                        [ Response (BSC8.pack (show clientId_ ++ "-" ++ show reqN))
                        | clientId_ <- [1..numClients]
                        , reqN <- [1..reqsPerClient]
                        ]
                sort resps `shouldBe` sort expectedResps
        mapConcurrently_ runChaosClient [1..numClients]

