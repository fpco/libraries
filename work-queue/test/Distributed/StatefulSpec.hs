{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE ParallelListComp #-}
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
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE DataKinds #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
module Distributed.StatefulSpec (spec) where

import           ClassyPrelude
import           Control.Concurrent (threadDelay)
import qualified Control.Concurrent.Mesosync.Lifted.Safe as Async
import           Control.DeepSeq (NFData)
import           Control.Monad.State (modify, execState)
import qualified Data.Conduit.Network as CN
import qualified Data.HashMap.Strict as HMS
import           Data.Store (Store)
import           Data.Store.TypeHash (mkManyHasTypeHash)
import qualified Data.UUID as UUID
import qualified Data.UUID.V4 as UUID
import           Distributed.JobQueue.Client
import           Distributed.JobQueue.Worker
import           Distributed.Redis (Redis)
import           Distributed.RequestSlaves
import           Distributed.Stateful
import           Distributed.Stateful.Master
import           Distributed.Types
import           FP.Redis (MonadConnect)
import           System.Random (randomRIO)
import           Test.Hspec hiding (shouldBe, shouldSatisfy)
import qualified Test.QuickCheck as QC
import           Control.Monad.Logger.JSON.Extra (LoggingT)
import           TestUtils

newtype State = State [Input] -- All the inputs up to now
  deriving (QC.CoArbitrary, QC.Arbitrary, Show, Store, Eq, Ord, NFData)
newtype Input = Input Int
  deriving (QC.CoArbitrary, QC.Arbitrary, Show, Store, Eq, Ord, NFData)
newtype Output = Output [Input] -- All the inputs up to now
  deriving (QC.CoArbitrary, QC.Arbitrary, Show, Store, Eq, Ord, NFData)

mkManyHasTypeHash [[t|State|], [t|Input|], [t|Output|], [t|Int|]]

testUpdate ::
     (MonadConnect m, Eq key, Hashable key)
  => MasterHandle m key State () Input Output
  -> HMS.HashMap StateId [Input] -- ^ Inputs
  -> m () -- ^ Will crash if the output is not right
testUpdate mh inputs = do
  prevStates <- getStates mh
  outputs <- update mh () inputs
  forM_ (HMS.toList outputs) $ \(oldStateId, stateOutputs) ->
    case HMS.lookup oldStateId inputs of
      Nothing -> stateOutputs `shouldBe` mempty
      Just stateInputs -> do
        Just (State inputs_) <- return (HMS.lookup oldStateId prevStates)
        let expectedOutputs = [Output (input : inputs_) | input <- stateInputs]
        sort (HMS.elems stateOutputs) `shouldBe`  sort expectedOutputs

type Runner m key = forall a.
       MasterArgs m State () Input Output
    -> Int -- ^ Desired slaves
    -> (MasterHandle m key State () Input Output -> m a)
    -> m (a, Maybe Profiling)

performSimpleTest :: (MonadConnect m, Eq key, Hashable key) => Int -> MasterHandle m key State () Input Output -> m ()
performSimpleTest initialStates mh = do
  void (resetStates mh (map State (replicate initialStates [])))
  tokenCount :: IORef Int <- newIORef 0
  replicateM_ 5 $ do
    states0 <- getStates mh
    inputs <- for states0 $ \_ -> do
      children :: Int <- liftIO (randomRIO (0, 3))
      replicateM children $ do
        count <- readIORef tokenCount
        writeIORef tokenCount (count+1)
        return (Input count)
    testUpdate mh inputs

testMasterArgs :: forall m. (MonadConnect m) => Maybe (Int, Int) -> Int -> MasterArgs m State () Input Output
testMasterArgs mbDelay n = (defaultMasterArgs f) { maMaxBatchSize = Just n, maDoProfiling = DoProfiling }
  where
    f :: () -> Input -> State -> m (State, Output)
    f _ input (State inputs) = do
      liftIO $ case mbDelay of
        Nothing -> return ()
        Just x -> threadDelay =<< randomRIO x
      return (State (input : inputs), Output (input : inputs))

genericSpec :: forall key. (Eq key, Hashable key) => Runner (LoggingT IO) key -> Spec
genericSpec runner = do
  loggingIt_ "Passes simple comparison with pure implementation (no slaves)" $ do
    ((), mprof) <- runner (testMasterArgs (Just delay) 2) 0 (performSimpleTest 10)
    mprof `shouldSatisfy` \case
        Just (Profiling _ Nothing) -> True
        _ -> False
  loggingIt_ "Passes simple comparison with pure implementation (one slave)" $ do
    ((), mprof) <- runner (testMasterArgs (Just delay) 2) 1 (performSimpleTest 10)
    checkDelay mprof
  loggingIt_ "Passes simple comparison with pure implementation (10 slaves)" $ do
    ((), mprof) <- runner (testMasterArgs (Just delay) 3) 10 (performSimpleTest 100)
    checkDelay mprof
  stressfulTest $ loggingIt_ "Passes simple comparison with pure implementation (50 slaves)" $ do
    ((), mprof) <- runner (testMasterArgs (Just delay) 5) 50 (performSimpleTest 1000)
    checkDelay mprof
  where
    delay = (10, 500) :: (Int, Int)
    checkDelay msp = msp `shouldSatisfy` \case
        Nothing -> False
        Just (Profiling _ Nothing) -> False
        Just (Profiling _ (Just sp)) ->
            let updateWallTime = _pcWallTime . _spUpdate $ sp
                nUpdates = _pcCount . _spUpdate $ sp
            in updateWallTime >= fromIntegral nUpdates * (fromIntegral (fst delay) / (1000 * 1000))

spec :: Spec
spec = do
  describe "Pure" (genericSpec runSimplePureStateful)
  describe "NetworkMessage" (genericSpec (runSimpleNMStateful "127.0.0.1"))
  describe "JobQueue" $ do
    redisIt "gets all slaves available (short)" $ \r -> testSlaveConnections r 10 9
    redisIt "does not take more than maxSlaves slaves" $ \r -> testSlaveConnections r 10 5
    stressfulTest $
      redisIt_ "fullfills all requests (short, many)" (void (fullfillsAllRequests Nothing 50 3 300))
    stressfulTest $ redisIt_ "fullfills all requests (long, few)" $ do
      (numSlavesAtStartup, numSlavesAtShutdown) <- fullfillsAllRequests (Just (10, 500)) 10 10 30
      let increased = flip execState (0 :: Int) $
            forM_ (HMS.toList numSlavesAtStartup) $ \(reqId, startup) -> do
              let shutdown = numSlavesAtShutdown HMS.! reqId
              when (shutdown > startup) (modify (+1))
      unless (increased > 10) $
        fail "Didn't get many increases in slaves!"


testSlaveConnections :: MonadConnect m => Redis -> Int -> Int -> m ()
testSlaveConnections r workersToSpawn slavesToAccept = do
  let jqc = testJobQueueConfig
  let ss = CN.serverSettings 0 "*"
  let nmsma = NMStatefulMasterArgs
        { nmsmaMinimumSlaves = Nothing
        , nmsmaMaximumSlaves = Just slavesToAccept
        , nmsmaSlavesWaitingTime = 30 * 1000 * 1000
        }
      worker :: forall void m. (MonadConnect m) => m void
      worker =
        runJobQueueStatefulWorker jqc ss "127.0.0.1" Nothing Nothing (testMasterArgs Nothing 5) nmsma $
          \mh _reqId () -> do
            nSlaves <- waitForHUnitPass upToAMinute $ do
              n <- getNumSlaves mh
              n `shouldBe` slavesToAccept
              return n
            return $ DontReenqueue nSlaves
      client :: forall m. (MonadConnect m) => m ()
      client = withJobClient jqc $ \(jc :: JobClient Int) -> do
        rid <- liftIO (RequestId . UUID.toASCIIBytes <$> UUID.nextRandom)
        submitRequest jc rid ()
        void $ waitForResponse_ jc rid
  raceAgainstVoids
    (do
      client
      -- Check that there are no masters anymore
      waitForHUnitPass upToAMinute $ do
        wcis <- getWorkerRequests r
        wcis `shouldBe` [])
    (replicate workersToSpawn worker)

fullfillsAllRequests :: (MonadConnect m) => Maybe (Int, Int) -> Int -> Int -> Int -> m (HMS.HashMap RequestId Int, HMS.HashMap RequestId Int)
fullfillsAllRequests mbDelay numClients requestsPerClient numWorkers = do
  let jqc = testJobQueueConfig
  let ss = CN.serverSettings 0 "*"
  let nmsma = NMStatefulMasterArgs
        { nmsmaMinimumSlaves = Nothing
        , nmsmaMaximumSlaves = Just 7
        , nmsmaSlavesWaitingTime = 1000 * 1000
        }
  numSlavesAtStartupRef :: IORef (HMS.HashMap RequestId Int) <- newIORef mempty
  numSlavesAtShutdownRef :: IORef (HMS.HashMap RequestId Int) <- newIORef mempty
  let worker :: forall void m. (MonadConnect m) => m void
      worker =
        runJobQueueStatefulWorker jqc ss "127.0.0.1" Nothing Nothing (testMasterArgs mbDelay 5) nmsma $
          \mh reqId () -> do
            numSlaves <- getNumSlaves mh
            atomicModifyIORef' numSlavesAtStartupRef (\sl -> (HMS.insert reqId numSlaves sl, ()))
            performSimpleTest 10 mh
            numSlaves' <- getNumSlaves mh
            atomicModifyIORef' numSlavesAtShutdownRef (\sl -> (HMS.insert reqId numSlaves' sl, ()))
            return (DontReenqueue ())
      requestLoop :: (MonadConnect m) => Int -> m ()
      requestLoop _n = withJobClient jqc $ \(jc :: JobClient ((), Maybe Profiling)) -> forM_ [1..requestsPerClient] $ \(_m :: Int) -> do
        rid <- liftIO (RequestId . UUID.toASCIIBytes <$> UUID.nextRandom)
        submitRequest jc rid ()
        Just ((), _) <- waitForResponse_ jc rid
        return ()
  raceAgainstVoids
    (void (Async.mapConcurrently requestLoop [(1::Int)..numClients]))
    (replicate numWorkers worker)
  -- Check that we sometimes gained slaves while executing
  numSlavesAtStartup <- readIORef numSlavesAtStartupRef
  numSlavesAtShutdown <- readIORef numSlavesAtShutdownRef
  return (numSlavesAtStartup, numSlavesAtShutdown)
