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
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE RankNTypes #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
module Distributed.StatefulSpec (spec) where

import ClassyPrelude
import Data.Serialize (Serialize)
import Test.Hspec hiding (shouldBe)
import qualified Test.Hspec
import FP.Redis (MonadConnect)
import Control.Concurrent (threadDelay)
import qualified Test.QuickCheck as QC
import Control.DeepSeq (NFData)
import qualified Data.HashMap.Strict as HMS
import System.Random (randomRIO)
import Data.TypeFingerprint (mkManyHasTypeFingerprint)
import Distributed.JobQueue.Worker
import Distributed.JobQueue.Client
import qualified Data.Conduit.Network as CN
import qualified Control.Concurrent.Async.Lifted.Safe as Async
import Data.Void (absurd)
import qualified Data.UUID as UUID
import qualified Data.UUID.V4 as UUID
import Distributed.Types
import qualified Data.List.NonEmpty as NE
import Control.Monad.State (modify, execState)

import TestUtils
import Distributed.Stateful
import Distributed.Stateful.Master

shouldBe :: (Eq a, Show a, MonadIO m) => a -> a -> m ()
shouldBe x y = liftIO (Test.Hspec.shouldBe x y)

newtype State = State [Input] -- All the inputs up to now
  deriving (QC.CoArbitrary, QC.Arbitrary, Show, Serialize, Eq, Ord, NFData)
newtype Input = Input Int
  deriving (QC.CoArbitrary, QC.Arbitrary, Show, Serialize, Eq, Ord, NFData)
newtype Output = Output [Input] -- All the inputs up to now
  deriving (QC.CoArbitrary, QC.Arbitrary, Show, Serialize, Eq, Ord, NFData)

mkManyHasTypeFingerprint [[t|State|], [t|Input|], [t|Output|], [t|Int|]]

testUpdate ::
     (MonadConnect m)
  => MasterHandle m State () Input Output
  -> HMS.HashMap StateId [Input] -- ^ Inputs
  -> m () -- ^ Will crash if the output is not right
testUpdate mh inputs = do
  prevStates <- getStates mh
  outputs <- update mh () inputs
  forM_ (HMS.toList outputs) $ \(oldStateId, stateOutputs) -> do
    case HMS.lookup oldStateId inputs of
      Nothing -> stateOutputs `shouldBe` mempty
      Just stateInputs -> do
        Just (State inputs_) <- return (HMS.lookup oldStateId prevStates)
        let expectedOutputs = [Output (input : inputs_) | input <- stateInputs]
        sort (HMS.elems stateOutputs) `shouldBe`  sort expectedOutputs

type Runner m = forall a.
       MasterArgs m State () Input Output
    -> Int -- ^ Desired slaves
    -> (MasterHandle m State () Input Output -> m a)
    -> m a

performSimpleTest :: (MonadConnect m) => Int -> MasterHandle m State () Input Output -> m ()
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

testMasterArgs :: forall m. (MonadConnect m) => Int -> MasterArgs m State () Input Output
testMasterArgs n = MasterArgs{maMaxBatchSize = Just n, maMinBatchSize = Nothing, maUpdate = f}
  where
    f :: () -> Input -> State -> m (State, Output)
    f _ input (State inputs) = do
      liftIO (threadDelay =<< randomRIO (100, 500))
      return (State (input : inputs), Output (input : inputs))

genericSpec :: (forall m. (MonadConnect m) => Runner m) -> Spec
genericSpec runner = do
  loggingIt "Passes simple comparison with pure implementation (no slaves)" $
    runner (testMasterArgs 2) 0 (performSimpleTest 10)
  loggingIt "Passes simple comparison with pure implementation (one slave)" $
    runner (testMasterArgs 2) 1 (performSimpleTest 10)
  loggingIt "Passes simple comparison with pure implementation (10 slaves)" $
    runner (testMasterArgs 3) 10 (performSimpleTest 100)
  loggingIt "Passes simple comparison with pure implementation (50 slaves)" $
    runner (testMasterArgs 5) 50 (performSimpleTest 1000)
  -- loggingIt "Passes simple comparison with pure implementation (large)" $
  --   runSimpleTest 500 5 5000 5

spec :: Spec
spec = do
  describe "Pure" (genericSpec runSimplePureStateful)
  describe "NetworkMessage" (genericSpec (runSimpleNMStateful "127.0.0.1"))
  describe "JobQueue" $ do
    redisIt_ "gets all slaves available" $ do
      let jqc = testJobQueueConfig
      let ss = CN.serverSettings 0 "*"
      let nmsma = NMStatefulMasterArgs
            { nmsmaMinimumSlaves = Nothing
            , nmsmaMaximumSlaves = Nothing
            , nmsmaSlavesWaitingTime = 1000 * 1000
            }
      let worker :: forall void m. (MonadConnect m) => m void
          worker =
            runJobQueueStatefulWorker jqc ss "127.0.0.1" Nothing (testMasterArgs 5) nmsma $
              \mh _reqId () -> do
                liftIO (threadDelay (1000 * 1000))
                DontReenqueue <$> getNumSlaves mh
          workersToSpawn = 10
          client :: forall m. (MonadConnect m) => m ()
          client = withJobClient jqc $ \(jc :: JobClient Int) -> do
            rid <- liftIO (RequestId . UUID.toASCIIBytes <$> UUID.nextRandom)
            submitRequest jc rid ()
            Just slaves <- waitForResponse_ jc rid
            unless (slaves == workersToSpawn - 1) $ do
              fail ("Expecting " ++ show (workersToSpawn - 1) ++ ", but got " ++ show slaves)
      fmap (either absurd id) $ Async.race
        (NE.head <$> Async.mapConcurrently (\_ -> worker) (NE.fromList [(1::Int)..workersToSpawn]))
        client
    redisIt_ "fullfills all requests" $ do
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
            runJobQueueStatefulWorker jqc ss "127.0.0.1" Nothing (testMasterArgs 5) nmsma $
              \mh reqId () -> do
                numSlaves <- getNumSlaves mh
                atomicModifyIORef' numSlavesAtStartupRef (\sl -> (HMS.insert reqId numSlaves sl, ()))
                performSimpleTest 100 mh
                numSlaves' <- getNumSlaves mh
                atomicModifyIORef' numSlavesAtShutdownRef (\sl -> (HMS.insert reqId numSlaves' sl, ()))
                return (DontReenqueue ())
          requestLoop :: (MonadConnect m) => m ()
          requestLoop = withJobClient jqc $ \(jc :: JobClient ()) -> replicateM_ 10 $ do
            rid <- liftIO (RequestId . UUID.toASCIIBytes <$> UUID.nextRandom)
            submitRequest jc rid ()
            Just () <- waitForResponse_ jc rid
            return ()
      fmap (either absurd id) $ Async.race
        (NE.head <$> Async.mapConcurrently (\_ -> worker) (NE.fromList [(1::Int)..30]))
        (void (Async.mapConcurrently (\_ -> requestLoop) [(1::Int)..10]))
      -- Check that we sometimes gained slaves while executing
      numSlavesAtStartup <- readIORef numSlavesAtStartupRef
      numSlavesAtShutdown <- readIORef numSlavesAtShutdownRef
      let increased = flip execState (0 :: Int) $
            forM_ (HMS.toList numSlavesAtStartup) $ \(reqId, startup) -> do
              let shutdown = numSlavesAtShutdown HMS.! reqId
              when (shutdown > startup) (modify (+1))
      unless (increased > 10) $
        fail "Didn't get many increases in slaves!"

{-
  it "Passes quickcheck comparison with the pure implementation" $
    QC.property $ QC.forAll QC.arbitrary $
    \( QC.Blind (function :: Context -> Input -> State -> (State, Output))
     , initialStates :: [State]
     , updates :: [(Context, [[Input]])]
     , numSlaves :: Int
     ) -> (QC.==>) (numSlaves > 2) $ loggingProperty $ do
      runSimplePure (MasterArgs (Just 5) Nothing) numSlaves (\a b c -> return (function a b c)) $ \mh -> do
        let go :: PureState State -> (Context, [[Input]]) -> LoggingT IO (PureState State)
            go ps (ctx, inputs) = do
              let sids' = sort (HMS.keys (pureStates ps))
              let inputMap = HMS.fromList (zip sids' (inputs ++ repeat []))
              let (ps', outputs') = pureUpdate function ctx inputMap ps
              -- putStrLn "===="
              -- print ctx
              -- print ("inputs", inputMap)
              -- print ("outputs", outputs')
              -- print ("before", ps)
              -- print ("after", ps')
              sids <- getStateIds mh
              sort (HS.toList sids) `shouldBe` sids'
              curStates <- getStates mh
              curStates `shouldBe` pureStates ps
              outputs <- update mh ctx inputMap
              -- print outputs
              -- print outputs'
              outputs `shouldBe` outputs'
              return ps'
        void $ foldM go (initialPureState initialStates) (take 4 updates)
        return True

newtype Context = Context Int deriving (QC.CoArbitrary, QC.Arbitrary, Show, Serialize, Eq)
newtype Input = Input Int deriving (QC.CoArbitrary, QC.Arbitrary, Show, Serialize, Eq)
newtype State = State Int deriving (QC.CoArbitrary, QC.Arbitrary, Show, Serialize, Eq, NFData)
newtype Output = Output Int deriving (QC.CoArbitrary, QC.Arbitrary, Show, Serialize, Eq, NFData)

data PureState state = PureState
    { pureStates :: HMS.HashMap StateId state
    , pureIdCounter :: Int
    } deriving (Show)

initialPureState :: [state] -> PureState state
initialPureState states = PureState
    { pureStates = HMS.fromList $ zip (map StateId [0..]) states
    , pureIdCounter = length states
    }

pureUpdate :: forall state context input output.
              (context -> input -> state -> (state, output))
           -> context
           -> HMS.HashMap StateId [input]
           -> PureState state
           -> (PureState state, HMS.HashMap StateId (HMS.HashMap StateId output))
pureUpdate f context inputs ps = (ps', outputs)
  where
    sortedInputs :: [(StateId, [input])]
    sortedInputs = sortBy (comparing fst) (HMS.toList inputs)
    labeledInputs :: [(StateId, StateId, input)]
    labeledInputs =
      zipWith (\sid' (sid, inp) -> (sid, StateId sid', inp))
              [pureIdCounter ps ..]
              (concatMap (\(sid, inps) -> map (sid, ) inps) sortedInputs)
    ps' = PureState
      { pureStates = HMS.fromList (map (\(_, sid, (state, _)) -> (sid, state)) results)
      , pureIdCounter = pureIdCounter ps + length labeledInputs
      }
    results :: [(StateId, StateId, (state, output))]
    results = map (\(sid, sid', input) -> (sid, sid', f context input (pureStates ps HMS.! sid))) labeledInputs
    outputs :: HMS.HashMap StateId (HMS.HashMap StateId output)
    outputs = HMS.fromListWith (<>) (map (\(sid, sid', (_, output)) -> (sid, HMS.singleton sid' output)) results)
-}