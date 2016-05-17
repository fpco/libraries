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

mkManyHasTypeFingerprint [[t|State|], [t|Input|], [t|Output|]]

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
       MasterArgs
    -> Int -- ^ Desired slaves
    -> (() -> Input -> State -> m (State, Output))
    -> (MasterHandle m State () Input Output -> m a)
    -> m a

runSimpleTest :: forall m.
     (MonadConnect m)
  => Runner m
  -> Int -- ^ Num slaves
  -> Int -- ^ Max batch size
  -> Int -- ^ Initial number of states
  -> Int -- ^ Inputs iterations
  -> m ()
runSimpleTest runner numSlaves maxBatchSize initialStates iterations = do
  let ma = MasterArgs{maMaxBatchSize = Just maxBatchSize, maMinBatchSize = Nothing}
  runner ma numSlaves f $ \mh -> do
    void (resetStates mh (map State (replicate initialStates [])))
    tokenCount :: IORef Int <- newIORef 0
    replicateM_ iterations $ do
      states0 <- getStates mh
      inputs <- for states0 $ \_ -> do
        children :: Int <- liftIO (randomRIO (0, 3))
        replicateM children $ do
          count <- readIORef tokenCount
          writeIORef tokenCount (count+1)
          return (Input count)
      testUpdate mh inputs
  where
    f :: () -> Input -> State -> m (State, Output)
    f _ input (State inputs) = do
      liftIO (threadDelay =<< randomRIO (100, 1000))
      return (State (input : inputs), Output (input : inputs))

genericSpec :: (forall m. (MonadConnect m) => Runner m) -> Spec
genericSpec runner = do
  loggingIt "Passes simple comparison with pure implementation (small)" $
    runSimpleTest runner 1 2 10 5
  loggingIt "Passes simple comparison with pure implementation (medium)" $
    runSimpleTest runner 10 5 100 5
  -- loggingIt "Passes simple comparison with pure implementation (large)" $
  --   runSimpleTest 500 5 5000 5

spec :: Spec
spec = do
  describe "Pure" (genericSpec runSimplePureStateful)
  describe "NetworkMessage" (genericSpec (runSimpleNMStateful "127.0.0.1"))

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