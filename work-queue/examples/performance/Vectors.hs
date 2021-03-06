{-# OPTIONS_GHC -fno-warn-orphans #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE RankNTypes #-}
module Vectors ( Options (..)
               , masterArgs
               , myAction, myStates
               , csvInfo
               , jqc
               ) where

import           ClassyPrelude
import           Control.DeepSeq
import           Control.Exception (evaluate)
import           Control.Monad.Random
import qualified Data.HashMap.Strict as HMS
import qualified Data.HashSet as HS
import           Data.Store.TypeHash (mkManyHasTypeHash)
import qualified Data.Vector as V
import           Distributed.JobQueue.Client
import           Distributed.Stateful
import           Distributed.Stateful.Master
import           FP.Redis (MonadConnect)
import           TypeHash.Orphans ()

type Request = [State]
type Response = Double
type State = ( V.Vector Double
             , V.Vector Double
             )
type Input = V.Vector Double
type Context = Double
type Output = Double

$(mkManyHasTypeHash [ [t| Request |]
                    ])

-- | Perform some computations.  The only aim is to take about a
-- fraction of a millisecond, which is similar to an update in the
-- distributed particle filter.
myUpdate :: MonadConnect m => Options -> Update m State Context Input Output
myUpdate Options{..} context input (!v, !v') = do
    foo <- forM [1..100::Double] $ \i -> do
        let prod = V.zipWith (\x y -> x*y*i) v v'
            result = V.sum prod
            v1' = V.map (*(i*V.sum input)) v
            v2' = V.map (*(context*i)) $ V.zipWith (/) v1' v'
        state' <- liftIO . evaluate $ result `deepseq` v1' `deepseq` v2' `deepseq` (v1', v2')
        return (state', result)
    return $! foldl' (\((v1,v2),x) ((v1',v2'),x') -> ((V.zipWith (+) v1 v1',V.zipWith (+) v2 v2'),x+x')) ((V.replicate optVLength 0, V.replicate optVLength 0),0) foo


masterArgs :: MonadConnect m => Options -> MasterArgs m State Context Input Output
masterArgs opts = (defaultMasterArgs (myUpdate opts)) {maDoProfiling = DoProfiling}

-- | Random states (with fixed random generator)
myStates :: Options -> [State]
myStates Options{..} =
    let r = mkStdGen 42
    in concat $ mapM (const (evalRandT go r)) [1..optNStates :: Int]
  where
    go :: RandT StdGen [] State
    go = do
        v1 <- V.generateM optVLength $ \_ -> getRandomR (0,1)
        v2 <- V.generateM optVLength $ \_ -> getRandomR (0,1)
        v1 `deepseq` v2 `deepseq` return (v1, v2)

-- | We'll use the same input over and over again.
myInputs :: [Input]
myInputs = [V.enumFromN 1 20]

myAction :: (MonadConnect m, Eq key, Hashable key) => Request -> MasterHandle m key State Context Input Output -> m Response
myAction req mh = do
    _ <- resetStates mh req
    finalStates <- mapM (\_ -> do
                                stateIds <- getStateIds mh
                                let inputs = HMS.fromList $ zip (HS.toList stateIds) (repeat myInputs)
                                newStates <- update mh 5 inputs
                                return newStates
                        ) [1..5::Int]
    return (sum $ sum <$> (unsafeHead finalStates :: HashMap StateId (HashMap StateId Output)))

jqc :: JobQueueConfig
jqc = defaultJobQueueConfig "perf:unmotivated:"

data Options = Options
               { optVLength :: Int
               , optNStates :: Int
               , optOutput :: FilePath
               }

csvInfo :: Options -> ProfilingColumns
csvInfo opts =
    [ ("l", pack . show . optVLength $ opts)
    , ("N", pack . show . optNStates $ opts)]
