{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Main where

import           ClassyPrelude
import           Control.DeepSeq (NFData)
import           Data.Random.RVar
import           Data.Store
import qualified Data.Vector as V
import qualified Distributed.Stateful.Master as WQ
import           FP.Redis (MonadConnect)
import qualified Data.HashMap.Strict as HMS
import           Data.Random
import           Data.Random.Source
-- * type definitions

data Particle parameter state summary = Particle
    { pparameter :: parameter
      -- ^ the parameter that need to be estimated by the particle filter
    , pstate :: state
      -- ^ the state that is evolved
    , pestimate :: summary
      -- ^ the observable that is predicted and compared against the measurement
    , pweight :: Double
    } deriving (Eq,Show,Read,Generic,NFData,Store)

-- | A particle cloud
type Particles parameter state summary = (V.Vector (Particle parameter state summary))

-- | Function that calculates the observable
type Estimate parameter state summary = parameter -> state -> summary
type EvolveParticle parameter state summary input =
    Particle parameter state summary
    -> Maybe input
    -> Maybe summary
    -> (summary -> summary -> Double)
    -> Estimate parameter state summary
    -> Particle parameter state summary

evolveParticle :: (parameter -> state -> input -> state) -> EvolveParticle parameter state summary input
evolveParticle f Particle{..} minput mmeasurement weigh estimator =
    let state' = case minput of
            Just input -> f pparameter pstate input
            Nothing -> pstate
        estimate' = estimator pparameter state'
        weight' = case mmeasurement of
            Just measurement -> weigh measurement estimate'
            Nothing -> pweight
    in Particle
       { pparameter = pparameter
       , pstate = state'
       , pestimate = estimate'
       , pweight = weight'
       }

data EvolvingSystem input summary =
    EvolvingSystem { evolveStep :: Maybe (EvolvingSystem input summary, (Maybe input, Maybe summary)) }

newtype PFState parameter state summary =
    PFState {pfsParticle :: Particle parameter state summary}
    deriving (Eq,Show,Generic,Store,NFData)

data PFConfig  parameter state summary input = PFConfig
    { pfcSystem :: EvolvingSystem input summary
    -- ^ system that we want to model
    , pfcEstimate :: Estimate parameter state summary
    -- ^ estimator function
    , pfcEvolve :: EvolveParticle parameter state summary input
    , pfcRegulator :: forall m. Monad m => Particles parameter state summary -> state -> (RVarT m) state
    , pfcWeigh :: summary -> summary -> Double
    , pfcEffectiveParticleThreshold :: Double
    -- ^ When the ratio of @effective particles/particles@ falls below this threshold, we'll perform resampling
    , pfcSampleParameter :: Foldable t => t (Double, parameter) -> parameter
    }

-- | Context that is the same for all particles in a simulation
data PFContext parameter state summary input = PFContext
    { pfcInput :: Maybe input
    , pfcMeasurement :: Maybe summary
    }

data PFInput = PFInput
    deriving (Eq,Show,Generic,Store,NFData)

data PFOutput=
    PFOutput { pfoWeight :: Double }
    deriving (Eq,Show,Generic,Store,NFData)

data PFRequest parameter state summary input = PFRequest
    { rparticles :: Particles parameter state summary
      -- ^ initial ensemble of particles
    }

newtype PFResponse parameter = PFResponse parameter


dpfMaster :: forall m parameter state summary input .
             (MonadConnect m, MonadRandom m
             , NFData parameter, NFData state, NFData summary)
             => PFConfig parameter state summary input
             -> WQ.MasterHandle m (PFState parameter state summary)
                                  (PFContext parameter state summary input)
                                  PFInput
                                  PFOutput
             -> PFRequest parameter state summary input
             -> m (PFResponse parameter)
dpfMaster PFConfig{..} mh PFRequest{..} = do
    sids <- WQ.resetStates mh $ PFState <$> V.toList rparticles
    let initialWeights = (const (PFOutput $ 1/fromIntegral nParticles) <$> sids :: HMS.HashMap WQ.StateId PFOutput)
    evolve initialWeights pfcSystem
  where
      evolve :: HMS.HashMap WQ.StateId PFOutput -> EvolvingSystem input summary -> m (PFResponse parameter)
      evolve weights system = case evolveStep system of
          Nothing -> sendResponse
          Just (system', (minput, msummary)) -> do
              weights' <- fold <$> WQ.update mh (PFContext minput msummary) (const [PFInput] <$> weights)
              resample weights' system'
      resample weights system = do
          let nEffParticles = (sum $ pfoWeight <$> weights) / fromIntegral nParticles
          if nEffParticles > pfcEffectiveParticleThreshold
              then evolve weights system
              else do
                  let weightedStates = V.fromList . HMS.elems $ HMS.mapWithKey (\sid (PFOutput w) -> (w, sid)) weights
                      stateRVar = weightedVectorRVar weightedStates
                  -- draw from weighted states
                  sids <- V.generateM nParticles (\_i -> sample stateRVar) :: m (V.Vector WQ.StateId)
                  states <- WQ.getStates mh :: m (HMS.HashMap WQ.StateId (PFState parameter state summary))
                  let newParticles =
                          pfsParticle
                          . fromMaybe (error "lookup in states failed")
                          . (flip HMS.lookup states) <$> sids :: Particles parameter state summary
                      regulator = pfcRegulator newParticles :: state -> RVar state
                  regulatedParticles <- V.mapM (\p@Particle{..} -> do
                                                       state' <- sample $ regulator pstate
                                                       return p { pstate = state' }
                                               ) newParticles
                  sids' <- WQ.resetStates mh $ PFState <$> V.toList regulatedParticles
                  let weights' = PFOutput . pweight . pfsParticle <$> sids'
                  evolve weights' system
      sendResponse = do
          states <- WQ.getStates mh
          let parameterEstimate = pfcSampleParameter $ (\(PFState p) -> (pweight p, pparameter p)) <$> states
          return $ PFResponse parameterEstimate
      nParticles = V.length rparticles

weightedVectorRVar :: V.Vector (Double, a) -> RVar a
weightedVectorRVar vec = do
    let drawVector = V.postscanl' (\(summedWeight, _ignore) (w, sid) -> (summedWeight + w, sid)) (0,undefined) vec
        normalisation = fst . V.last $ drawVector
    r <- uniform 0 normalisation :: RVar Double
    case V.dropWhile ((<r) . fst) drawVector of
        v | V.null v -> error "Could not draw"
        v -> return . snd . V.head $ v


dpfSlave :: forall parameter state summary input .
            PFConfig parameter state summary input
            -> PFContext parameter state summary input
            -> PFInput
            -> PFState parameter state summary
            -> (PFState parameter state summary, PFOutput)
dpfSlave PFConfig{..} PFContext{..} PFInput (PFState p) =
    let p' = pfcEvolve p pfcInput pfcMeasurement pfcWeigh pfcEstimate
    in (PFState p', PFOutput (pweight p'))







evolvingSystemFromList :: [(Maybe input, Maybe summary)] -> EvolvingSystem input summary
evolvingSystemFromList [] = EvolvingSystem Nothing
evolvingSystemFromList (x:xs) = EvolvingSystem (Just (evolvingSystemFromList xs, x))
