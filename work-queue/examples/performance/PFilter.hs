{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE BangPatterns #-}
module PFilter
       ( Options (..)
       , pfConfig
       , dpfSlave
       , dpfMaster
       , generateRequest
       , jqc
       , csvInfo
       ) where

import           ClassyPrelude
import           Control.DeepSeq (NFData)
import           Control.Monad.Logger
import qualified Data.HashMap.Strict as HMS
import qualified Data.Map.Strict as M
import           Data.Number.Erf
import           Data.Random
import           Data.Store
import           Data.Store.TypeHash (mkManyHasTypeHash)
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as UV
import           Distributed.JobQueue.Client
import           Distributed.Stateful.Internal.Profiling
import           Distributed.Stateful.Master
import qualified Distributed.Stateful.Master as WQ
import           FP.Redis (MonadConnect)
import           PerformanceUtils
-- * type definitions

data Particle parameter state summary = Particle
    { pparameter :: !parameter
      -- ^ the parameter that need to be estimated by the particle filter
    , pstate :: !state
      -- ^ the state that is evolved
    , pestimate :: !summary
      -- ^ the observable that is predicted and compared against the measurement
    , pweight :: !Double
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

data PFConfig parameter state summary input = PFConfig
    { pfcSystem :: EvolvingSystem input summary
    -- ^ system that we want to model
    , pfcEstimate :: Estimate parameter state summary
    -- ^ estimator function
    , pfcEvolve :: EvolveParticle parameter state summary input
    , pfcRegulator :: forall m. Monad m => Particles parameter state summary -> parameter -> (RVarT m) parameter
    , pfcWeigh :: summary -> summary -> Double
    , pfcEffectiveParticleThreshold :: Double
    -- ^ When the ratio of @effective particles/particles@ falls below this threshold, we'll perform resampling
    , pfcSampleParameter :: HMS.HashMap WQ.StateId (Double, parameter) -> parameter
    }

-- | Context that is the same for all particles in a simulation
data PFContext parameter state summary input = PFContext
    { pfcInput :: !(Maybe input)
    , pfcMeasurement :: !(Maybe summary)
    } deriving (Eq,Show,Generic,Store,NFData)

data PFInput = PFInput
    deriving (Eq,Show,Generic,Store,NFData)

data PFOutput=
    PFOutput { pfoWeight :: !Double }
    deriving (Eq,Show,Generic,Store,NFData)

data PFRequest parameter state summary input = PFRequest
    { rparticles :: !(Particles parameter state summary)
      -- ^ initial ensemble of particles
    } deriving (Eq,Show,Generic,Store,NFData)

newtype PFResponse parameter = PFResponse parameter
                             deriving (Eq,Show,Generic,Store,NFData)

dpfMaster :: forall m s parameter state summary input .
             (MonadConnect m, RandomSource m s
             , NFData parameter, NFData state, NFData summary, NFData input
             , Store parameter, Store state, Store summary, Store input)
             => PFConfig parameter state summary input
             -> s
             -> PFRequest parameter state summary input
             -> WQ.MasterHandle m (PFState parameter state summary)
                                  (PFContext parameter state summary input)
                                  PFInput
                                  PFOutput
             -> m (PFResponse parameter, SlaveProfiling, MasterProfiling)
dpfMaster PFConfig{..} s PFRequest{..} mh = do
    sids <- WQ.resetStates mh $ PFState <$> V.toList rparticles
    let initialWeights = const (PFOutput $ 1/fromIntegral nParticles) <$> sids :: HMS.HashMap WQ.StateId PFOutput
    evolve initialWeights pfcSystem
  where
      evolve weights system = case evolveStep system of
          Nothing -> sendResponse
          Just (system', (minput, msummary)) -> do
              weights' <- fold <$> WQ.update mh (PFContext minput msummary) (const [PFInput] <$> weights)
              resample weights' system'
      resample weights system = getMasterProfilingIORef mh >>= \mp -> do
          let nEffParticles = sum (pfoWeight <$> weights) / fromIntegral nParticles
          if nEffParticles > pfcEffectiveParticleThreshold
              then $logInfoS logSourceBench (unwords ["not resampling,", tshow nEffParticles, ">", tshow pfcEffectiveParticleThreshold])
                   >> evolve weights system
              else do
                  $logInfoS logSourceBench (unwords ["resampling,", tshow nEffParticles, "<=", tshow pfcEffectiveParticleThreshold] )
                  let weightedStates = V.fromList . HMS.elems $ HMS.mapWithKey (\sid (PFOutput w) -> (w, sid)) weights
                      stateRVar = weightedVectorRVar weightedStates
                  -- draw from weighted states
                  sids <- V.generateM nParticles (\_i -> sampleFrom s stateRVar) :: m (V.Vector WQ.StateId)
                  states <- WQ.getStates mh :: m (HMS.HashMap WQ.StateId (PFState parameter state summary))
                  let newParticles =
                          pfsParticle
                          . fromMaybe (error "lookup in states failed")
                          . flip HMS.lookup states <$> sids :: Particles parameter state summary
                      regulator = pfcRegulator newParticles :: parameter -> RVar parameter
                  regulatedParticles <- V.mapM (\p@Particle{..} -> do
                                                      parameter' <- sampleFrom s $ regulator pparameter
                                                      return p { pparameter = parameter'
                                                               , pweight = 1 / fromIntegral nParticles
                                                               }
                                               ) newParticles
                  sids' <- WQ.resetStates mh $ PFState <$> V.toList regulatedParticles
                  let weights' = PFOutput . pweight . pfsParticle <$> sids'
                  evolve weights' system
      sendResponse = do
          states <- WQ.getStates mh
          let parameterEstimate = pfcSampleParameter $ (\(PFState p) -> (pweight p, pparameter p)) <$> states
          sp <- HMS.elems <$> getSlavesProfiling mh >>= \case
              [] -> $logErrorS logSourceBench "No slave profiling data!" >> return emptySlaveProfiling
              sp:sps -> return $ foldl' (<>) sp sps
          $logInfoS logSourceBench ("slave profiling raw data: " ++ tshow sp)
          mp <- getMasterProfiling mh
          $logInfoS logSourceBench ("master profiling raw data: " ++ tshow mp)
          return (PFResponse parameterEstimate, sp, mp)
      nParticles = V.length rparticles

weightedVectorRVar :: Show a => V.Vector (Double, a) -> RVar a
weightedVectorRVar vec = do
    let drawVector = V.postscanl'
            (\(summedWeight, _ignore) (w, sid) -> (summedWeight + w, sid))
            (0,error "this should get dropped in the postscan")
            vec
        normalisation = fst . V.last $ drawVector
        drawMap = M.fromAscList . V.toList $ drawVector
    r <- uniform 0 normalisation :: RVar Double
    case M.lookupGT r drawMap of
        Nothing -> error $ unwords ["Could not draw", show r, "from", show drawVector]
        Just (_, x) -> return x

dpfSlave :: forall m parameter state summary input. MonadConnect m
            => PFConfig parameter state summary input
            -> Update m (PFState parameter state summary)
                      (PFContext parameter state summary input)
                      PFInput PFOutput
dpfSlave PFConfig{..} PFContext{..} PFInput (PFState p) =
    let p' = pfcEvolve p pfcInput pfcMeasurement pfcWeigh pfcEstimate
    in return (PFState p', PFOutput (pweight p'))

evolvingSystemFromList :: [(Maybe input, Maybe summary)] -> EvolvingSystem input summary
evolvingSystemFromList [] = EvolvingSystem Nothing
evolvingSystemFromList (x:xs) = EvolvingSystem (Just (evolvingSystemFromList xs, x))

-- | Command line options
data Options = Options
               { optStepsize :: Double
               , optDeltaT :: Double
               , optSteps :: Int
               , optOmega2 :: Double
               , optOmega2Range :: Double -- ^ Size of the interval in which we distribute the particles
               , optPhi0 :: Double
               , optResampleThreshold :: Double
               , optNParticles :: Int
               , optOutput :: FilePath
               }

-- | For evolving the dynamical system,
-- we use a simple fourth order Runge-Kutta, with a fixed time step.
integrate :: (Double -> UV.Vector Double -> UV.Vector Double)
          -- ^ Function @f@ on the RHS of the differential
          -- equations, as in @dy/dx = f x y@
          -> Double
          -- ^ stepzise
          -> Double
          -- ^ integration interval
          -> Double
          -- ^ starting @x@
          -> UV.Vector Double
          -- ^ starting @ys@
          -> UV.Vector Double
integrate f stepsize deltaX x ys = go deltaX
  where go remainingInterval | remainingInterval < stepsize = step remainingInterval
        go remainingInterval = let !ys' = step stepsize
                    in integrate f stepsize (remainingInterval - stepsize) (x + stepsize) ys'
        step h =
            let k1 = (*(h/6)) `UV.map` f x ys
                k2 = (*(h/3)) `UV.map` f (x + (h/2)) (UV.zipWith (\y k -> y + k/2) ys k1)
                k3 = (*(h/3)) `UV.map` f (x + (h/2)) (UV.zipWith (\y k -> y + k/2) ys k2)
                k4 = (*(h/6)) `UV.map` f (x + h) (UV.zipWith (+) ys k3)
            in UV.zipWith5 (\a b c d e -> a + b + c + d + e) ys k1 k2 k3 k4

pendulum :: Double -> Double -> UV.Vector Double -> UV.Vector Double
pendulum omega2 _ ys | UV.length ys == 2 = UV.fromList [ys UV.! 1, - omega2 * sin (ys UV.! 0)]
pendulum _ _ ys = error $ "wrong lenght of ys in pendulum " ++ show (UV.length ys)

data MyParameter = MyParameter
                   { omega2 :: Double
                   }
             deriving (Generic, NFData, Show)
instance Store MyParameter
data MyState = MyState { t :: !Double -- ^ time
                       , ys :: !(UV.Vector Double) -- ^ [phi, phi']
                       }
             deriving (Generic, NFData, Show)
instance Store MyState
type MySummary = Double -- ^ phi
type MyInput = Double -- ^ timestep

pfConfig :: Options -> PFConfig MyParameter MyState MySummary MyInput
pfConfig opts@Options{..} =
    let pfcSystem = mySystem opts
        pfcEstimate _parm MyState{..} = UV.head ys
        pfcEvolve = evolveParticle (\MyParameter{..} MyState{..} dt ->
                                       (MyState (t+dt) (integrate (pendulum omega2) optStepsize dt t ys)))
        pfcRegulator _ MyParameter{..} = do
            noise <- normalT 0 0.01 -- this is rather ad hoc
            return MyParameter { omega2 = omega2 + noise }
        pfcWeigh phi phi' = 1 - abs (erf (180/pi*(phi-phi')))
        pfcEffectiveParticleThreshold = optResampleThreshold
        pfcSampleParameter xs = let (normalisation, omega') = HMS.foldl'
                                        (\(weightSum, omegaSum) (weight, MyParameter{..}) -> (weightSum + weight, omegaSum + weight * omega2)) (0,0) xs
                                in MyParameter { omega2 = omega' / normalisation }
    in PFConfig{..}

mySystem :: Options -> EvolvingSystem MyInput MySummary
mySystem Options{..} =
    let vec = V.unfoldr (\(n,ys) -> if n > 0
                         then let ys' = integrate (pendulum optOmega2) optStepsize optDeltaT 0 ys
                              in Just ((optDeltaT, UV.head ys'), (n-1, ys'))
                         else Nothing)
              (optSteps, UV.fromList [optPhi0, 0])
    in evolvingSystemFromList . V.toList . V.map (Just *** Just) $ vec

generateRequest :: (MonadConnect m, RandomSource m s) => Options -> s -> m (PFRequest MyParameter MyState MySummary MyInput)
generateRequest Options{..} s = do
    let dist = uniform (min 0 (optOmega2 - (optOmega2Range/2))) (optOmega2 + (optOmega2Range/2))
    parameters <- V.replicateM optNParticles (sampleFrom s dist)
    let trueParticle = Particle { pparameter = MyParameter optOmega2
                            , pstate = MyState 0 (UV.fromList [optPhi0, 0])
                            , pestimate = optPhi0
                            , pweight = 1/fromIntegral optNParticles
                            }
    let particles = V.map (\parameter -> trueParticle { pparameter = MyParameter parameter }) parameters
    return (PFRequest particles)

jqc :: JobQueueConfig
jqc = defaultJobQueueConfig "perf:pfilter:"


$(mkManyHasTypeHash [ [t| PFResponse MyParameter |]
                    , [t| PFRequest MyParameter MyState MySummary MyInput |]
                    , [t| PFOutput |]
                    , [t| PFInput |]
                    , [t| PFContext MyParameter MyState MySummary MyInput |]
                    , [t| PFState MyParameter MyState MySummary |]
                    ])

csvInfo ::  Options -> CSVInfo
csvInfo opts = CSVInfo
    [ ("stepsize", pack . show . optStepsize $ opts)
    , ("deltat", pack . show . optDeltaT $ opts)
    , ("steps", pack . show . optSteps $ opts)
    , ("particles", pack . show . optNParticles $ opts)
    , ("omega2", pack . show . optOmega2 $ opts)
    , ("omega2_interval", pack . show . optOmega2Range $ opts)
    , ("phi0", pack . show . optPhi0 $ opts)
    , ("resample_threshold", pack . show . optResampleThreshold $ opts)
    ]
