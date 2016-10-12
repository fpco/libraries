{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-|
Module: Distributed.Stateful.Internal.Profiling
Descriptions: Measure the time that certain actions take during the course of a distributed computation.

We do not use the profiling features of the run-time system, for two reasons:

- It has significant performance costs
- We need profiling across different system processes

Instead, we use the function 'withProfiling' to take the wall-time,
cpu-time, and number of times an action is invoked.
-}
module Distributed.Stateful.Internal.Profiling where

import ClassyPrelude
import Control.DeepSeq
import Control.Lens
import Criterion.Measurement
import Data.Store (Store)
import Data.Store.TypeHash (mkHasTypeHash)

-- | Indicate whether to perform profiling or not.
data DoProfiling = DoProfiling
                 | NoProfiling
                 deriving (Generic, Eq, Show, NFData, Store)

-- | Data type to store profiling information for a section of code.
data ProfilingCounter = ProfilingCounter
    { _pcWallTime :: {-# UNPACK #-} !Double -- ^ Accumulated wall-time spent in a section
    , _pcCPUTime  :: {-# UNPACK #-} !Double -- ^ Accumulated CPU-time spent in a section
    , _pcCount    :: {-# UNPACK #-} !Int    -- ^ Number of times a section is invoked
    } deriving (Eq, Show, Generic, NFData)
instance Store ProfilingCounter
makeLenses ''ProfilingCounter

instance Semigroup ProfilingCounter where
    pc <> pc' = ProfilingCounter
        { _pcWallTime = view pcWallTime pc + view pcWallTime pc'
        , _pcCPUTime = view pcCPUTime pc + view pcCPUTime pc'
        , _pcCount = view pcCount pc + view pcCount pc'
        }

instance Monoid ProfilingCounter where
    mempty = ProfilingCounter 0 0 0
    mappend = (<>)

-- | Representation of profiling data to store in CSV files.
--
-- Each tuple has the name and the printed representation of the
-- profiling result.
type ProfilingOutput = [(Text, Text)]

profilingCounterOutput :: Text -> ProfilingCounter -> ProfilingOutput
profilingCounterOutput name ProfilingCounter{..} =
    [ (name ++ "Wall", tshow _pcWallTime)
    , (name ++ "CPU", tshow _pcCPUTime)
    , (name ++ "Count", tshow _pcCount)
    ]

-- | Profiling data for a slave.
data SlaveProfiling = SlaveProfiling
    { _spReceive        :: !ProfilingCounter
    , _spWork           :: !ProfilingCounter
    , _spSend           :: !ProfilingCounter
    , _spStatefulUpdate :: !ProfilingCounter
    , _spHTLookups      :: !ProfilingCounter
    , _spHTInserts      :: !ProfilingCounter
    , _spHTDeletes      :: !ProfilingCounter
    , _spHTFromList     :: !ProfilingCounter
    , _spHTToList       :: !ProfilingCounter
    , _spUpdate         :: !ProfilingCounter
    } deriving (Eq, Show, Generic, NFData)
instance Store SlaveProfiling
makeLenses ''SlaveProfiling
$(mkHasTypeHash =<< [t|Maybe SlaveProfiling|])

emptySlaveProfiling :: SlaveProfiling
emptySlaveProfiling = SlaveProfiling mempty mempty mempty mempty mempty mempty mempty mempty mempty mempty

-- combine profiling data from multiple slaves by summing
instance Semigroup SlaveProfiling where
    sp <> sp' = SlaveProfiling
        { _spReceive = view spReceive sp <> view spReceive sp'
        , _spWork = view spWork sp <> view spWork sp'
        , _spSend = view spSend sp <> view spSend sp'
        , _spStatefulUpdate = view spStatefulUpdate sp <> view spStatefulUpdate sp'
        , _spHTLookups = view spHTLookups sp <> view spHTLookups sp'
        , _spHTInserts = view spHTInserts sp <> view spHTInserts sp'
        , _spHTDeletes = view spHTDeletes sp <> view spHTDeletes sp'
        , _spHTFromList = view spHTFromList sp <> view spHTFromList sp'
        , _spHTToList = view spHTToList sp <> view spHTToList sp'
        , _spUpdate = view spUpdate sp <> view spUpdate sp'
        }

slaveProfilingOutput :: SlaveProfiling -> ProfilingOutput
slaveProfilingOutput SlaveProfiling{..} = concat
    [ profilingCounterOutput "spReceive" _spReceive
    , profilingCounterOutput "spWork" _spWork
    , profilingCounterOutput "spSend" _spSend
    , profilingCounterOutput "spStatefulUpdate" _spStatefulUpdate
    , profilingCounterOutput "spHTLookups" _spHTLookups
    , profilingCounterOutput "spHTInserts" _spHTInserts
    , profilingCounterOutput "spHTDeletes" _spHTDeletes
    , profilingCounterOutput "spHTFromList" _spHTFromList
    , profilingCounterOutput "spHTToList" _spHTToList
    , profilingCounterOutput "spUpdate" _spUpdate
    ]

-- | Collect timing data for some action
withProfiling :: forall a b m. MonadIO m
    => Maybe (IORef b)
    -> Lens' b ProfilingCounter
    -> m a
    -> m a
withProfiling Nothing _ action = action
withProfiling (Just ref) l action = do
    tw0 <- liftIO getTime
    tcpu0 <- liftIO getCPUTime
    res <- action
    tw1 <- liftIO getTime
    tcpu1 <- liftIO getCPUTime
    liftIO . modifyIORef' ref $ update (tw1 - tw0) (tcpu1 - tcpu0)
    return res
  where
      update :: Double -> Double -> b -> b
      update tw tcpu sp = sp & l . pcWallTime +~ tw
                             & l . pcCPUTime +~ tcpu
                             & l . pcCount +~ 1
{-# INLINE withProfiling #-}
