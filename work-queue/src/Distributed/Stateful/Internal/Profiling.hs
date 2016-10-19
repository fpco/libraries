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
    } deriving (Eq, Show, Generic, NFData, Store)
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
    } deriving (Eq, Show, Generic, NFData, Store)
makeLenses ''SlaveProfiling

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

-- | Profiling data for the master
data MasterProfiling = MasterProfiling
    { _mpTotalUpdate      :: !ProfilingCounter
    , _mpUpdateSlaves     :: !ProfilingCounter
    , _mpUpdateSlavesStep :: !ProfilingCounter
    , _mpSend             :: !ProfilingCounter
    , _mpMasterLoop       :: !ProfilingCounter
    , _mpHandleResponse   :: !ProfilingCounter
    , _mpDecode           :: !ProfilingCounter
    , _mpReceive          :: !ProfilingCounter
    } deriving (Eq, Generic, Show, Store)
makeLenses ''MasterProfiling

emptyMasterProfiling :: MasterProfiling
emptyMasterProfiling = MasterProfiling mempty mempty mempty mempty mempty mempty mempty mempty

-- | Profiling data for a Stateful computation.
data Profiling = Profiling
    { profMaster :: !MasterProfiling
      -- ^ profiling for the master
    , profSlave  :: !(Maybe SlaveProfiling)
      -- ^ summed profiling of all slaves (if there were any)
    } deriving (Eq, Generic, Show, Store)
$(mkHasTypeHash =<< [t|Maybe Profiling|])

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

-- | Representation of profiling data to store in CSV files.
--
-- Each tuple has the name and the printed representation of the
-- profiling result.
type ProfilingColumns = [(Text, Text)]

profilingCounterColumns :: Text -> ProfilingCounter -> ProfilingColumns
profilingCounterColumns name ProfilingCounter{..} =
    [ (name ++ "Wall", tshow _pcWallTime)
    , (name ++ "CPU", tshow _pcCPUTime)
    , (name ++ "Count", tshow _pcCount)
    ]

slaveProfilingColumns :: SlaveProfiling -> ProfilingColumns
slaveProfilingColumns SlaveProfiling{..} = concat
    [ profilingCounterColumns "spReceive" _spReceive
    , profilingCounterColumns "spWork" _spWork
    , profilingCounterColumns "spSend" _spSend
    , profilingCounterColumns "spStatefulUpdate" _spStatefulUpdate
    , profilingCounterColumns "spHTLookups" _spHTLookups
    , profilingCounterColumns "spHTInserts" _spHTInserts
    , profilingCounterColumns "spHTDeletes" _spHTDeletes
    , profilingCounterColumns "spHTFromList" _spHTFromList
    , profilingCounterColumns "spHTToList" _spHTToList
    , profilingCounterColumns "spUpdate" _spUpdate
    ]

masterProfilingColumns :: MasterProfiling -> ProfilingColumns
masterProfilingColumns MasterProfiling{..} = concat
    [ profilingCounterColumns "mpTotalUpdate" _mpTotalUpdate
    , profilingCounterColumns "mpUpdateSlaves" _mpUpdateSlaves
    , profilingCounterColumns "mpUpdateSlavesStep" _mpUpdateSlavesStep
    , profilingCounterColumns "mpSend" _mpSend
    , profilingCounterColumns "mpMasterLoop" _mpMasterLoop
    , profilingCounterColumns "mpHandleResponse" _mpHandleResponse
    , profilingCounterColumns "mpDecode" _mpDecode
    , profilingCounterColumns "mpReceive" _mpReceive
    ]

mProfilingColumns :: Maybe Profiling -> ProfilingColumns
mProfilingColumns Nothing = map (second (const "NA")) $ slaveProfilingColumns emptySlaveProfiling <> masterProfilingColumns emptyMasterProfiling
mProfilingColumns (Just (Profiling mp Nothing)) = map (second (const "NA")) (slaveProfilingColumns emptySlaveProfiling) <> masterProfilingColumns mp
mProfilingColumns (Just (Profiling mp (Just sp))) = slaveProfilingColumns sp <> masterProfilingColumns mp
