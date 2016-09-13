{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
module Distributed.Stateful.Internal.Profiling where

import           Data.Store (Store)
import           Control.DeepSeq
import           ClassyPrelude
import           Control.Lens
import           Criterion.Measurement


-- | Profiling data for the slave.
--
-- We measure the wall-time for the actual work, as well as for
-- sending and waiting for messages.
--
-- The times are total times, accumulated over the whole distributed
-- calculation.
data SlaveProfiling = SlaveProfiling
    { _spReceive :: !Double
    , _spWork :: !Double
    , _spSend :: !Double
    , _spStatefulUpdate :: !Double
    , _spHTLookups :: !Double
    , _spHTInserts :: !Double
    , _spHTDeletes :: !Double
    , _spHTFromList :: !Double
    , _spHTToList :: !Double
    , _spUpdate :: !Double
    , _spNUpdates :: !Int
    , _spUpdateInner :: !Double
    , _spUpdateInnerBody :: !Double
    , _spEvalInputs :: !Double
    , _spEvalContext :: !Double
    , _spEvalInput :: !Double
    , _spEvalState :: !Double
    } deriving (Eq, Show, Generic, NFData)
instance Store SlaveProfiling
makeLenses ''SlaveProfiling

emptySlaveProfiling :: SlaveProfiling
emptySlaveProfiling = SlaveProfiling 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0

-- combine profiling data by summing
instance Semigroup SlaveProfiling where
    sp <> sp' = SlaveProfiling
        { _spReceive = view spReceive sp + view spReceive sp'
        , _spWork = view spWork sp + view spWork sp'
        , _spSend = view spSend sp + view spSend sp'
        , _spStatefulUpdate = view spStatefulUpdate sp + view spStatefulUpdate sp'
        , _spHTLookups = view spHTLookups sp + view spHTLookups sp'
        , _spHTInserts = view spHTInserts sp + view spHTInserts sp'
        , _spHTDeletes = view spHTDeletes sp + view spHTDeletes sp'
        , _spHTFromList = view spHTFromList sp + view spHTFromList sp'
        , _spHTToList = view spHTToList sp + view spHTToList sp'
        , _spUpdate = view spUpdate sp + view spUpdate sp'
        , _spNUpdates = view spNUpdates sp + view spNUpdates sp'
        , _spUpdateInner = view spUpdateInner sp + view spUpdateInner sp'
        , _spUpdateInnerBody = view spUpdateInnerBody sp + view spUpdateInnerBody sp'
        , _spEvalInputs = view spEvalInputs sp + view spEvalInputs sp'
        , _spEvalContext = view spEvalContext sp + view spEvalContext sp'
        , _spEvalInput = view spEvalInput sp + view spEvalInput sp'
        , _spEvalState = view spEvalState sp + view spEvalState sp'
        }

slaveProfilingToCsv :: SlaveProfiling -> [(Text, Text)]
slaveProfilingToCsv sp =
    [ ("slaveReceiveFraction", tshow $ fraction spReceive)
    , ("slaveWorkFraction", tshow $ fraction spWork)
    , ("slaveSendFraction", tshow $ fraction spSend)
    , ("slaveReceiveTime", tshow $ view spReceive sp)
    , ("slaveWorkTime", tshow $ view spWork sp)
    , ("slaveSendTime", tshow $ view spSend sp)
    , ("StatefulUpdate", tshow $ view spStatefulUpdate sp)
    , ("HTLookups", tshow $ view spHTLookups sp)
    , ("HTInserts", tshow $ view spHTInserts sp)
    , ("HTDeletes", tshow $ view spHTDeletes sp)
    , ("HTFromLis", tshow $ view spHTFromList sp)
    , ("HTToList", tshow $ view spHTToList sp)
    , ("Update", tshow $ view spUpdate sp)
    , ("NUpdates", tshow $ view spNUpdates sp)
    , ("UpdateInner", tshow $ view spUpdateInner sp)
    , ("UpdateInnerBody", tshow $ view spUpdateInnerBody sp)
    , ("EvalInputs", tshow $ view spEvalInputs sp)
    , ("EvalContext", tshow $ view spEvalContext sp)
    , ("EvalInput", tshow $ view spEvalInput sp)
    , ("EvalState", tshow $ view spEvalState sp)
    ]
  where
    total = sum [view l sp | l <- [spReceive, spWork, spSend]]
    fraction l = view l sp / total

data MasterProfiling = MasterProfiling
    { _mpTotalUpdate :: !Double
    , _mpUpdateSlaves :: !Double
    , _mpUpdateSlavesStep :: !Double
    , _mpSendLoop :: !Double
    , _mpSlaveLoop :: !Double
    } deriving (Generic, Show)
instance Store MasterProfiling
makeLenses ''MasterProfiling

emptyMasterProfiling :: MasterProfiling
emptyMasterProfiling = MasterProfiling 0 0 0 0 0

masterProfilingToCsv :: MasterProfiling -> [(Text, Text)]
masterProfilingToCsv mp =
    [ ("mpTotalUpdate", tshow $ view mpTotalUpdate mp)
    , ("mpUpdateSlaves", tshow $ view mpUpdateSlaves mp)
    , ("mpUpdateSlavesStep", tshow $ view mpUpdateSlavesStep mp)
    , ("mpSendLoop", tshow $ view mpSendLoop mp)
    , ("mpSlaveLoop", tshow $ view mpSlaveLoop mp)
    ]

withProfiling :: forall a b m. MonadIO m
    => IORef b
    -> Lens' b Double
    -> m a
    -> m a
withProfiling ref l action = do
    t0 <- liftIO getTime
    res <- action
    t1 <- liftIO getTime
    liftIO . modifyIORef' ref $ update (t1 - t0)
    return res
  where
      update :: Double -> b -> b
      update t sp = sp & l +~ t
withProfilingMVar :: forall a b m. MonadIO m
    => MVar b
    -> Lens' b Double
    -> m a
    -> m a
withProfilingMVar ref l action = do
    t0 <- liftIO getTime
    res <- action
    t1 <- liftIO getTime
    liftIO . modifyMVar_ ref $ update (t1 - t0)
    return res
  where
      update :: Double -> b -> IO b
      update t sp = return $ sp & l +~ t

withSlaveProfileCounter :: MonadIO m
    => IORef SlaveProfiling
    -> Lens' SlaveProfiling Int
    -> m a
    -> m a
withSlaveProfileCounter ref l action = do
    res <- action
    liftIO . modifyIORef' ref $ \sp -> sp & l +~ 1
    return res
