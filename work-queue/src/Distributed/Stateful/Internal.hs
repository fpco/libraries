{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
module Distributed.Stateful.Internal where

import           ClassyPrelude
import           Control.DeepSeq (NFData)
import           Control.Lens
import           Control.Monad.Logger.JSON.Extra
import           Criterion.Measurement
import qualified Data.HashSet as HS
import qualified Data.HashTable.IO as HT
import           Data.Proxy (Proxy(..))
import           Data.Store (Store)
import           Data.Store.TypeHash
import           Data.Store.TypeHash.Orphans ()
import           Text.Printf (PrintfArg(..))

type HashTable k v = HT.CuckooHashTable k v

-- | Stateful connection.
--
-- Values of type @req@ can be written to, and values of type @resp@
-- can be read from the connection.
data StatefulConn m req resp = StatefulConn
  { scWrite :: !(req -> m ())
    -- ^ Write a request to the connection.
  , scRead :: !(m resp)
    -- ^ Read a response from the connection.
  }

-- | Identifier for a slave.
newtype SlaveId = SlaveId {unSlaveId :: Int}
  deriving (Generic, Eq, Ord, Show, Hashable, NFData, Store)
instance PrintfArg SlaveId where
  formatArg = formatArg . unSlaveId
  parseFormat = parseFormat . unSlaveId

-- | Identifier for the state of a sub computation.
newtype StateId = StateId {unStateId :: Int}
  deriving (Generic, Eq, Ord, Show, Hashable, NFData, Store)
instance PrintfArg StateId where
  formatArg = formatArg . unStateId
  parseFormat = parseFormat . unStateId

-- | One step in a stateful computation.
type Update m state context input output =
    context
    -- ^ Read-only environment that is the same for all computations
    -> input
    -- ^ Input specific to this step in the computation
    -> state
    -- ^ Current state of the computation
    -> m (state, output)
    -- ^ Updated state and output after the computation.

-- | Profiling data for the slave.
--
-- We measure the wall-time for the actual work, as well as for
-- sending and waiting for messages.
--
-- To keep the overhead low, we use an exponential moving average.
data SlaveProfiling = SlaveProfiling
    { _spReceiveTime :: !Double
    , _spWorkTime :: !Double
    , _spSendTime :: !Double
    , _spMovingAvgAlpha :: !Double
    } deriving (Eq, Show, Generic, NFData)
instance Store SlaveProfiling
makeLenses ''SlaveProfiling

-- | Exponential moving average
updateMovingAvg :: Double -> Double -> Double -> Double
updateMovingAvg alpha oldAvg newDatum = (1-alpha)*oldAvg + alpha*newDatum

withSlaveProfiling :: MonadIO m
    => IORef SlaveProfiling
    -> Lens' SlaveProfiling Double
    -> m a
    -> m a
withSlaveProfiling ref l action = do
    t0 <- liftIO getTime
    res <- action
    t1 <- liftIO getTime
    liftIO . modifyIORef' ref $ update (t1 - t0)
    return res
  where
      update :: Double -> SlaveProfiling -> SlaveProfiling
      update t sp = let oldAvg = view l sp
                        alpha = view spMovingAvgAlpha sp
                        newAvg = updateMovingAvg alpha oldAvg t
                    in set l newAvg sp

-- | A request to the slaves
data SlaveReq state context input
  = -- | Get all the states.
    SReqResetState
      ![(StateId, state)] -- New states
    -- | Add additional states.  The states should be serialized, in
    -- the form of a 'ByteString'.
  | SReqAddStates
      ![(StateId, ByteString)]
      -- States to add. 'ByteString' because they're just
      -- forwarded by master.
  | SReqRemoveStates
      !SlaveId
      !(HS.HashSet StateId) -- States to get
  | SReqUpdate
      !context
      ![(StateId, [(StateId, input)])]
      -- The outer map tells us which states to update. The inner map
      -- provides the new StateIds, and the inputs which should be
      -- provided to 'saUpdate'.
  | SReqGetStates
  | SReqGetProfile
  | SReqQuit
  deriving (Generic, Eq, Show, NFData, Store)

instance (HasTypeHash state, HasTypeHash context, HasTypeHash input) => HasTypeHash (SlaveReq state context input) where
    typeHash _ = typeHash (Proxy :: Proxy (state, context, input))

data SlaveResp state output
  = SRespResetState
  | SRespAddStates
      ![StateId]
  | SRespRemoveStates
      !SlaveId
      ![(StateId, ByteString)]
      -- States to remove. 'ByteString' because they're just
      -- forwarded by master.
  | SRespUpdate ![(StateId, [(StateId, output)])]
  | SRespGetStates ![(StateId, state)]
  | SRespGetProfile SlaveProfiling
  | SRespError Text
  | SRespQuit
  deriving (Generic, Eq, Show, NFData, Store)

instance (HasTypeHash state, HasTypeHash output) => HasTypeHash (SlaveResp state output) where
    typeHash _ = typeHash (Proxy :: Proxy (state, output))

displayReq :: SlaveReq state context input -> Text
displayReq (SReqResetState mp) = "SReqResetState (" <> tshow (map fst mp) <> ")"
displayReq (SReqAddStates mp) = "SReqAddStates (" <> tshow (map fst mp) <> ")"
displayReq (SReqRemoveStates k mp) = "SReqRemoveStates (" <> tshow k <> ") (" <> tshow mp <> ")"
displayReq (SReqUpdate _ mp) = "SReqUpdate (" <> tshow (map (map fst . snd) mp) <> ")"
displayReq SReqGetStates = "SReqGetStates"
displayReq SReqGetProfile = "SReqGetProfile"
displayReq SReqQuit = "SReqQuit"

displayResp :: SlaveResp state output -> Text
displayResp SRespResetState = "SRespResetState"
displayResp (SRespAddStates states) = "SRespAddStates (" <> tshow states <> ")"
displayResp (SRespRemoveStates k mp) = "SRespRemoveStates (" <> tshow k <> ") (" <> tshow (map fst mp) <> ")"
displayResp (SRespUpdate mp) = "SRespUpdate (" <> tshow (map (map fst . snd) mp) <> ")"
displayResp (SRespGetStates mp) = "SRespGetStates (" <> tshow (map fst mp) <> ")"
displayResp (SRespError err) = "SRespError " <> tshow err
displayResp (SRespGetProfile sp) = "SRespGetStates " <> tshow sp
displayResp SRespQuit = "SRespQuit"

throwAndLog :: (Exception e, MonadIO m, MonadLogger m) => e -> m a
throwAndLog err = do
  logErrorNJ (show err)
  liftIO $ throwIO err

data StatefulUpdateException
  = InputStateNotFound !StateId
  deriving (Eq, Show, Typeable)
instance Exception StatefulUpdateException

statefulUpdate ::
     (MonadThrow m, MonadIO m)
  => (context -> input -> state -> m (state, output))
  -> HashTable StateId state
  -> context
  -> [(StateId, [(StateId, input)])]
  -> m [(StateId, [(StateId, output)])]
statefulUpdate update states context inputs = do
  results <- forM inputs $ \(oldStateId, innerInputs) -> do
    state <- liftIO (HT.lookup states oldStateId) >>= \case
      Nothing -> throwM (InputStateNotFound oldStateId)
      Just state0 -> liftIO (states `HT.delete` oldStateId) >> return state0
    updatedInnerStateAndOutput <- forM innerInputs $ \(newStateId, input) -> do
        (newState, output) <- update context input state
        liftIO $ HT.insert states newStateId newState
        return (newStateId, (newState, output))
    return (oldStateId, updatedInnerStateAndOutput)
  let outputs = map (second (map (second  snd))) results
  return outputs
