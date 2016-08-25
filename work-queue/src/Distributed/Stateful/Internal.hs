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
import           Control.Monad.Logger.JSON.Extra
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
  | SRespError Text
  | SRespQuit
  deriving (Generic, Eq, Show, NFData, Store)

instance (HasTypeHash state, HasTypeHash output) => HasTypeHash (SlaveResp state output) where
    typeHash _ = typeHash (Proxy :: Proxy (state, output))

displayReq :: SlaveReq state context input -> Text
displayReq (SReqResetState mp) = "SReqResetState (" <> pack (show ((map fst) mp)) <> ")"
displayReq (SReqAddStates mp) = "SReqAddStates (" <> pack (show ((map fst) mp)) <> ")"
displayReq (SReqRemoveStates k mp) = "SReqRemoveStates (" <> pack (show k) <> ") (" <> pack (show mp) <> ")"
displayReq (SReqUpdate _ mp) = "SReqUpdate (" <> pack (show (map (map fst . snd) mp)) <> ")"
displayReq SReqGetStates = "SReqGetStates"
displayReq SReqQuit = "SReqQuit"

displayResp :: SlaveResp state output -> Text
displayResp SRespResetState = "SRespResetState"
displayResp (SRespAddStates states) = "SRespAddStates (" <> pack (show states) <> ")"
displayResp (SRespRemoveStates k mp) = "SRespRemoveStates (" <> pack (show k) <> ") (" <> pack (show ((map fst) mp)) <> ")"
displayResp (SRespUpdate mp) = "SRespUpdate (" <> pack (show (map (map fst . snd) mp)) <> ")"
displayResp (SRespGetStates mp) = "SRespGetStates (" <> pack (show ((map fst) mp)) <> ")"
displayResp (SRespError err) = "SRespError " <> pack (show err)
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
  -> m (HashTable StateId state, [(StateId, [(StateId, output)])])
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
  return (states, outputs)
