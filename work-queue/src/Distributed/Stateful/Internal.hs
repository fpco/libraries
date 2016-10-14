{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE MultiParamTypeClasses #-}
module Distributed.Stateful.Internal
       ( module Distributed.Stateful.Internal
       , module Distributed.Stateful.Internal.Profiling
       ) where

import           ClassyPrelude
import           Control.DeepSeq (NFData, force)
import           Control.Exception.Lifted (evaluate)
import           Control.Monad.Logger.JSON.Extra
import           Control.Monad.Trans.Control (MonadBaseControl)
import qualified Data.HashSet as HS
import qualified Data.HashTable.IO as HT
import           Data.Proxy (Proxy(..))
import           Data.Store (Store)
import           Data.Store.TypeHash
import           Data.Store.TypeHash.Orphans ()
import           Distributed.Stateful.Internal.Profiling
import           Text.Printf (PrintfArg(..))
import qualified Data.Store.Streaming as S
import qualified System.IO.ByteBuffer as BB
import qualified Data.ByteString as BS
import qualified Data.Store as S
import qualified Data.Vector as V

data EventType
  = ETRead
  deriving (Show, Eq)

data EventManager m key = EventManager
  { emControl :: !(key -> EventType -> m ())
  , emControlDelete :: !(key -> m ())
  , emWait :: !(m (V.Vector (key, EventType)))
  }

data StatefulConn m key req resp = StatefulConn
  { scWrite :: !(BS.ByteString -> m ())
  , scRead :: !(m BS.ByteString)

  , scByteBuffer :: !(BB.ByteBuffer)
  , scFillByteBuffer :: !(S.FillByteBuffer () m)

  , scConnKey :: !key
  }

{-
-- | Stateful connection.
--
-- Values of type @req@ can be written to, and values of type @resp@
-- can be read from the connection.
data StatefulConn m req resp = StatefulConn
  { scWrite :: !(BS.ByteString -> m ())
    -- ^ A blocking write.
  , scRegisterCanRead :: !(forall a. m () -> m a -> m a)
    -- ^ Calls the callback when the stateful conn can read.
  , scRead :: !(m BS.ByteString)
    -- ^ A blocking read.
  , scByteBuffer :: !BB.ByteBuffer
  , scFillByteBuffer :: !(S.FillByteBuffer () m)
    -- ^ Non-blockingly write all the readable stuff in the conn into a byte
    -- buffer.
  }
-}

{-# INLINE scEncodeAndWrite #-}
scEncodeAndWrite :: (Store req, MonadIO m) => StatefulConn m key req resp -> req -> m ()
scEncodeAndWrite conn x = scWrite conn (S.encodeMessage (S.Message x))

newtype StatefulConnDecodeFailure = StatefulConnDecodeFailure String
  deriving (Typeable, Show)
instance Exception StatefulConnDecodeFailure

scDecodeAndRead :: (Store resp, MonadIO m, MonadBaseControl IO m) => StatefulConn m key req resp -> m resp
scDecodeAndRead conn = catch
  (do mbResp <- S.decodeMessageBS (scByteBuffer conn) $ do
        bs <- scRead conn
        if null bs then return Nothing else return (Just bs)
      case mbResp of
        Nothing -> liftIO (throwIO (StatefulConnDecodeFailure "scDecodeAndRead: no data"))
        Just (S.Message resp) -> return resp)
  (\ ex@(S.PeekException _ _) -> throwIO . StatefulConnDecodeFailure . ("scDecodeAndRead: " ++) . show $ ex)

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
  = -- | Initialise a slave, specify whether it should perform profiling.
    SReqInit !DoProfiling
    -- | Get all the states.
  | SReqResetState
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
  = SRespInit
  | SRespResetState
  | SRespAddStates
      ![StateId]
  | SRespRemoveStates
      !SlaveId
      ![(StateId, ByteString)]
      -- States to remove. 'ByteString' because they're just
      -- forwarded by master.
  | SRespUpdate ![(StateId, [(StateId, output)])]
  | SRespGetStates ![(StateId, state)]
  | SRespGetProfile !(Maybe SlaveProfiling)
  | SRespError Text
  | SRespQuit
  deriving (Generic, Eq, Show, NFData, Store)

instance (HasTypeHash state, HasTypeHash output) => HasTypeHash (SlaveResp state output) where
    typeHash _ = typeHash (Proxy :: Proxy (state, output))

displayReq :: SlaveReq state context input -> Text
displayReq (SReqInit doProfiling) = "SReqInit (" <> tshow doProfiling <> ")"
displayReq (SReqResetState mp) = "SReqResetState (" <> tshow (map fst mp) <> ")"
displayReq (SReqAddStates mp) = "SReqAddStates (" <> tshow (map fst mp) <> ")"
displayReq (SReqRemoveStates k mp) = "SReqRemoveStates (" <> tshow k <> ") (" <> tshow mp <> ")"
displayReq (SReqUpdate _ mp) = "SReqUpdate (" <> tshow (map (map fst . snd) mp) <> ")"
displayReq SReqGetStates = "SReqGetStates"
displayReq SReqGetProfile = "SReqGetProfile"
displayReq SReqQuit = "SReqQuit"

displayResp :: SlaveResp state output -> Text
displayResp SRespInit = "SRespInit"
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

type HashTable k v = HT.CuckooHashTable k v

statefulUpdate ::
     (MonadThrow m, MonadIO m, MonadBaseControl IO m, NFData state, NFData output, NFData input, NFData context)
  => Maybe (IORef SlaveProfiling)
  -> (context -> input -> state -> m (state, output))
  -> HashTable StateId state
  -> context
  -> [(StateId, [(StateId, input)])]
  -> m [(StateId, [(StateId, output)])]
statefulUpdate sp update states context inputs = withProfiling sp spStatefulUpdate $
  forM inputs $ \(!oldStateId, !innerInputs) -> do
    state <- (liftIO . withProfiling sp spHTLookups $ HT.lookup states oldStateId) >>= \case
      Nothing -> throwM (InputStateNotFound oldStateId)
      Just state0 -> (liftIO . withProfiling sp spHTDeletes $ states `HT.delete` oldStateId) >> return state0
    updatedInnerStateAndOutput <- forM innerInputs $ \(!newStateId, !input) -> do
        (!newState, !output) <-
            withProfiling sp spUpdate $
            evaluate . force =<< update context input state
        liftIO . withProfiling sp spHTInserts $ HT.insert states newStateId newState
        return (newStateId, output)
    return (oldStateId, updatedInnerStateAndOutput)
