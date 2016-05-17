{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
module Distributed.Stateful.Internal where

import           ClassyPrelude
import           Control.DeepSeq (NFData)
import           Control.Monad.Logger
import qualified Data.HashMap.Strict as HMS
import qualified Data.HashSet as HS
import           Data.Proxy (Proxy(..))
import qualified Data.Serialize as B
import           Data.Serialize.Orphans ()
import           Data.TypeFingerprint
import           Text.Printf (PrintfArg(..))

data StatefulConn m req resp = StatefulConn
  { scWrite :: !(req -> m ())
  , scRead :: !(m resp)
  }

newtype SlaveId = SlaveId {unSlaveId :: Int}
  deriving (Generic, Eq, Ord, Show, Hashable, NFData, B.Serialize)
instance PrintfArg SlaveId where
  formatArg = formatArg . unSlaveId
  parseFormat = parseFormat . unSlaveId

newtype StateId = StateId {unStateId :: Int}
  deriving (Generic, Eq, Ord, Show, Hashable, NFData, B.Serialize)
instance PrintfArg StateId where
  formatArg = formatArg . unStateId
  parseFormat = parseFormat . unStateId

data SlaveReq state context input
  = SReqResetState
      !(HMS.HashMap StateId state) -- New states
  | SReqAddStates
      !(HMS.HashMap StateId ByteString)
      -- States to add. 'ByteString' because they're just
      -- forwarded by master.
  | SReqRemoveStates
      !SlaveId
      !(HS.HashSet StateId) -- States to get
  | SReqUpdate
      !context
      !(HMS.HashMap StateId (HMS.HashMap StateId input))
      -- The outer map tells us which states to update. The inner map
      -- provides the new StateIds, and the inputs which should be
      -- provided to 'saUpdate'.
  | SReqGetStates
  | SReqQuit
  deriving (Generic, Eq, Show, NFData, B.Serialize)

instance (HasTypeFingerprint state, HasTypeFingerprint context, HasTypeFingerprint input) => HasTypeFingerprint (SlaveReq state context input) where
    typeFingerprint _ = combineTypeFingerprints
        [ typeFingerprint (Proxy :: Proxy state)
        , typeFingerprint (Proxy :: Proxy context)
        , typeFingerprint (Proxy :: Proxy input)
        ]
    showType _ = "SlaveReq (" ++ showType (Proxy :: Proxy state) ++ ") (" ++ showType (Proxy :: Proxy context) ++ ") (" ++ showType (Proxy :: Proxy input) ++ ")"

data SlaveResp state output
  = SRespResetState
  | SRespAddStates
      !(HS.HashSet StateId)
  | SRespRemoveStates
      !SlaveId
      !(HMS.HashMap StateId ByteString)
      -- States to remove. 'ByteString' because they're just
      -- forwarded by master.
  | SRespUpdate !(HMS.HashMap StateId (HMS.HashMap StateId output)) -- TODO consider making this a simple list -- we don't really need it to be a HMS.
  | SRespGetStates !(HMS.HashMap StateId state)
  | SRespError Text
  | SRespQuit
  deriving (Generic, Eq, Show, NFData, B.Serialize)

instance (HasTypeFingerprint state, HasTypeFingerprint output) => HasTypeFingerprint (SlaveResp state output) where
    typeFingerprint _ = combineTypeFingerprints
        [ typeFingerprint (Proxy :: Proxy state)
        , typeFingerprint (Proxy :: Proxy output)
        ]
    showType _ = "SlaveResp (" ++ showType (Proxy :: Proxy state) ++ ") (" ++ showType (Proxy :: Proxy output) ++ ")"

displayReq :: SlaveReq state context input -> Text
displayReq (SReqResetState mp) = "SReqResetState (" <> pack (show (HMS.keys mp)) <> ")"
displayReq (SReqAddStates mp) = "SReqAddStates (" <> pack (show (HMS.keys mp)) <> ")"
displayReq (SReqRemoveStates k mp) = "SReqRemoveStates (" <> pack (show k) <> ") (" <>pack (show (HS.toList mp)) <> ")"
displayReq (SReqUpdate _ mp) = "SReqUpdate (" <> pack (show (fmap HMS.keys mp)) <> ")"
displayReq SReqGetStates = "SReqGetStates"
displayReq SReqQuit = "SReqQuit"

displayResp :: SlaveResp state output -> Text
displayResp SRespResetState = "SRespResetState"
displayResp (SRespAddStates states) = "SRespAddStates (" <> pack (show states) <> ")"
displayResp (SRespRemoveStates k mp) = "SRespRemoveStates (" <> pack (show k) <> ") (" <> pack (show (HMS.keys mp)) <> ")"
displayResp (SRespUpdate mp) = "SRespUpdate (" <> pack (show (fmap HMS.keys mp)) <> ")"
displayResp (SRespGetStates mp) = "SRespGetStates (" <> pack (show (HMS.keys mp)) <> ")"
displayResp (SRespError err) = "SRespError " <> pack (show err)
displayResp SRespQuit = "SRespQuit"

throwAndLog :: (Exception e, MonadIO m, MonadLogger m) => e -> m a
throwAndLog err = do
  logErrorN (pack (show err))
  liftIO $ throwIO err
