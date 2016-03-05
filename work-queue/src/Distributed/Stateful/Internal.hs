{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE OverloadedStrings #-}
module Distributed.Stateful.Internal where

import           ClassyPrelude
import           Control.DeepSeq (NFData)
import           Control.Monad.Logger
import qualified Data.HashMap.Strict as HMS
import qualified Data.HashSet as HS
import qualified Data.Serialize as B
import           Data.Serialize.Orphans ()
import           Data.TypeFingerprint
import           Text.Printf (PrintfArg(..))
import           Data.Proxy (Proxy(..))

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
      !(HMS.HashMap StateId state) -- States to add
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
  deriving (Generic, Eq, Show, NFData, B.Serialize)

instance (HasTypeFingerprint state, HasTypeFingerprint context, HasTypeFingerprint input) => HasTypeFingerprint (SlaveReq state context input) where
    typeFingerprint _ = typeFingerprint (Proxy :: Proxy (state, context, input))

data SlaveResp state output
  = SRespResetState
  | SRespAddStates
  | SRespRemoveStates
      !SlaveId
      !(HMS.HashMap StateId state)
  | SRespUpdate !(HMS.HashMap StateId (HMS.HashMap StateId output)) -- TODO consider making this a simple list -- we don't really need it to be a HMS.
  | SRespGetStates !(HMS.HashMap StateId state)
  | SRespError Text
  deriving (Generic, Eq, Show, NFData, B.Serialize)

instance (HasTypeFingerprint state, HasTypeFingerprint output) => HasTypeFingerprint (SlaveResp state output) where
    typeFingerprint _ = typeFingerprint (Proxy :: Proxy (state, output))

displayReq :: SlaveReq state context input -> Text
displayReq (SReqResetState mp) = "SReqResetState (" <> pack (show (HMS.keys mp)) <> ")"
displayReq (SReqAddStates mp) = "SReqAddStates (" <> pack (show (HMS.keys mp)) <> ")"
displayReq (SReqRemoveStates k mp) = "SReqRemoveStates (" <> pack (show k) <> ") " <> "(" <> pack (show (HS.toList mp)) <> ")"
displayReq (SReqUpdate _ mp) = "SReqUpdate (" <> pack (show (fmap HMS.keys mp)) <> ")"
displayReq SReqGetStates = "SReqGetStates"

displayResp :: SlaveResp state output -> Text
displayResp SRespResetState = "SRespResetState"
displayResp SRespAddStates = "SRespAddStates"
displayResp (SRespRemoveStates k mp) = "SRespRemoveStates (" <> pack (show k) <> ") " <> "(" <> pack (show (HMS.keys mp)) <> ")"
displayResp (SRespUpdate mp) = "SRespUpdate (" <> pack (show (fmap HMS.keys mp)) <> ")"
displayResp (SRespGetStates mp) = "SRespGetStates (" <> pack (show (HMS.keys mp)) <> ")"
displayResp (SRespError err) = "SRespError " <> pack (show err)

type LogFunc = Loc -> LogSource -> LogLevel -> LogStr -> IO ()

throwAndLog :: (Exception e, MonadIO m) => LogFunc -> e -> m a
throwAndLog logFunc err = do
  runLoggingT (logErrorN (pack (show err))) logFunc
  liftIO $ throwIO err
