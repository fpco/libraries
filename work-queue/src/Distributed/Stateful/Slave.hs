{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE ViewPatterns #-}
module Distributed.Stateful.Slave
  ( SlaveArgs(..)
  , StateId
  , runSlave
  ) where

import           ClassyPrelude
import           Control.DeepSeq (force, NFData)
import           Control.Exception.Lifted (evaluate, AsyncException)
import           Control.Monad.Logger (logDebugNS, MonadLogger)
import qualified Data.Conduit.Network as CN
import qualified Data.HashMap.Strict as HMS
import qualified Data.HashSet as HS
import qualified Data.Streaming.NetworkMessage as NM
import           Distributed.Stateful.Internal
import           Distributed.ConnectRequest (WorkerConnectInfo(..))
import           Control.Monad.Trans.Control (MonadBaseControl, control)

-- | Arguments for 'runSlave'.
data SlaveArgs m state context input output = SlaveArgs
  { saUpdate :: !(context -> input -> state -> m (state, output))
    -- ^ Function run on the slave when 'update' is invoked on the
    -- master.
  , saConnectInfo :: !WorkerConnectInfo
    -- ^ Settings for the slave's TCP connection.
  , saNMSettings :: !NM.NMSettings
    -- ^ Settings for the connection to the master.
  }

data SlaveException
  = AddingExistingStates [StateId]
  | MissingStatesToRemove [StateId]
  | InputStateNotFound StateId
  deriving (Eq, Show, Typeable)

instance Exception SlaveException

-- | Runs a stateful slave. Returns when it gets disconnected from the
-- master.
runSlave :: forall state context input output void m.
     ( MonadIO m, MonadBaseControl IO m, MonadLogger m
     , NM.Sendable (SlaveReq state context input), NM.Sendable (SlaveResp state output)
     , NFData state, NFData output
     )
  => SlaveArgs m state context input output
  -> m void
runSlave SlaveArgs{..} = do
    let WorkerConnectInfo host port = saConnectInfo
    -- REVIEW TODO: Shouldn't this be 'tryAny' or similar too?
    control $ \run -> CN.runTCPClient (CN.clientSettings port host) $
      NM.runNMApp saNMSettings $ \nm -> run $ do
        go (NM.nmRead nm) (NM.nmWrite nm) HMS.empty
  where
    throw = throwAndLog
    debug msg = logDebugNS "Distributed.Stateful.Slave" msg
    go :: 
         m (SlaveReq state context input)
      -> (SlaveResp state output -> m ())
      -> (HMS.HashMap StateId state)
      -> m void
    go recv send states = do
      req <- recv
      debug (displayReq req)
      eres <- try ((do
        res <- case req of
          SReqResetState states' -> return (SRespResetState, states')
          SReqGetStates -> return (SRespGetStates states, states)
          SReqAddStates newStates -> do
            let aliased = HMS.keys (HMS.intersection newStates states)
            unless (null aliased) $ throw (AddingExistingStates aliased)
            return (SRespAddStates, HMS.union newStates states)
          SReqRemoveStates slaveRequesting stateIdsToDelete -> do
            let eitherLookup sid =
                  case HMS.lookup sid states of
                    Nothing -> Left sid
                    Just x -> Right (sid, x)
            let (missing, toSend) = partitionEithers $ map eitherLookup $ HS.toList stateIdsToDelete
            unless (null missing) $ throw (MissingStatesToRemove missing)
            let states' = foldl' (flip HMS.delete) states stateIdsToDelete
            return (SRespRemoveStates slaveRequesting (HMS.fromList toSend), states')
          SReqUpdate context inputs -> do
            results <- forM (HMS.toList inputs) $ \(oldStateId, innerInputs) ->
              if null innerInputs then return Nothing else Just <$> do
                state <- case HMS.lookup oldStateId states of
                  Nothing -> throw (InputStateNotFound oldStateId)
                  Just state0 -> return state0
                fmap ((oldStateId, ) . HMS.fromList) $ forM (HMS.toList innerInputs) $ \(newStateId, input) ->
                  fmap (newStateId, ) $ saUpdate context input state
            let resultsMap = HMS.fromList (catMaybes results)
            let states' = foldMap (fmap fst) resultsMap
            let outputs = fmap (fmap snd) resultsMap
            return (SRespUpdate outputs, states' `HMS.union` (states `HMS.difference` inputs))
        evaluate (force res)) :: m (SlaveResp state output, HMS.HashMap StateId state))
      case eres of
        Right (output, states') -> do
          send output
          debug (displayResp output)
          go recv send states'
        Left (fromException -> Just (err :: AsyncException)) -> throwIO err
        Left (err :: SomeException) -> do
          send (SRespError (pack (show err)))
          throwAndLog err
