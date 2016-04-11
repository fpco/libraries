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
import           Control.Exception (evaluate, AsyncException)
import           Control.Monad.Logger (runLoggingT, logDebugNS)
import qualified Data.Conduit.Network as CN
import qualified Data.HashMap.Strict as HMS
import qualified Data.HashSet as HS
import qualified Data.Streaming.NetworkMessage as NM
import           Data.Void (absurd)
import           Distributed.Stateful.Internal
import           Distributed.Types (LogFunc, WorkerConnectInfo(..))
import           Distributed.WorkQueue (handleSlaveException)

-- | Arguments for 'runSlave'.
data SlaveArgs state context input output = SlaveArgs
  { saUpdate :: !(context -> input -> state -> IO (state, output))
    -- ^ Function run on the slave when 'update' is invoked on the
    -- master.
  , saInit :: !(IO ())
    -- ^ Action to run upon initial connection.
  , saConnectInfo :: !WorkerConnectInfo
    -- ^ Settings for the slave's TCP connection.
  , saNMSettings :: !NM.NMSettings
    -- ^ Settings for the connection to the master.
  , saLogFunc :: !LogFunc
    -- ^ Function used for logging.
  }

data SlaveException
  = AddingExistingStates [StateId]
  | MissingStatesToRemove [StateId]
  | InputStateNotFound StateId
  deriving (Eq, Show, Typeable)

instance Exception SlaveException

-- | Runs a stateful slave. Returns when it gets disconnected from the
-- master.
runSlave :: forall state context input output.
     ( NM.Sendable (SlaveReq state context input), NM.Sendable (SlaveResp state output)
     , NFData state, NFData output
     )
  => SlaveArgs state context input output
  -> IO ()
runSlave SlaveArgs{..} = do
    let WorkerConnectInfo host port = saConnectInfo
    -- REVIEW TODO: Shouldn't this be 'tryAny' or similar too?
    eres <- try $
        CN.runTCPClient (CN.clientSettings port host) $
        NM.runNMApp saNMSettings $ \nm -> do
          saInit
          go (NM.nmRead nm) (NM.nmWrite nm) HMS.empty
    case eres of
        Right x -> absurd x
        Left err -> runLogger $ handleSlaveException saConnectInfo "Distributed.Stateful.Slave.runSlave" err
  where
    runLogger f = runLoggingT f saLogFunc
    throw = throwAndLog saLogFunc
    debug msg = runLogger (logDebugNS "Distributed.Stateful.Slave" msg)
    go :: IO (SlaveReq state context input)
      -> (SlaveResp state output -> IO ())
      -> (HMS.HashMap StateId state)
      -> IO void
    go recv send states = do
      req <- recv
      debug (displayReq req)
      eres <- try $ do
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
        evaluate (force res)
      case eres of
        Right (output, states') -> do
          send output
          debug (displayResp output)
          go recv send states'
        Left (fromException -> Just (err :: AsyncException)) -> throwIO err
        Left (err :: SomeException) -> do
          send (SRespError (pack (show err)))
          throwAndLog saLogFunc err
