{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}
module Distributed.Stateful.Slave
  ( SlaveArgs(..)
  , StateId
  , runSlave
  ) where

import           ClassyPrelude
import           Control.DeepSeq (force, NFData)
import           Control.Exception (evaluate, AsyncException)
import qualified Data.Binary as B
import           Data.Binary.Orphans ()
import qualified Data.Conduit.Network as CN
import qualified Data.HashMap.Strict as HMS
import qualified Data.HashSet as HS
import qualified Data.Streaming.NetworkMessage as NM
import           Distributed.Stateful.Internal

-- | Arguments for 'runSlave'.
data SlaveArgs state context input output = SlaveArgs
  { saUpdate :: !(context -> input -> state -> IO (state, output))
    -- ^ Function run on the slave when 'update' is invoked on the
    -- master.
  , saInit :: !(IO ())
    -- ^ Action to run upon initial connection.
  , saClientSettings :: !CN.ClientSettings
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
  deriving (Show, Typeable)

instance Exception SlaveException

-- | Runs a stateful slave, and never returns (though may throw
-- exceptions).
runSlave :: forall state context input output a.
     ( B.Binary state, NFData state, B.Binary context, B.Binary input, B.Binary output, NFData output
     )
  => SlaveArgs state context input output -> IO a
runSlave SlaveArgs{..} =
    CN.runTCPClient saClientSettings $
    NM.generalRunNMApp saNMSettings (const "") (const "") $ \nm -> do
      saInit
      go (NM.nmRead nm) (NM.nmWrite nm) HMS.empty
  where
    throw = throwAndLog saLogFunc
    go :: forall b.
         IO (SlaveReq state context input)
      -> (SlaveResp state output -> IO ())
      -> (HMS.HashMap StateId state)
      -> IO b
    go recv send states = do
      req <- recv
      eres <- try $ do
        res <- case req of
          SReqResetState states' -> return (SRespResetState, states')
          SReqAddStates newStates -> do
            let aliased = HMS.keys (HMS.intersection newStates states)
            unless (null aliased) $ throw (AddingExistingStates aliased)
            return (SRespAddStates, HMS.union newStates states)
          SReqGetStates -> return (SRespGetStates states, states)
          SReqRemoveStates slaveRequesting stateIdsToDelete -> do
            let eitherLookup sid =
                  case HMS.lookup sid states of
                    Nothing -> Left sid
                    Just x -> Right (sid, x)
            let (missing, toSend) =
                  partitionEithers $ map eitherLookup $ HS.toList stateIdsToDelete
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
            return (SRespUpdate outputs, states')
        evaluate (force res)
      case eres of
        Right (output, states') -> do
          send output
          go recv send states'
        Left (fromException -> Just (err :: AsyncException)) -> throwIO err
        Left (err :: SomeException) -> do
          send (SRespError (pack (show err)))
          throwAndLog saLogFunc err
