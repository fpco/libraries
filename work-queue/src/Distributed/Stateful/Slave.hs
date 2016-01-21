{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
module Distributed.Stateful.Slave
  ( SlaveArgs(..)
  , StateId
  , runSlave
  ) where

import           ClassyPrelude
import           Control.DeepSeq (force, NFData)
import           Control.Exception (evaluate)
import qualified Data.Binary as B
import           Data.Binary.Orphans ()
import qualified Data.Conduit.Network as CN
import qualified Data.HashMap.Strict as HMS
import qualified Data.HashSet as HS
import qualified Data.Streaming.NetworkMessage as NM
import           Distributed.Stateful.Internal
import           Text.Printf (printf)

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
  }

data SlaveState state
  = SSNotInitialized
  | SSInitialized !(HMS.HashMap StateId state)
  deriving (Generic, Eq, Show, NFData, B.Binary)

-- | Runs a stateful slave, and never returns (though may throw
-- exceptions).
runSlave :: forall state context input output m a.
     ( B.Binary state, NFData state, B.Binary context, B.Binary input, B.Binary output
     , MonadIO m
     )
  => SlaveArgs state context input output -> m a
runSlave SlaveArgs{..} = liftIO $ do
    liftIO $ CN.runTCPClient saClientSettings $
      NM.generalRunNMApp saNMSettings (const "") (const "") $ \nm -> do
        saInit
        go (NM.nmRead nm) (NM.nmWrite nm) SSNotInitialized
  where
    go :: forall b.
         IO (SlaveReq state context input)
      -> (SlaveResp state output -> IO ())
      -> SlaveState state
      -> IO b
    go recv send = let
      loop slaveState = do
        req <- recv
        let getStates' :: String -> HMS.HashMap StateId state
            getStates' err = case slaveState of
              SSNotInitialized -> error ("slave: state not initialized (" ++ err ++ ")")
              SSInitialized states0 -> states0
        case req of
          SReqResetState states -> do
            send SRespResetState
            loop (SSInitialized states)
          SReqAddStates newStates -> do
            let states' = HMS.unionWith (error "slave: adding existing states") (getStates' "SReqAddStates") newStates
            send SRespAddStates
            loop (SSInitialized states')
          SReqRemoveStates slaveRequesting stateIdsToDelete -> do
            let states = getStates' "SReqRemoveStates"
            let states' = foldl' (flip HMS.delete) states stateIdsToDelete
            let statesToSend = HMS.fromList
                  [(stateId, states HMS.! stateId) | stateId <- HS.toList stateIdsToDelete]
            send (SRespRemoveStates slaveRequesting statesToSend)
            loop (SSInitialized states')
          SReqUpdate context inputs -> do
            let states0 = getStates' "SReqBroadcast"
            results <- forM (HMS.toList inputs) $ \(oldStateId, innerInputs) ->
              if null innerInputs then return Nothing else Just <$> do
                let state = case HMS.lookup oldStateId states0 of
                      Nothing -> error (printf "slave: Could not find state %d (SReqBroadcast)" oldStateId)
                      Just state0 -> state0
                fmap ((oldStateId, ) . HMS.fromList) $ forM (HMS.toList innerInputs) $ \(newStateId, input) ->
                  fmap (newStateId, ) $ saUpdate context input state
            let resultsMap = HMS.fromList (catMaybes results)
            let states = foldMap (fmap fst) resultsMap
            let outputs = fmap (fmap snd) resultsMap
            void (evaluate (force states))
            send (SRespUpdate outputs)
            loop (SSInitialized states)
          SReqGetStates -> do
            let states = getStates' "SReqGetStates"
            send (SRespGetStates states)
            loop (SSInitialized states)
      in loop
