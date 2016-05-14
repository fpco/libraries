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
  , StatefulConn(..)
  , runSlave
  ) where

import           ClassyPrelude
import           Control.DeepSeq (force, NFData)
import           Control.Exception.Lifted (evaluate)
import           Control.Monad.Logger (logDebugNS)
import qualified Data.HashMap.Strict as HMS
import qualified Data.HashSet as HS
import           Distributed.Stateful.Internal
import           FP.Redis (MonadConnect)
import qualified Data.Serialize as C

-- | Arguments for 'runSlave'.
data SlaveArgs m state context input output = SlaveArgs
  { saUpdate :: !(context -> input -> state -> m (state, output))
    -- ^ Function run on the slave when 'update' is invoked on the
    -- master.
  , saConn :: !(StatefulConn m (SlaveResp state output) (SlaveReq state context input))
  }

data SlaveException
  = AddingExistingStates [StateId]
  | MissingStatesToRemove [StateId]
  | InputStateNotFound StateId
  | DecodeStateError String
  deriving (Eq, Show, Typeable)

instance Exception SlaveException

-- | Runs a stateful slave. Returns when it gets disconnected from the
-- master.
{-# INLINE runSlave #-}
runSlave :: forall state context input output void m.
     (MonadConnect m, NFData state, NFData output, C.Serialize state)
  => SlaveArgs m state context input output
  -> m void
runSlave SlaveArgs{..} = do
    let recv = scRead saConn
    let send = scWrite saConn
    go recv send mempty
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
      eres <- tryAny ((do
        res <- case req of
          SReqResetState states' -> return (SRespResetState, states')
          SReqGetStates -> return (SRespGetStates states, states)
          SReqAddStates newStates0 -> do
            let decodeOrThrow bs = case C.decodeEof bs of
                  Left err -> throw (DecodeStateError err)
                  Right x -> return x
            newStates <- mapM decodeOrThrow newStates0
            let aliased = HMS.keys (HMS.intersection newStates states)
            unless (null aliased) $ throw (AddingExistingStates aliased)
            return (SRespAddStates (HS.fromList (HMS.keys newStates)), HMS.union newStates states)
          SReqRemoveStates requesting stateIdsToDelete -> do
            let eitherLookup sid =
                  case HMS.lookup sid states of
                    Nothing -> Left sid
                    Just x -> Right (sid, x)
            let (missing, toSend) = partitionEithers $ map eitherLookup $ HS.toList stateIdsToDelete
            unless (null missing) $ throw (MissingStatesToRemove missing)
            let states' = foldl' (flip HMS.delete) states stateIdsToDelete
            return (SRespRemoveStates requesting (C.encode <$> HMS.fromList toSend), states')
          SReqUpdate context inputs -> do
            results <- forM (HMS.toList inputs) $ \(oldStateId, innerInputs) -> do
              state <- case HMS.lookup oldStateId states of
                Nothing -> throw (InputStateNotFound oldStateId)
                Just state0 -> return state0
              fmap ((oldStateId, ) . HMS.fromList) $ forM (HMS.toList innerInputs) $ \(newStateId, input) ->
                fmap (newStateId, ) $ saUpdate context input state
            let resultsMap = HMS.fromList results
            let states' = foldMap (fmap fst) resultsMap
            let outputs = fmap (fmap snd) resultsMap
            return (SRespUpdate outputs, states' `HMS.union` (states `HMS.difference` inputs))
        evaluate (force res)) :: m (SlaveResp state output, HMS.HashMap StateId state))
      case eres of
        Right (output, states') -> do
          send output
          debug (displayResp output)
          go recv send states'
        Left (err :: SomeException) -> do
          send (SRespError (pack (show err)))
          throwAndLog err
