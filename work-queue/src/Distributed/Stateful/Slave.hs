{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE FlexibleContexts #-}
{-|
Module: Distributed.Stateful.Slave
Description: Configure and run slaves for a distributed stateful computation.
-}
module Distributed.Stateful.Slave
  ( -- * Configuration and creation of slave nodes
    SlaveArgs(..)
  , Update
  , runSlave
    -- * Stateful Connection
  , StatefulConn(..)
  ) where

import           ClassyPrelude
import           Control.DeepSeq (NFData)
import           Control.Monad.Logger (logDebugNS)
import qualified Data.HashMap.Strict as HMS
import qualified Data.HashSet as HS
import           Distributed.Stateful.Internal
import           FP.Redis (MonadConnect)
import qualified Data.Store as S

-- | Arguments for 'runSlave'.
data SlaveArgs m state context input output = SlaveArgs
  { saUpdate :: !(Update m state context input output)
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

-- | Runs a stateful slave. Returns when the master sends the "quit" command.
{-# INLINE runSlave #-}
runSlave :: forall state context input output m.
     (MonadConnect m, NFData state, NFData output, S.Store state)
  => SlaveArgs m state context input output
  -> m ()
runSlave SlaveArgs{..} = do
    let recv = scRead saConn
    let send = scWrite saConn
        -- We're only catching 'SlaveException's here, since they
        -- indicate that something was wrong about the request, and
        -- should be sent back to the master.
        handler :: SlaveException -> m ()
        handler err = do
            send (SRespError (pack (show err)))
            throwAndLog err
    go recv send mempty `catch` handler
  where
    throw = throwAndLog
    debug msg = logDebugNS "Distributed.Stateful.Slave" msg
    go ::
         m (SlaveReq state context input)
      -> (SlaveResp state output -> m ())
      -> (HMS.HashMap StateId state)
      -> m ()
    go recv send states = do
      req <- recv
      debug (displayReq req)
      -- WARNING: All exceptions thrown here should be of type
      -- 'SlaveException', as only those will be catched.
      (output, mbStates) <- case req of
          SReqResetState states' -> return (SRespResetState, (Just states'))
          SReqGetStates -> return (SRespGetStates states, (Just states))
          SReqAddStates newStates0 -> do
            let decodeOrThrow bs = case S.decode bs of
                  Left err -> throw (DecodeStateError (show err))
                  Right x -> return x
            newStates <- mapM decodeOrThrow newStates0
            let aliased = HMS.keys (HMS.intersection newStates states)
            unless (null aliased) $ throw (AddingExistingStates aliased)
            return (SRespAddStates (HS.fromList (HMS.keys newStates)), Just (HMS.union newStates states))
          SReqRemoveStates requesting stateIdsToDelete -> do
            let eitherLookup sid =
                  case HMS.lookup sid states of
                    Nothing -> Left sid
                    Just x -> Right (sid, x)
            let (missing, toSend) = partitionEithers $ map eitherLookup $ HS.toList stateIdsToDelete
            unless (null missing) $ throw (MissingStatesToRemove missing)
            let states' = foldl' (flip HMS.delete) states stateIdsToDelete
            return (SRespRemoveStates requesting (S.encode <$> HMS.fromList toSend), Just states')
          SReqUpdate context inputs -> do
            (states', outputs) <- statefulUpdate saUpdate states context inputs
            return (SRespUpdate outputs, Just states')
          SReqQuit -> do
            return (SRespQuit, Nothing)
      send output
      debug (displayResp output)
      case mbStates of
            Nothing -> return ()
            Just states' -> go recv send states'
