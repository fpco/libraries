{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE LambdaCase #-}
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
import           Control.Monad.Logger.JSON.Extra (logDebugNSJ)
import qualified Data.HashSet as HS
import qualified Data.HashTable.IO as HT
import qualified Data.Store as S
import           Distributed.Stateful.Internal
import           FP.Redis (MonadConnect)

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
     (MonadConnect m, NFData state, NFData output, S.Store state, NFData input, NFData context)
  => SlaveArgs m state context input output
  -> m ()
runSlave SlaveArgs{..} = do
    states <- liftIO HT.new
    let recv = scRead saConn
    let send = scWrite saConn
        -- We're only catching 'SlaveException's here, since they
        -- indicate that something was wrong about the request, and
        -- should be sent back to the master.
        handler :: SlaveException -> m ()
        handler err = do
            send (SRespError (pack (show err)))
            throwAndLog err
    go recv send states Nothing `catch` handler
  where
    throw = throwAndLog
    debug msg = logDebugNSJ "Distributed.Stateful.Slave" msg
    go ::
         m (SlaveReq state context input)
      -> (SlaveResp state output -> m ())
      -> HashTable StateId state
      -> Maybe (IORef SlaveProfiling)
      -> m ()
    go recv send states sp = do
      req <- withProfiling sp spReceive recv
      debug (displayReq req)
      -- WARNING: All exceptions thrown here should be of type
      -- 'SlaveException', as only those will be catched.
      (output, mbStates, sp') <- withProfiling sp spWork $ case req of
          SReqInit doProfiling -> do
              sp' <- case doProfiling of
                  NoProfiling -> return Nothing
                  DoProfiling -> Just <$> newIORef emptySlaveProfiling
              return (SRespInit, Just states, sp')
          SReqResetState states' -> do
              statesMap' <- liftIO . withProfiling sp spHTFromList $ HT.fromList states'
              return (SRespResetState, Just statesMap', sp)
          SReqGetStates -> do
              statesList <- liftIO . withProfiling sp spHTToList $ HT.toList states
              return (SRespGetStates statesList, Just states, sp)
          SReqAddStates newStates0 -> do
            let decodeOrThrow bs = case S.decode bs of
                  Left err -> throw (DecodeStateError (show err))
                  Right x -> return x
            newStates <- forM newStates0 $ \(sid,bs) -> do
                bs' <- decodeOrThrow bs
                return (sid,bs')
            aliased <- filter snd <$> forM newStates
                (\(sid,_) -> do
                        mVal <- liftIO . withProfiling sp spHTLookups $ states `HT.lookup` sid
                        return (sid, isJust mVal))
            unless (null aliased) $ throw (AddingExistingStates $ map fst aliased)
            forM_ newStates $ \(sid, state) -> liftIO . withProfiling sp spHTInserts $ HT.insert states sid state
            return (SRespAddStates (fst <$> newStates), Just states, sp)
          SReqRemoveStates requesting stateIdsToDelete -> do
            let eitherLookup sid = (liftIO . withProfiling sp spHTLookups $ HT.lookup states sid) >>= \case
                    Nothing -> return $ Left sid
                    Just x -> return $ Right (sid, x)
            (missing, toSend) <- partitionEithers <$> mapM eitherLookup (HS.toList stateIdsToDelete)
            unless (null missing) $ throw (MissingStatesToRemove missing)
            withProfiling sp spHTDeletes $ forM_ stateIdsToDelete (liftIO . HT.delete states)
            return (SRespRemoveStates requesting (second S.encode <$> toSend), Just states, sp)
          SReqUpdate context inputs -> do
            outputs <- statefulUpdate sp saUpdate states context inputs
            return (SRespUpdate outputs, Just states, sp)
          SReqGetProfile -> do
            slaveProfile <- case sp of
                Just sp' -> Just <$> readIORef sp'
                Nothing -> return Nothing
            return (SRespGetProfile slaveProfile, Just states, sp)
          SReqQuit -> do
            return (SRespQuit, Nothing, sp)
      withProfiling sp spSend $ send output
      debug (displayResp output)
      case mbStates of
            Nothing -> return ()
            Just states' -> go recv send states' sp'
