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
  | UnexpectedRequest Text
  deriving (Eq, Show, Typeable)

instance Exception SlaveException

-- | Runs a stateful slave. Returns when the master sends the "quit" command.
{-# INLINE runSlave #-}
runSlave :: forall state context input output m.
     (MonadConnect m, NFData state, NFData output, S.Store state, S.Store context, S.Store output, S.Store input, NFData input, NFData context)
  => SlaveArgs m state context input output
  -> m ()
runSlave SlaveArgs{..} = do
    states <- liftIO HT.new
    let recv = scDecodeAndRead saConn
    let send = scEncodeAndWrite saConn
        -- We're only catching 'SlaveException's here, since they
        -- indicate that something was wrong about the request, and
        -- should be sent back to the master.
        handler :: SlaveException -> m a
        handler err = do
            send (SRespError (pack (show err)))
            throwAndLog err
    sp <- init recv send `catch` handler
    go recv send states sp `catch` handler
  where
    throw = throwAndLog
    debug = logDebugNSJ "Distributed.Stateful.Slave"
    init recv send = do
      req <- recv
      debug (displayReq req)
      sp <- case req of
          SReqInit NoProfiling -> return Nothing
          SReqInit DoProfiling -> Just <$> newIORef emptySlaveProfiling
          _ -> throwIO (UnexpectedRequest (displayReq req))
      let output = SRespInit
      withProfiling sp spSend $ send output
      debug (displayResp output)
      return sp
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
      (output, mbStates) <- withProfiling sp spWork $ case req of
          SReqInit doProfiling -> throwIO (UnexpectedRequest (displayReq req))
          SReqResetState states' -> do
              statesMap' <- liftIO . withProfiling sp spHTFromList $ HT.fromList states'
              return (SRespResetState, Just statesMap')
          SReqGetStates -> do
              statesList <- liftIO . withProfiling sp spHTToList $ HT.toList states
              return (SRespGetStates statesList, Just states)
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
            return (SRespAddStates (fst <$> newStates), Just states)
          SReqRemoveStates requesting stateIdsToDelete -> do
            let eitherLookup sid = (liftIO . withProfiling sp spHTLookups $ HT.lookup states sid) >>= \case
                    Nothing -> return $ Left sid
                    Just x -> return $ Right (sid, x)
            (missing, toSend) <- partitionEithers <$> mapM eitherLookup (HS.toList stateIdsToDelete)
            unless (null missing) $ throw (MissingStatesToRemove missing)
            withProfiling sp spHTDeletes $ forM_ stateIdsToDelete (liftIO . HT.delete states)
            return (SRespRemoveStates requesting (second S.encode <$> toSend), Just states)
          SReqUpdate context inputs -> do
            outputs <- statefulUpdate sp saUpdate states context inputs
            return (SRespUpdate outputs, Just states)
          SReqGetProfile -> do
            slaveProfile <- case sp of
                Just sp' -> Just <$> readIORef sp'
                Nothing -> return Nothing
            return (SRespGetProfile slaveProfile, Just states)
          SReqQuit -> do
            return (SRespQuit, Nothing)
      withProfiling sp spSend $ send output
      debug (displayResp output)
      case mbStates of
            Nothing -> return ()
            Just states' -> go recv send states' sp
