{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE KindSignatures #-}
{-|
Module: Distributed.Stateful.Master
Description: Master nodes for a distributed stateful computation.

A master coordinates a diatributed stateful computation.

All functions that run or act on a master need a 'MasterHandle' in
order to do so.  'MasterHandle's are created via 'initMaster'.
-}

module Distributed.Stateful.Master
    ( -- * Configuration and creation of master nodes
      MasterArgs(..)
    , defaultMasterArgs
    , Update
    , MasterHandle
    , initMaster
    , DoProfiling (..)
    , StateId , SlaveId
    , StatefulConn(..)
      -- * Operations on master nodes
    , closeMaster
    , update
    , resetStates
    , getStateIds
    , getStates
    , addSlaveConnection
    , getNumSlaves
      -- * Types
    , SlaveConn
    , MasterException(..)
      -- * Performance profiling
    , getSlavesProfiling
    , getMasterProfilng
    , getProfiling
    , SlaveProfiling(..), emptySlaveProfiling
    , MasterProfiling(..), emptyMasterProfiling
    , Profiling (..)
    , ProfilingCounter(..)
    ) where

import           ClassyPrelude
import           Control.DeepSeq (NFData)
import           Control.Lens (makeLenses, set, at, _Just, over)
import           Control.Monad.Logger.JSON.Extra (MonadLogger, logWarnJ, logInfoJ, logDebugJ)
import           Control.Monad.Trans.Control (MonadBaseControl)
import qualified Data.HashMap.Strict as HMS
import qualified Data.HashSet as HS
import qualified Data.HashTable.IO as HT
import           Data.List.Split (chunksOf)
import           Data.SimpleSupply
import           Distributed.Stateful.Internal
import           FP.Redis (MonadConnect)
import           Text.Printf (printf)
import qualified Data.Store as S
import qualified Data.Store.Streaming as S

-- | Defines the behaviour of a master.
data MasterArgs m state context input output = MasterArgs
  { maMaxBatchSize :: !(Maybe Int)
    -- ^ The maximum amount of states that will be transferred at once. If 'Nothing', they
    -- will be all transferred at once, and no "rebalancing" will ever happen.
  , maUpdate :: !(Update m state context input output)
    -- ^ This argument will be used if 'update' is invoked when no slaves have
    -- been added yet.
  , maDoProfiling :: !DoProfiling
    -- ^ Specifies whether to perform profiling or not.
  }

defaultMasterArgs :: Update m state context input output -> MasterArgs m state context input output
defaultMasterArgs upd = MasterArgs
  { maMaxBatchSize = Just 5
  , maUpdate = upd
  , maDoProfiling = NoProfiling
  }

-- | Handle to a master.  Can be created using 'initMaster'.
newtype MasterHandle m state context input output =
  MasterHandle {_unMasterHandle :: MVar (MasterHandle_ m state context input output)}

data Slaves m state context input output
  = NoSlavesYet ![(StateId, state)]
  | Slaves !(HMS.HashMap SlaveId (Slave m state context input output))

data MasterHandle_ m state context input output = MasterHandle_
  { mhSlaves :: !(Slaves m state context input output)
  , mhSlaveIdSupply :: !(Supply SlaveId)
  , mhStateIdSupply :: !(Supply StateId)
  , mhArgs :: !(MasterArgs m state context input output)
  , mhProfiling :: !(Maybe (IORef MasterProfiling))
  }

-- | Connection to a slave.  Requests to the slave can be written, and
-- responses read, with this.
type SlaveConn m state context input output =
  StatefulConn m (SlaveReq state context input) (SlaveResp state output)

data Slave m state context input output = Slave
  { slaveConnection :: !(SlaveConn m state context input output)
  , slaveStates :: !(HMS.HashMap StateId ())
  }

-- | Exceptions specific to masters.
data MasterException
  = MasterException Text
  | InputMissingException StateId
  | UnusedInputsException [StateId]
  | NoSlavesConnectedException
  | UnexpectedResponse Text Text
  | ExceptionFromSlave Text
  deriving (Eq, Show, Typeable)

instance Exception MasterException

-- | Create a new 'MasterHandle'.
initMaster ::
     (MonadBaseControl IO m, MonadIO m, MonadLogger m)
  => MasterArgs m state context input output
  -> m (MasterHandle m state context input output)
initMaster ma@MasterArgs{..} = do
  let throwME = throwAndLog . MasterException . pack
  case maMaxBatchSize of
    Just maxBatchSize | maxBatchSize < 1 ->
      throwME (printf "Distribute.master: maMaxBatchSize must be > 0 (got %d)" maxBatchSize)
    _ -> return ()
  slaveIdSupply <- newSupply (SlaveId 0) (\(SlaveId n) -> SlaveId (n + 1))
  stateIdSupply <- newSupply (StateId 0) (\(StateId n) -> StateId (n + 1))
  mmp <- case maDoProfiling of
      DoProfiling -> Just <$> newIORef emptyMasterProfiling
      NoProfiling -> return Nothing
  let mh = MasterHandle_
        { mhSlaves = NoSlavesYet mempty
        , mhSlaveIdSupply = slaveIdSupply
        , mhStateIdSupply = stateIdSupply
        , mhArgs = ma
        , mhProfiling = mmp
        }
  MasterHandle <$> newMVar mh

-- | This will shut down all the slaves.
closeMaster :: forall m state context input output.
     (MonadConnect m, S.Store state, S.Store context, S.Store input, S.Store output)
  => MasterHandle m state context input output
  -> m ()
closeMaster (MasterHandle mhv) =
  withMVar mhv $ \MasterHandle_{..} -> do
    case mhSlaves of
      NoSlavesYet _ -> return ()
      Slaves slaves ->
        -- Ignore all exceptions we might have when quitting the slaves
        forM_ (HMS.toList slaves) $ \(slaveId, slave) -> do
          mbErr <- tryAny $ do
            scEncodeAndWrite (slaveConnection slave) (SReqQuit :: SlaveReq state context input)
            readExpect "closeMaster" (slaveConnection slave) $ \case
              (SRespQuit :: SlaveResp state output) -> return ()
              _ -> Nothing
          case mbErr of
            Left err -> do
              $logWarnJ (printf
                "Error while quitting slave %d (this can happen when the master is closed because of an error): %s" slaveId (show err) :: String)
            Right () -> return ()

{-# INLINE readExpect #-}
readExpect :: forall m state context input output a.
     (MonadIO m, MonadBaseControl IO m, S.Store state, S.Store output)
  => Text
  -> SlaveConn m state context input output
  -> (SlaveResp state output -> Maybe a)
  -> m a
readExpect loc slave f = do
  resp <- scDecodeAndRead slave
  case resp of
    SRespError err -> throwIO (ExceptionFromSlave err)
    _ -> case f resp of
      Nothing -> throwIO (UnexpectedResponse loc (displayResp resp))
      Just x -> return x

{-# INLINE integrateNewStates #-}
integrateNewStates ::
     HMS.HashMap SlaveId (Slave m state context input output)
  -> [(SlaveId, HMS.HashMap StateId ())]
  -> HMS.HashMap SlaveId (Slave m state context input output)
integrateNewStates oldSlaves =
  go ((\sl -> sl{slaveStates = mempty}) <$> oldSlaves)
  where
    go slaves = \case
      [] -> slaves
      (slaveId, states) : newStates -> let
        sl = oldSlaves HMS.! slaveId
        in go (HMS.insert slaveId sl{slaveStates = states} slaves) newStates

{-# INLINE assignInputsToSlaves #-}
assignInputsToSlaves ::
     (MonadThrow m)
  => HMS.HashMap SlaveId (HMS.HashMap StateId ())
  -> HMS.HashMap StateId [(StateId, input)]
  -> m [(SlaveId, [(StateId, [(StateId, input)])])]
assignInputsToSlaves slaveStates inputMap = do
  let go (rs, inputs) (slaveId, states) = do
        -- TODO: Other Map + Set datatypes have functions for doing this
        -- more efficiently / concisely.
        r <- forM (HMS.keys states) $ \k ->
          case HMS.lookup k inputs of
            Nothing -> throwM (InputMissingException k)
            Just input -> return (k, input)
        let inputs' = inputs `HMS.difference` states
        return ((slaveId, r) : rs, inputs')
  (slavesIdsAndInputs, unusedInputs) <- foldM go ([], inputMap) (HMS.toList slaveStates)
  when (not (HMS.null unusedInputs)) $
    throwM (UnusedInputsException (HMS.keys unusedInputs))
  return slavesIdsAndInputs

data UpdateSlaveResp output
  = USRespAddStates ![StateId]
  | USRespRemoveStates !SlaveId [(StateId, ByteString)]
  | USRespUpdate ![(StateId, [(StateId, output)])]
  | USRespInit
  deriving (Eq, Show, Generic)
instance (NFData output) => NFData (UpdateSlaveResp output)

data UpdateSlaveReq input
  = USReqUpdate ![(StateId, [(StateId, input)])]
  | USReqAddStates ![(StateId, ByteString)]
  | USReqRemoveStates !SlaveId !(HS.HashSet StateId)
  deriving (Eq, Show, Generic)
instance (NFData input) => NFData (UpdateSlaveReq input)

data UpdateSlaveStep input output = UpdateSlaveStep
  { ussReqs :: ![(SlaveId, UpdateSlaveReq input)]
  , ussSlavesStatus :: !(HMS.HashMap SlaveId SlaveStatus)
  , ussOutputs :: ![(StateId, [(StateId, output)])]
  }

type SlavesStatus = HMS.HashMap SlaveId SlaveStatus

data SlaveStatus = SlaveStatus
  { _ssWaitingResps :: !Int
  , _ssRemainingStates :: ![StateId]
  , _ssWaitingForStates :: !Bool
  }
makeLenses ''SlaveStatus

{-# INLINE updateSlavesStep #-}
updateSlavesStep :: forall m input output.
     (MonadThrow m, MonadLogger m, MonadIO m)
  => Maybe (IORef MasterProfiling)
  -> Int -- ^ Max batch size
  -> HMS.HashMap StateId [(StateId, input)]  -- we need lookup here, so this should be a 'HashMap'
  -> SlaveId
  -> UpdateSlaveResp output
  -> SlavesStatus
  -> m (UpdateSlaveStep input output)
updateSlavesStep mp maxBatchSize inputs respSlaveId resp statuses0 = withProfiling mp mpUpdateSlavesStep $ do
  let statuses1 = over (at respSlaveId . _Just . ssWaitingResps) (\c -> c - 1) statuses0
  (statuses2, requests, outputs) <- case resp of
    USRespInit -> do
      addOutputs_ <$> findSomethingToUpdate respSlaveId statuses1
    USRespUpdate outputs -> do
      addOutputs outputs <$> findSomethingToUpdate respSlaveId statuses1
    USRespAddStates statesIds -> do
      let statuses' =
            over (at respSlaveId . _Just) (over ssRemainingStates (++ statesIds) . set ssWaitingForStates False) $
            statuses1
      addOutputs_ <$> findSomethingToUpdate respSlaveId statuses'
    USRespRemoveStates requestingSlaveId states -> do
      return (addOutputs_ (statuses1, [(requestingSlaveId, USReqAddStates states)]))
  let statuses3 = foldl' (\statuses (slaveId, _) -> over (at slaveId . _Just . ssWaitingResps) (+ 1) statuses) statuses2 requests
  return UpdateSlaveStep
    { ussReqs = requests
    , ussSlavesStatus = statuses3
    , ussOutputs = outputs
    }
  where
    addOutputs outputs (statuses, requests) = (statuses, requests, outputs)
    addOutputs_ = addOutputs mempty

    {-# INLINE findSomethingToUpdate #-}
    findSomethingToUpdate ::
         SlaveId
      -> SlavesStatus
      -> m (SlavesStatus, [(SlaveId, UpdateSlaveReq input)])
    findSomethingToUpdate slaveId uss = do
      -- If we have some states in ssRemaining, just update those. Otherwise,
      -- try to steal from another slave.
      let ss = uss HMS.! slaveId
      let (toUpdateInputs, remaining) = takeEnoughStatesToUpdate (_ssRemainingStates ss)
      if null toUpdateInputs
        then do -- Steal slaves
          let mbStolen = stealSlavesFromSomebody slaveId uss
          case mbStolen of
            Nothing -> do -- We're done for this slave.
              $logInfoJ ("Tried to get something to do for slave " ++ tshow (unSlaveId slaveId) ++ ", but couldn't find anything")
              return (uss, [])
            Just (uss', stolenFrom, stolenStates) -> do -- Request stolen slaves
              $logInfoJ ("Stealing " ++ tshow (HS.size stolenStates) ++ " states from slave " ++ tshow (unSlaveId stolenFrom) ++ " for slave " ++ tshow (unSlaveId slaveId))
              return (uss', [(stolenFrom, USReqRemoveStates slaveId stolenStates)])
        else do -- Send update command
          let uss' = set (at slaveId . _Just . ssRemainingStates) remaining uss
          return (uss', [(slaveId, USReqUpdate toUpdateInputs)])

    {-# INLINE takeEnoughStatesToUpdate #-}
    takeEnoughStatesToUpdate ::
         [StateId]
      -> ([(StateId, [(StateId, input)])], [StateId])
    takeEnoughStatesToUpdate remaining00 = go remaining00 0
      where
        go remaining0 grabbed = if grabbed >= maxBatchSize
          then ([], remaining0)
          else case remaining0 of
            [] -> ([], remaining0)
            stateId : remaining -> let
              inps = inputs HMS.! stateId
              in first ((stateId, inps) :) (go remaining (grabbed + length inps))

    {-# INLINE stealSlavesFromSomebody #-}
    stealSlavesFromSomebody ::
         SlaveId -- Requesting the slaves
      -> SlavesStatus
      -> Maybe (SlavesStatus, SlaveId, HS.HashSet StateId)
    stealSlavesFromSomebody requestingSlaveId uss = do
      let goodCandidate (slaveId, ss) = do
            guard (slaveId /= requestingSlaveId)
            guard (not (null (_ssRemainingStates ss)))
            let (toTransfer, remaining) = takeEnoughStatesToUpdate (_ssRemainingStates ss)
            return (slaveId, map fst toTransfer, sum (map (length . snd) toTransfer), remaining)
      let candidates :: [(SlaveId, [StateId], Int, [StateId])]
          candidates = mapMaybe goodCandidate (HMS.toList uss)
      guard (not (null candidates))
      -- Pick candidate with highest number of states to steal states from
      let (candidateSlaveId, statesToBeTransferred, _, remainingStates) =
            maximumByEx (comparing (\(_, _, x, _) -> x)) candidates
      let uss' =
            set (at candidateSlaveId . _Just . ssRemainingStates) remainingStates $
            set (at requestingSlaveId . _Just . ssWaitingForStates) True uss
      return (uss', candidateSlaveId, HS.fromList statesToBeTransferred)

data SlaveWithConnection m state context input output =
    SlaveWithConnection
        { swcSlaveId :: !SlaveId
        , swcConn :: !(SlaveConn m state context input output)
        , swcCont :: !(IORef (Maybe (ByteString -> m (S.PeekMessage m (SlaveResp state output)))))
        }

data MasterLoopState output = MasterLoopState
  { _mlsSlavesStatus :: !SlavesStatus
  , mlsOutputs :: !(HMS.HashMap SlaveId [(StateId, [(StateId, output)])])
  , mlsNumActiveSlaves :: !Int
  }

{-# INLINE updateSlaves #-}
updateSlaves :: forall state context input output m.
     (MonadConnect m, NFData input, NFData output
     , S.Store state, S.Store context, S.Store input, S.Store output)
  => Maybe (IORef MasterProfiling)
  -> Int -- ^ Max batch size
  -> HMS.HashMap SlaveId (Slave m state context input output)
  -- ^ Slaves connections
  -> context
  -- ^ Context to the computation
  -> HMS.HashMap StateId [(StateId, input)]
  -- ^ Inputs to the computation
  -> m [(SlaveId, [(StateId, [(StateId, output)])])]
updateSlaves mp maxBatchSize slaves context inputMap = withProfiling mp mpUpdateSlaves $ do
  let slaveStatus0 slave = SlaveStatus
        { _ssWaitingResps = 1 -- The init
        , _ssRemainingStates = HMS.keys (slaveStates slave)
        , _ssWaitingForStates = False
        }
      statuses0 = slaveStatus0 <$> slaves
  statuses1 <- foldM sendRespInit statuses0 (HMS.keys slaves)
  swcList <- forM (HMS.toList slaves) $ \(slaveId, slave) -> do
      cont <- newIORef Nothing
      return (SlaveWithConnection slaveId (slaveConnection slave) cont)
  masterLoopStateRef :: IORef (MasterLoopState output) <- newIORef (MasterLoopState statuses1 HMS.empty (HMS.size slaves))
  masterLoopLock :: MVar () <- newMVar ()
  masterLoopDone :: MVar [(SlaveId, [(StateId, [(StateId, output)])])] <- newEmptyMVar -- once the masterLoop is finished, this MVar will contain the result.  Before that, it is empty.
  let registerAll :: [SlaveWithConnection m state context input output] -> m a -> m a
      registerAll [] cont = cont
      registerAll (swc@SlaveWithConnection{..} : xs) cont =
        registerAll xs $
          scRegisterCanRead swcConn (withMVar masterLoopLock (\() -> masterLoopEntry swc masterLoopStateRef masterLoopDone)) cont
          -- scRegisterCanRead swcConn (masterLoopEntry swc masterLoopStateRef masterLoopDone) cont
  registerAll swcList (takeMVar masterLoopDone)
  where
    masterLoopEntry ::
         SlaveWithConnection m state context input output
      -> IORef (MasterLoopState output)
      -> MVar [(SlaveId, [(StateId, [(StateId, output)])])]
      -> m ()
    masterLoopEntry swc masterLoopStateRef masterLoopDone = do
      mls <- readIORef masterLoopStateRef
      mls' <- withProfiling mp mpMasterLoop $ masterLoop swc mls
      writeIORef masterLoopStateRef mls'
      when (mlsNumActiveSlaves mls' == 0) $
      -- unless done $
        putMVar masterLoopDone (HMS.toList (mlsOutputs mls'))

    masterLoop ::
         SlaveWithConnection m state context input output
      -> MasterLoopState output
      -> m (MasterLoopState output)
    masterLoop swc@SlaveWithConnection{..} mls@(MasterLoopState statuses outputs nActiveSlaves) = if nActiveSlaves == 0
      then return mls
      else do
        cont0 <- readIORef swcCont >>= \case
            Nothing -> doPeek swcConn
            Just cont0 -> return (S.NeedMoreInput cont0)
        case cont0 of
            S.Done (S.Message _resp) -> error "updateSlaves.masterLoop: unexpected Done continuation."
            S.NeedMoreInput cont -> do
                bs <- withProfiling mp mpReceive $ scRead swcConn
                cont bs >>= \case
                    (S.NeedMoreInput cont') -> do
                        writeIORef swcCont (Just cont')
                        return mls
                    S.Done (S.Message resp) -> withProfiling mp mpHandleResponse $
                        handleResponse statuses outputs nActiveSlaves swc resp

    handleResponse statuses outputs nActiveSlaves swc@SlaveWithConnection{..} resp = do
        resp' <- case resp of
            SRespInit -> throwIO (UnexpectedResponse "updateSlaves" (displayResp resp))
            SRespResetState -> throwIO (UnexpectedResponse "updateSlaves" (displayResp resp))
            SRespGetStates _ -> throwIO (UnexpectedResponse "updateSlaves" (displayResp resp))
            SRespQuit -> throwIO (UnexpectedResponse "updateSlaves" (displayResp resp))
            SRespError err -> throwIO (ExceptionFromSlave err)
            SRespAddStates addedStates -> return (USRespAddStates addedStates)
            SRespRemoveStates requestingSlaveId removedStates ->
              return (USRespRemoveStates requestingSlaveId removedStates)
            SRespUpdate outputs_ -> return (USRespUpdate outputs_)
            SRespGetProfile _ -> throwIO (UnexpectedResponse "updateSlaves" (displayResp resp))
        UpdateSlaveStep requests statuses' newOutputs <- updateSlavesStep mp maxBatchSize inputMap swcSlaveId resp' statuses
        forM_ requests sendRequest
        let status = statuses' HMS.! swcSlaveId
            outputs' = HMS.insertWith (<>) swcSlaveId newOutputs outputs
        nActiveSlaves' <- if _ssWaitingResps status == 0 && not (_ssWaitingForStates status)
                            then do
                                $logInfoJ ("Slave " ++ tshow (unSlaveId swcSlaveId) ++ " is done")
                                return (nActiveSlaves - 1)
                         else return nActiveSlaves
        -- see if we can decode another message from the chunk we read
        doPeek swcConn >>= \case
            S.Done (S.Message newResp) -> handleResponse statuses' outputs' nActiveSlaves' swc newResp
            S.NeedMoreInput cont' -> do
                writeIORef swcCont (Just cont')
                let mls = MasterLoopState statuses' outputs' nActiveSlaves'
                return mls

    doPeek conn = withProfiling mp mpDecode $ S.peekMessage (scByteBuffer conn)
        `catch` (\ ex@(S.PeekException _ _) -> throwIO . StatefulConnDecodeFailure . ("updateSlaves: " ++) . show $ ex)

    sendRespInit statuses slaveId = do
        UpdateSlaveStep{..} <- updateSlavesStep mp maxBatchSize inputMap slaveId USRespInit statuses
        forM_ ussReqs sendRequest
        return ussSlavesStatus

    sendRequest (slaveId, req) = do
        let req' = case req of
                      USReqUpdate inputs -> SReqUpdate context inputs
                      USReqRemoveStates x y -> SReqRemoveStates x y
                      USReqAddStates y -> SReqAddStates y
            conn = slaveConnection (slaves HMS.! slaveId)
        $logDebugJ ("Sending request to slave " ++ tshow (unSlaveId slaveId) ++ ": " ++ displayReq req')
        withProfiling mp mpSend $ scEncodeAndWrite conn req'

-- | Send an update request to all the slaves. This will cause each of
-- the slaves to apply 'Distributed.Stateful.Slave.saUpdate' to each of its states. The outputs of
-- these invocations are returned in a 'HashMap'.
--
-- It rebalances the states among the slaves during this process. This
-- way, if some of the slaves finish early, they can help the others
-- out, by moving their workitems elsewhere.
--
-- NOTE: if this throws an exception, then `MasterHandle` may be in an
-- inconsistent state, and the whole computation should be aborted.
{-# INLINE update #-}
update :: forall state context input output m.
     (MonadConnect m, NFData input, NFData output, NFData state, NFData context
     , S.Store context, S.Store input, S.Store state, S.Store output)
  => MasterHandle m state context input output
  -> context
  -> HMS.HashMap StateId [input]
  -> m (HMS.HashMap StateId (HMS.HashMap StateId output))
update (MasterHandle mv) context inputs0 = modifyMVar mv $ \mh -> do
  -- TODO add checks for well-formedness of inputs'
  -- Give state ids to each of the inputs, which will be used to label the
  -- state resulting from invoking saUpdate with that input.
  let sortedInputs = sortBy (comparing fst) (HMS.toList inputs0)
  inputList :: [(StateId, [(StateId, input)])] <-
    withSupplyM (mhStateIdSupply mh) $
    forM sortedInputs $ \(stateId, inps) ->
      (stateId,) <$> mapM (\input -> (, input) <$> askSupplyM) inps
  case mhSlaves mh of
    NoSlavesYet states -> do
      $logWarnJ ("Executing update without any slaves" :: Text)
      stateMap <- liftIO $ HT.fromList states
      outputs <- statefulUpdate Nothing (maUpdate (mhArgs mh)) stateMap context inputList
      statesList' <- liftIO $ HT.toList stateMap
      let mh' = mh{ mhSlaves = NoSlavesYet statesList' }
      return (mh', fmap HMS.fromList . HMS.fromList $ outputs)
    Slaves slaves -> do
      $logInfoJ ("Executing update with " ++ tshow (HMS.size slaves) ++ " slaves")
      let inputMap = HMS.fromList inputList
      -- Update the slave states.
      outputs :: [(SlaveId, [(StateId, [(StateId, output)])])] <-
        case maMaxBatchSize (mhArgs mh) of
          Nothing -> do
            slaveIdsAndInputs <- either throwAndLog return $
              assignInputsToSlaves (slaveStates <$> slaves) inputMap
            forM_ slaveIdsAndInputs $ \(slaveId, inps) -> do
              scEncodeAndWrite
                (slaveConnection (slaves HMS.! slaveId))
                (SReqUpdate context inps :: SlaveReq state context input)
            forM slaveIdsAndInputs $ \(slaveId, _) ->
              liftM (slaveId,) $ readExpect "update (1)" (slaveConnection (slaves HMS.! slaveId)) $ \case
                (SRespUpdate outputs :: SlaveResp state output) -> Just outputs
                _ -> Nothing
          Just maxBatchSize -> withProfiling (mhProfiling mh) mpTotalUpdate $
              updateSlaves (mhProfiling mh) maxBatchSize slaves context inputMap
      let mh' = mh
            { mhSlaves = Slaves $
                integrateNewStates slaves (second (fmap (const ()) . HMS.fromList . mconcat . map snd) <$> outputs)
            }
      return (mh', fmap HMS.fromList . HMS.fromList $ foldMap snd outputs)

-- | Adds a connection to a slave.
addSlaveConnection ::
     (MonadConnect m, S.Store state, S.Store context, S.Store input, S.Store output)
  => MasterHandle m state context input output
  -> SlaveConn m state context input output
  -> m ()
addSlaveConnection (MasterHandle mhv) conn = modifyMVar_ mhv $ \mh -> do
  slaveId <- askSupply (mhSlaveIdSupply mh)
  initSlave (maDoProfiling . mhArgs $ mh) conn
  case mhSlaves mh of
    NoSlavesYet states -> do
      let slaves = HMS.fromList [(slaveId, Slave conn (const () <$> HMS.fromList states))]
      sendStates [(conn, states)]
      return mh{mhSlaves = Slaves slaves}
    Slaves slaves -> do
      return mh{mhSlaves = Slaves (HMS.insert slaveId (Slave conn mempty) slaves)}

initSlave ::
       (MonadConnect m, S.Store state, S.Store context, S.Store input, S.Store output)
    => DoProfiling
    -> SlaveConn m state context input output
    -> m ()
initSlave doProfiling conn = do
    scEncodeAndWrite conn (SReqInit doProfiling)
    readExpect "initSlave" conn $ \case
        SRespInit -> Just ()
        _ -> Nothing

sendStates :: forall m state context input output.
       (Monad m, MonadBaseControl IO m, MonadIO m, S.Store state, S.Store context, S.Store input, S.Store output)
    => [(SlaveConn m state context input output, [(StateId, state)])]
    -> m ()
sendStates slavesWithStates = do
  -- Send states
  forM_ slavesWithStates $ \(conn, states) -> do
    scEncodeAndWrite conn (SReqResetState states :: SlaveReq state context input)
  forM_ slavesWithStates $ \(conn, _) -> do
    readExpect "sendStates" conn $ \case
      (SRespResetState :: SlaveResp state output) -> Just ()
      _ -> Nothing

-- | This sets the states stored in the slaves. It distributes the
-- states among the currently connected slaves.
--
-- If no slaves are connected, this will throw a
-- 'NoSlavesConnectedException'.
{-# INLINE resetStates #-}
resetStates :: forall state context input output m.
     (MonadBaseControl IO m, MonadIO m, MonadLogger m
     , S.Store state, S.Store context, S.Store input, S.Store output)
  => MasterHandle m state context input output
  -> [state]
  -> m (HMS.HashMap StateId state)
resetStates (MasterHandle mhv) states0 = modifyMVar mhv $ \mh -> do
  -- Get a list of StateIds for states
  allStates <- withSupplyM (mhStateIdSupply mh) $ mapM (\state -> (, state) <$> askSupplyM) states0
  case mhSlaves mh of
    NoSlavesYet _oldStates -> do
      return (mh{mhSlaves = NoSlavesYet allStates}, HMS.fromList allStates)
    Slaves slaves -> do
      -- Divide up the states among the slaves
      when (HMS.null slaves) $
        throwAndLog NoSlavesConnectedException
      let numStatesPerSlave =
            let (chunkSize, leftover) = length states0 `quotRem` length slaves
            in if leftover == 0 then chunkSize else chunkSize + 1
      let slavesStates :: HMS.HashMap SlaveId [(StateId, state)]
          slavesStates = HMS.fromList $ zip (HMS.keys slaves) $
            -- Note that some slaves might be initially empty. That's fine and inevitable.
            chunksOf numStatesPerSlave allStates ++ repeat []
      let slaveStatesId :: [(SlaveId, HMS.HashMap StateId ())]
          slaveStatesId = map (second (HMS.fromList . map (second (const ())))) (HMS.toList slavesStates)
      let slaves' = integrateNewStates slaves slaveStatesId
      sendStates
        [ (slaveConnection slave, slavesStates HMS.! slaveId)
        | (slaveId, slave) <- HMS.toList slaves
        ]
      return (mh{mhSlaves = Slaves slaves'}, HMS.fromList allStates)

-- | Get all the 'StateId's of states of the computation handled by a
-- master.
getStateIds ::
     (MonadConnect m)
  => MasterHandle m state context input output
  -> m (HS.HashSet StateId)
getStateIds (MasterHandle mhv) = do
  mh <- readMVar mhv
  return $ case mhSlaves mh of
    NoSlavesYet states -> HS.fromList (fst <$> states)
    Slaves slaves -> HS.fromList (HMS.keys (fold (fmap slaveStates slaves)))

-- | Fetches current states stored in the slaves.
{-# INLINE getStates #-}
getStates :: forall m state context input output.
     (MonadConnect m, S.Store state, S.Store context, S.Store input, S.Store output)
  => MasterHandle m state context input output
  -> m (HMS.HashMap StateId state)
getStates (MasterHandle mhv) = withMVar mhv $ \mh -> do
  -- Send states
  case mhSlaves mh of
    NoSlavesYet states -> return $ HMS.fromList states
    Slaves slaves -> do
      forM_ slaves (\slave_ -> scEncodeAndWrite (slaveConnection slave_) (SReqGetStates :: SlaveReq state context input))
      responses <- forM (HMS.elems slaves) $ \slave_ -> do
        readExpect "getStates" (slaveConnection slave_) $ \case
          (SRespGetStates states :: SlaveResp state output) -> Just states
          _ -> Nothing
      return (HMS.fromList $ mconcat responses)

-- | Retrieves performance profiling data for each slave.
getSlavesProfiling :: forall m state context input output.
       (MonadConnect m, S.Store state, S.Store context, S.Store input, S.Store output)
    => MasterHandle m state context input output
    -> m (HMS.HashMap SlaveId (Maybe SlaveProfiling))
getSlavesProfiling (MasterHandle mhv) = withMVar mhv $ \mh ->
    case mhSlaves mh of
        NoSlavesYet _states -> return HMS.empty
        Slaves slaves -> do
            forM_ slaves $ \slave -> scEncodeAndWrite (slaveConnection slave) (SReqGetProfile :: SlaveReq state context input)
            forM slaves $ \slave ->
                readExpect "getSlavesProfiling" (slaveConnection slave) $ \case
                    (SRespGetProfile sp :: SlaveResp state output) -> return sp
                    _ -> Nothing

-- | Retrieves performance profiling data for the master.
getMasterProfilng ::
       MonadConnect m
    => MasterHandle m state context input output
    -> m (Maybe MasterProfiling)
getMasterProfilng (MasterHandle mhv) = readMVar mhv >>= \mh ->
    case mhProfiling mh of
        Nothing -> return Nothing
        Just mp -> Just <$> readIORef mp

-- | Retrieves performance profiling data for the whole computation, master and slaves.
--
-- The profiling data for the slaves is summed.
getProfiling :: forall m state context input output.
       (MonadConnect m, S.Store state, S.Store context, S.Store input, S.Store output)
    => MasterHandle m state context input output
    -> m (Maybe Profiling)
getProfiling mhandle@(MasterHandle mhv) = readMVar mhv >>= \mh ->
    case mhProfiling mh of
        Nothing -> return Nothing
        Just mpref -> do
            sp <- catMaybes . HMS.elems <$> getSlavesProfiling mhandle >>= \case
                [] -> return Nothing
                sp:sps -> return . Just $ foldl' (<>) sp sps
            mp <- readIORef mpref
            return . Just $ Profiling mp sp

-- | Get the number of slaves connected to a master.
getNumSlaves ::
     (MonadConnect m)
  => MasterHandle m state context input output
  -> m Int
getNumSlaves (MasterHandle mhv) = do
  mh <- readMVar mhv
  return $ case mhSlaves mh of
    NoSlavesYet _ -> 0
    Slaves slaves -> HMS.size slaves
