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
    , Update
    , MasterHandle
    , initMaster
      -- * Operations on master nodes
    , closeMaster
    , update
    , resetStates
    , getStateIds
    , getStates
    , getSlavesProfiling
    , getMasterProfiling
    , getMasterProfilingIORef
    , addSlaveConnection
    , getNumSlaves
      -- * Types
    , SlaveConn
    , MasterException(..)
      -- those are re-exported from Internal.
    , StateId
    , SlaveId
    , StatefulConn(..)
    , SlaveProfiling(..), emptySlaveProfiling
    , MasterProfiling(..)
    ) where

import           ClassyPrelude hiding (Chan, readChan, writeChan)
import           Control.DeepSeq (NFData)
import           Control.Lens (makeLenses, set, at, _Just, over)
import           Control.Monad.Logger.JSON.Extra (MonadLogger, logWarnJ, logInfoJ, logDebugJ)
import           Control.Monad.STM (orElse, retry)
import           Control.Monad.Trans.Control (MonadBaseControl)
import qualified Data.HashMap.Strict as HMS
import qualified Data.HashSet as HS
import qualified Data.HashTable.IO as HT
import           Data.List.Split (chunksOf)
import           Data.SimpleSupply
import           Distributed.Stateful.Internal
import           FP.Redis (MonadConnect)
import qualified System.Random as R
import           Text.Printf (printf)

-- | Defines the behaviour of a master.
data MasterArgs m state context input output = MasterArgs
  { maMaxBatchSize :: !(Maybe Int)
    -- ^ The maximum amount of states that will be transferred at once. If 'Nothing', they
    -- will be all transferred at once, and no "rebalancing" will ever happen.
  , maUpdate :: !(Update m state context input output)
    -- ^ This argument will be used if 'update' is invoked when no slaves have
    -- been added yet.
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
  , mhProfiling :: !(IORef MasterProfiling)
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
  mhp <- newIORef emptyMasterProfiling
  let mh = MasterHandle_
        { mhSlaves = NoSlavesYet mempty
        , mhSlaveIdSupply = slaveIdSupply
        , mhStateIdSupply = stateIdSupply
        , mhArgs = ma
        , mhProfiling = mhp
        }
  MasterHandle <$> newMVar mh

-- | This will shut down all the slaves.
closeMaster ::
     (MonadConnect m)
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
            scWrite (slaveConnection slave) SReqQuit
            readExpect "closeMaster" (slaveConnection slave) $ \case
              SRespQuit -> return ()
              _ -> Nothing
          case mbErr of
            Left err -> do
              $logWarnJ (printf
                "Error while quitting slave %d (this can happen when the master is closed because of an error): %s" slaveId (show err) :: String)
            Right () -> return ()

{-# INLINE readExpect #-}
readExpect ::
     (MonadIO m, MonadBaseControl IO m)
  => Text
  -> SlaveConn m state context input output
  -> (SlaveResp state output -> Maybe a)
  -> m a
readExpect loc slave f = do
  resp <- scRead slave
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
  => IORef MasterProfiling
  -> Int -- ^ Number of slaves
  -> Int -- ^ Min batch size
  -> HMS.HashMap StateId [(StateId, input)]  -- we need lookup here, so this should be a 'HashMap'
  -> SlaveId
  -> UpdateSlaveResp output
  -> SlavesStatus
  -> m (UpdateSlaveStep input output)
updateSlavesStep mp nSlaves maxBatchSize inputs respSlaveId resp statuses0 = withAtomicProfiling mp mpUpdateSlavesStep $ do
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
      (toUpdateInputs, remaining) <- takeEnoughStatesToUpdate (_ssRemainingStates ss)
      if null toUpdateInputs
        then do -- Steal slaves
          mbStolen <- stealSlavesFromSomebody slaveId uss
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
      -> m ([(StateId, [(StateId, input)])], [StateId])
    takeEnoughStatesToUpdate remaining00 = do
        -- We randomise the batchsize, in order to vary the load
        -- between the slaves.  If the slaves all have the same load,
        -- they are likely to finish their batches and request new
        -- jobs at the same time, which would lead to contention in
        -- the master.
        let maxLength = (length remaining00 `div` nSlaves)
        batchSize <- liftM (max maxBatchSize) <$> liftIO $ R.randomRIO (maxLength `div` 2, maxLength)
        return $ go batchSize remaining00 0
      where
        go batchSize remaining0 grabbed = if grabbed >= batchSize
          then ([], remaining0)
          else case remaining0 of
            [] -> ([], remaining0)
            stateId : remaining -> let
              inps = inputs HMS.! stateId
              in first ((stateId, inps) :) (go batchSize remaining (grabbed + length inps))

    {-# INLINE stealSlavesFromSomebody #-}
    stealSlavesFromSomebody ::
         SlaveId -- Requesting the slaves
      -> SlavesStatus
      -> m (Maybe (SlavesStatus, SlaveId, HS.HashSet StateId))
    stealSlavesFromSomebody requestingSlaveId uss = do
      let goodCandidate (slaveId, ss) =
            if (slaveId /= requestingSlaveId) && not (null (_ssRemainingStates ss))
            then do
                (toTransfer, remaining) <- takeEnoughStatesToUpdate (_ssRemainingStates ss)
                return $ Just (slaveId, map fst toTransfer, sum (map (length . snd) toTransfer), remaining)
            else return Nothing
      candidates <- catMaybes <$> mapM goodCandidate (HMS.toList uss) :: m [(SlaveId, [StateId], Int, [StateId])]
      if null candidates
          then return Nothing
          else do
      -- Pick candidate with highest number of states to steal states from
            let (candidateSlaveId, statesToBeTransferred, _, remainingStates) =
                    maximumByEx (comparing (\(_, _, x, _) -> x)) candidates
            let uss' =
                 set (at candidateSlaveId . _Just . ssRemainingStates) remainingStates $
                 set (at requestingSlaveId . _Just . ssWaitingForStates) True uss
            return $ return (uss', candidateSlaveId, HS.fromList statesToBeTransferred)

{-# INLINE updateSlaves #-}
updateSlaves :: forall state context input output m.
     (MonadConnect m, NFData input, NFData output)
  => IORef MasterProfiling
  -> Int -- ^ Number of slaves
  -> Int -- ^ Max batch size
  -> HMS.HashMap SlaveId (Slave m state context input output)
  -- ^ Slaves connections
  -> context
  -- ^ Context to the computation
  -> HMS.HashMap StateId [(StateId, input)]
  -- ^ Inputs to the computation
  -> m [(SlaveId, [(StateId, [(StateId, output)])])]
updateSlaves mp nSlaves maxBatchSize slaves context inputMap = withAtomicProfiling mp mpUpdateSlaves $ do
  -- initialise slaves
  let slaveStatus0 slave = SlaveStatus
        { _ssWaitingResps = 1 -- The init
        , _ssRemainingStates = HMS.keys (slaveStates slave)
        , _ssWaitingForStates = False
        }
      statuses0 = slaveStatus0 <$> slaves
  statuses1 <- foldM initSlave statuses0 (HMS.keys slaves)
  masterLoop statuses1
  where

    initSlave statuses slaveId = do
        UpdateSlaveStep{..} <- updateSlavesStep mp nSlaves maxBatchSize inputMap slaveId USRespInit statuses
        forM_ ussReqs sendRequest
        return ussSlavesStatus

    masterLoop ::
           SlavesStatus
        -> m [(SlaveId, [(StateId, [(StateId, output)])])]
    masterLoop statuses0 -- waitForAnySlave unregister
        = do
            let outputs0 = HMS.empty
            waits <- do waitHS <- forM slaves $ \slave -> do
                         wait <- scWaitReadSTM (slaveConnection slave)
                         return (slave, wait)
                        liftIO $ HT.fromListWithSizeHint nSlaves (HMS.toList waitHS)
                          :: m (HT.BasicHashTable SlaveId (Slave m state context input output, (STM (SlaveResp state output), m ())))
            let go outputs statuses nActiveSlaves
                 | nActiveSlaves == 0 = do
                     liftIO (HT.toList waits) >>= stopWaiting
                     return $ HMS.toList outputs
                 | otherwise = do
                     waitList <- liftIO (HT.toList waits)
                     liftIO (atomically (waitForAny waitList)) >>= \(slaveId, (resp, slave)) -> do
                         (statuses', requests, newOutputs) <- handleResponse slaveId resp statuses
                  -- immediately send new requests
                         forM_ requests sendRequest
                         let status = statuses' HMS.! slaveId
                             outputs' = HMS.insertWith (<>) slaveId newOutputs outputs
                         if _ssWaitingResps status == 0 && not (_ssWaitingForStates status)
                             then do
                                 $logInfoJ ("Slave " ++ tshow (unSlaveId slaveId) ++ " is done")
                                 liftIO $ HT.delete waits slaveId
                                 go outputs' statuses' (nActiveSlaves - 1)
                             else do
                                 let conn = slaveConnection slave
                                 wait <- scWaitReadSTM conn
                                 liftIO $ HT.insert waits slaveId (slave, wait)
                                 go outputs' statuses' nActiveSlaves
            go outputs0 statuses0 nSlaves

    waitForAny :: [(SlaveId, (Slave m state context input output, (STM (SlaveResp state output), a)))]
               -> STM (SlaveId, (SlaveResp state output, Slave m state context input output))
    waitForAny = foldr
          (orElse . \(slaveId, (slave, (wait, _release))) -> wait >>= \resp -> return (slaveId, (resp, slave)))
          retry

    stopWaiting :: [(SlaveId, (a, (b, m ())))] -> m ()
    stopWaiting = mapM_ (\(_slaveId, (_slave, (_wait, release))) -> release)

    handleResponse slaveId resp statuses = do
        resp' <- case resp of
                SRespResetState -> throwIO (UnexpectedResponse "updateSlaves" (displayResp resp))
                SRespGetStates _ -> throwIO (UnexpectedResponse "updateSlaves" (displayResp resp))
                SRespQuit -> throwIO (UnexpectedResponse "updateSlaves" (displayResp resp))
                SRespError err -> throwIO (ExceptionFromSlave err)
                SRespAddStates addedStates -> return (USRespAddStates addedStates)
                SRespRemoveStates requestingSlaveId removedStates ->
                  return (USRespRemoveStates requestingSlaveId removedStates)
                SRespUpdate outputs_ -> return (USRespUpdate outputs_)
                SRespGetProfile _ -> throwIO (UnexpectedResponse "updateSlaves" (displayResp resp))
        UpdateSlaveStep{..} <- updateSlavesStep mp nSlaves maxBatchSize inputMap slaveId resp' statuses
        return (ussSlavesStatus, ussReqs, ussOutputs)

    sendRequest (slaveId, req) = do
        let req' = case req of
                      USReqUpdate inputs -> SReqUpdate context inputs
                      USReqRemoveStates x y -> SReqRemoveStates x y
                      USReqAddStates y -> SReqAddStates y
            conn = slaveConnection (slaves HMS.! slaveId)
        $logDebugJ ("Sending request to slave " ++ tshow (unSlaveId slaveId) ++ ": " ++ displayReq req')
        withAtomicProfiling mp mpSendLoopSend $ scWrite conn req'

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
     (MonadConnect m, NFData input, NFData output, NFData state, NFData context)
  => MasterHandle m state context input output
  -> context
  -> HMS.HashMap StateId [input]
  -> m (HMS.HashMap StateId (HMS.HashMap StateId output))
update (MasterHandle mv) context inputs0 = modifyMVar mv $ \mh -> withAtomicProfiling (mhProfiling mh) mpTotalUpdate $ do
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
      _sp <- newIORef emptySlaveProfiling -- we'll not read this, but we need it to pass to 'statefulUpdate'.
      $logWarnJ ("Executing update without any slaves" :: Text)
      stateMap <- liftIO $ HT.fromList states
      outputs <- statefulUpdate _sp (maUpdate (mhArgs mh)) stateMap context inputList
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
              scWrite
                (slaveConnection (slaves HMS.! slaveId))
                (SReqUpdate context inps)
            forM slaveIdsAndInputs $ \(slaveId, _) ->
              liftM (slaveId,) $ readExpect "update (1)" (slaveConnection (slaves HMS.! slaveId)) $ \case
                SRespUpdate outputs -> Just outputs
                _ -> Nothing
          Just maxBatchSize -> updateSlaves (mhProfiling mh) (HMS.size slaves) maxBatchSize slaves context inputMap
      let mh' = mh
            { mhSlaves = Slaves $
                integrateNewStates slaves (second (fmap (const ()) . HMS.fromList . mconcat . map snd) <$> outputs)
            }
      return (mh', fmap HMS.fromList . HMS.fromList $ foldMap snd outputs)

-- | Adds a connection to a slave.
addSlaveConnection ::
     (MonadConnect m)
  => MasterHandle m state context input output
  -> SlaveConn m state context input output
  -> m ()
addSlaveConnection (MasterHandle mhv) conn = modifyMVar_ mhv $ \mh -> do
  slaveId <- askSupply (mhSlaveIdSupply mh)
  case mhSlaves mh of
    NoSlavesYet states -> do
      let slaves = HMS.fromList [(slaveId, Slave conn (const () <$> HMS.fromList states))]
      sendStates [(conn, states)]
      return mh{mhSlaves = Slaves slaves}
    Slaves slaves -> do
      return mh{mhSlaves = Slaves (HMS.insert slaveId (Slave conn mempty) slaves)}

sendStates ::
     (Monad m, MonadBaseControl IO m, MonadIO m)
  => [(SlaveConn m state context input output, [(StateId, state)])]
  -> m ()
sendStates slavesWithStates = do
  -- Send states
  forM_ slavesWithStates $ \(conn, states) -> do
    scWrite conn (SReqResetState states)
  forM_ slavesWithStates $ \(conn, _) -> do
    readExpect "sendStates" conn $ \case
      SRespResetState -> Just ()
      _ -> Nothing

-- | This sets the states stored in the slaves. It distributes the
-- states among the currently connected slaves.
--
-- If no slaves are connected, this will throw a
-- 'NoSlavesConnectedException'.
{-# INLINE resetStates #-}
resetStates :: forall state context input output m.
     (MonadBaseControl IO m, MonadIO m, MonadLogger m)
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
getStates ::
     (MonadConnect m)
  => MasterHandle m state context input output
  -> m (HMS.HashMap StateId state)
getStates (MasterHandle mhv) = withMVar mhv $ \mh -> do
  -- Send states
  case mhSlaves mh of
    NoSlavesYet states -> return $ HMS.fromList states
    Slaves slaves -> do
      forM_ slaves (\slave_ -> scWrite (slaveConnection slave_) SReqGetStates)
      responses <- forM (HMS.elems slaves) $ \slave_ -> do
        readExpect "getStates" (slaveConnection slave_) $ \case
          SRespGetStates states -> Just states
          _ -> Nothing
      return (HMS.fromList $ mconcat responses)

getSlavesProfiling ::
       MonadConnect m
    => MasterHandle m state context input output
    -> m (HMS.HashMap SlaveId SlaveProfiling)  -- [(SlaveId, SlaveProfiling)]
getSlavesProfiling (MasterHandle mhv) = withMVar mhv $ \mh ->
    case mhSlaves mh of
        NoSlavesYet _states -> return HMS.empty
        Slaves slaves -> do
            forM_ slaves $ \slave -> scWrite (slaveConnection slave) SReqGetProfile
            forM slaves $ \slave ->
                readExpect "getSlavesProfiling" (slaveConnection slave) $ \case
                    SRespGetProfile sp -> return sp
                    _ -> Nothing

getMasterProfiling ::
       MonadConnect m
    => MasterHandle m state context input output
    -> m MasterProfiling
getMasterProfiling (MasterHandle mhv) =
    readMVar mhv >>= \mh ->
    readIORef (mhProfiling mh)

getMasterProfilingIORef ::
    MonadConnect m
    => MasterHandle m state context input output
    -> m (IORef MasterProfiling)
getMasterProfilingIORef (MasterHandle mhv) = mhProfiling <$> readMVar mhv

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
