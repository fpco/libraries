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
module Distributed.Stateful.Master
    ( MasterArgs(..)
    , MasterHandle
    , MasterException(..)
    , StateId
    , SlaveId
    , initMaster
    , closeMaster
    , update
    , resetStates
    , getStateIds
    , getStates
    , addSlaveConnection
    , getNumSlaves
    , SlaveConn
    , StatefulConn(..)
    ) where

import           ClassyPrelude
import qualified Control.Concurrent.Async.Lifted.Safe as Async
import           Control.Monad.Logger (MonadLogger, logError, logWarn, logInfo, logDebug)
import           Control.Monad.Trans.Control (MonadBaseControl)
import qualified Data.HashMap.Strict as HMS
import qualified Data.HashSet as HS
import           Data.List.Split (chunksOf)
import           Data.SimpleSupply
import           Distributed.Stateful.Internal
import           Text.Printf (printf)
import           FP.Redis (MonadConnect)
import           Control.Lens (makeLenses, set, at, _Just, over)
import           Control.Exception (throw)
import           Control.Exception.Lifted (evaluate)
import           Control.DeepSeq (force, NFData)
import qualified Control.Concurrent.STM as STM
import           Data.Foldable (asum)
import           Data.Void (absurd)

-- | Arguments for 'mkMasterHandle'
data MasterArgs m state context input output = MasterArgs
  { maMaxBatchSize :: !(Maybe Int)
    -- ^ The maximum amount of states that will be transferred at once. If 'Nothing', they
    -- will be all transferred at once, and no "rebalancing" will ever happen.
  , maUpdate :: !(context -> input -> state -> m (state, output))
    -- ^ This argument will be used if 'update' is invoked when no slaves have
    -- been added yet.
  }

newtype MasterHandle m state context input output =
  MasterHandle {_unMasterHandle :: MVar (MasterHandle_ m state context input output)}

data Slaves m state context input output
  = NoSlavesYet !(HMS.HashMap StateId state)
  | Slaves !(HMS.HashMap SlaveId (Slave m state context input output))

data MasterHandle_ m state context input output = MasterHandle_
  { mhSlaves :: !(Slaves m state context input output)
  , mhSlaveIdSupply :: !(Supply SlaveId)
  , mhStateIdSupply :: !(Supply StateId)
  , mhArgs :: !(MasterArgs m state context input output)
  }

type SlaveConn m state context input output =
  StatefulConn m (SlaveReq state context input) (SlaveResp state output)

data Slave m state context input output = Slave
  { slaveConnection :: !(SlaveConn m state context input output)
  , slaveStates :: !(HMS.HashMap StateId ())
  }

data MasterException
  = MasterException Text
  | InputMissingException StateId
  | UnusedInputsException [StateId]
  | NoSlavesConnectedException
  | UnexpectedResponse Text Text
  | ExceptionFromSlave Text
  deriving (Eq, Show, Typeable)

instance Exception MasterException

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
  let mh = MasterHandle_
        { mhSlaves = NoSlavesYet mempty
        , mhSlaveIdSupply = slaveIdSupply
        , mhStateIdSupply = stateIdSupply
        , mhArgs = ma
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
              $logError $ pack $ printf
                "Error while quitting slave %d: %s" slaveId (show err)
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
  -> m ([(SlaveId, [(StateId, [(StateId, input)])])])
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

{-# INLINE strictSetDifference #-}
strictSetDifference ::
     (Show a, Eq a, Hashable a, MonadThrow m)
  => String -> HS.HashSet a -> HS.HashSet a -> m (HS.HashSet a)
strictSetDifference loc a b = do
  let c = a `HS.difference` b
  unless (HS.size c == HS.size a - HS.size b) $
    throwM $ MasterException $ pack $
      printf "%s: Bad set difference (strictSetDifference), %s - %s" loc (show a) (show b)
  return c

{-# INLINE strictMapDifference #-}
strictMapDifference ::
     (Show a, Eq a, Hashable a, MonadThrow m)
  => String -> HMS.HashMap a b -> HMS.HashMap a c -> m (HMS.HashMap a b)
strictMapDifference loc a b = do
  let c = a `HMS.difference` b
  unless (HMS.size c == HMS.size a - HMS.size b) $
    throwM $ MasterException $ pack $
      printf "%s: Bad map difference (strictMapDifference), %s - %s" loc (show (HMS.keys a)) (show (HMS.keys b))
  return c

data UpdateSlaveResp output
  = USRespAddStates !(HS.HashSet StateId)
  | USRespRemoveStates !SlaveId (HMS.HashMap StateId ByteString)
  | USRespUpdate !(HMS.HashMap StateId (HMS.HashMap StateId output))
  | USRespInit
  deriving (Eq, Show, Generic)
instance (NFData output) => NFData (UpdateSlaveResp output)

data UpdateSlaveReq input
  = USReqUpdate !(HMS.HashMap StateId (HMS.HashMap StateId input))
  | USReqAddStates !(HMS.HashMap StateId ByteString)
  | USReqRemoveStates !SlaveId !(HS.HashSet StateId)
  deriving (Eq, Show, Generic)
instance (NFData input) => NFData (UpdateSlaveReq input)

{-
data UpdateSlavesStatus input output = UpdateSlavesStatus
  { _ussSlaves :: !(HMS.HashMap SlaveId SlaveStatus))
  , _ussInputs :: !(HMS.HashMap StateId (HMS.HashMap StateId input)))
  , _ussOutputs :: !(HMS.HashMap SlaveId (HMS.HashMap StateId (HMS.HashMap StateId output))))
  } deriving (Generic)
makeLenses ''UpdateSlavesStatus
instance (NFData input, NFData output) => NFData (UpdateSlavesStatus input output)
-}

data UpdateSlaveStep input output = UpdateSlaveStep
  { ussReqs :: ![(SlaveId, UpdateSlaveReq input)]
  , ussSlavesStatus :: !(HMS.HashMap SlaveId SlaveStatus)
  , ussOutputs :: !(HMS.HashMap StateId (HMS.HashMap StateId output))
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
  => Int -- ^ Max batch size
  -> HMS.HashMap StateId (HMS.HashMap StateId input)
  -> SlaveId
  -> UpdateSlaveResp output
  -> SlavesStatus
  -> m (UpdateSlaveStep input output)
updateSlavesStep maxBatchSize inputs respSlaveId resp statuses0 = do
  let statuses1 = over (at respSlaveId . _Just . ssWaitingResps) (\c -> c - 1) statuses0
  (statuses2, requests, outputs) <- case resp of
    USRespInit -> do
      addOutputs_ <$> findSomethingToUpdate respSlaveId statuses1
    USRespUpdate outputs -> do
      addOutputs outputs <$> findSomethingToUpdate respSlaveId statuses1
    USRespAddStates statesIds -> do
      let statuses' =
            over (at respSlaveId . _Just) (over ssRemainingStates (++ HS.toList statesIds) . set ssWaitingForStates False) $
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
              $logInfo ("Tried to get something to do for slave " ++ tshow (unSlaveId slaveId) ++ ", but couldn't find anything")
              return (uss, [])
            Just (uss', stolenFrom, stolenStates) -> do -- Request stolen slaves
              $logInfo ("Stealing " ++ tshow (HS.size stolenStates) ++ " states from slave " ++ tshow (unSlaveId stolenFrom) ++ " for slave " ++ tshow (unSlaveId slaveId))
              return (uss', [(stolenFrom, USReqRemoveStates slaveId stolenStates)])
        else do -- Send update command
          let toUpdateInputsMap = HMS.fromList toUpdateInputs
          let uss' = set (at slaveId . _Just . ssRemainingStates) remaining uss
          return (uss', [(slaveId, USReqUpdate toUpdateInputsMap)])

    {-# INLINE takeEnoughStatesToUpdate #-}
    takeEnoughStatesToUpdate ::
         [StateId]
      -> ([(StateId, HMS.HashMap StateId input)], [StateId])
    takeEnoughStatesToUpdate remaining00 = go remaining00 0
      where
        go remaining0 grabbed = if grabbed >= maxBatchSize
          then ([], remaining0)
          else case remaining0 of
            [] -> ([], remaining0)
            stateId : remaining -> let
              inps = inputs HMS.! stateId
              in first ((stateId, inps) :) (go remaining (grabbed + HMS.size inps))

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
            return (slaveId, map fst toTransfer, sum (map (HMS.size . snd) toTransfer), remaining)
      let candidates :: [(SlaveId, [StateId], Int, [StateId])]
          candidates = catMaybes (map goodCandidate (HMS.toList uss))
      guard (not (null candidates))
      -- Pick candidate with highest number of states to steal states from
      let (candidateSlaveId, statesToBeTransferred, _, remainingStates) =
            maximumByEx (comparing (\(_, _, x, _) -> x)) candidates
      let uss' =
            set (at candidateSlaveId . _Just . ssRemainingStates) remainingStates $
            set (at requestingSlaveId . _Just . ssWaitingForStates) True uss
      return (uss', candidateSlaveId, HS.fromList statesToBeTransferred)

{-# INLINE updateSlaves #-}
updateSlaves :: forall state context input output m.
     (MonadConnect m, NFData input, NFData output)
  => Int -- ^ Max batch size
  -> HMS.HashMap SlaveId (Slave m state context input output)
  -- ^ Slaves connections
  -> context
  -- ^ Context to the computation
  -> HMS.HashMap StateId (HMS.HashMap StateId input)
  -- ^ Inputs to the computation
  -> m [(SlaveId, HMS.HashMap StateId (HMS.HashMap StateId output))]
updateSlaves maxBatchSize slaves context inputMap = do
  lockedConns :: HMS.HashMap SlaveId (MVar (SlaveConn m state context input output)) <-
    forM slaves (\slave -> newMVar (slaveConnection slave))
  let slaveStatus0 slave = SlaveStatus
        { _ssWaitingResps = 1 -- The init
        , _ssRemainingStates = HMS.keys (slaveStates slave)
        , _ssWaitingForStates = False
        }
  statusVar <- newMVar (slaveStatus0 <$> slaves)
  Async.mapConcurrently (slaveLoop lockedConns statusVar) (HMS.toList slaves)
  where
    slaveLoop ::
         HMS.HashMap SlaveId (MVar (SlaveConn m state context input output))
      -> MVar SlavesStatus
      -> (SlaveId, Slave m state context input output)
      -> m (SlaveId, HMS.HashMap StateId (HMS.HashMap StateId output))
    slaveLoop outgoingChans statusVar (slaveId, slave) = do
      {-
      incomingChan <- liftIO newTChanIO
      outs <- either absurd id <$> Async.race (receiveLoop incomingChan) (go incomingChan mempty USRespInit)
      return (slaveId, outs)
      -}
      outs <- go mempty USRespInit
      return (slaveId, outs)
      where
        go !outputs0 resp = do
          (statuses, requests, outputs1) <- modifyMVar statusVar $ \status -> do
            UpdateSlaveStep{..} <- updateSlavesStep maxBatchSize inputMap slaveId resp status
            return (ussSlavesStatus, (ussSlavesStatus, ussReqs, ussOutputs))
          forM_ requests $ \(slaveId_, req0) -> do
            let req = case req0 of
                  USReqUpdate inputs -> SReqUpdate context inputs
                  USReqRemoveStates x y -> SReqRemoveStates x y
                  USReqAddStates y -> SReqAddStates y
            withMVar (outgoingChans HMS.! slaveId_) $ \conn -> do
              $logDebug ("Sending request to slave " ++ tshow (unSlaveId slaveId) ++ ": " ++ displayReq req)
              scWrite conn req
          let outputs = outputs1 <> outputs0
          let status = statuses HMS.! slaveId
          if _ssWaitingResps status == 0 && not (_ssWaitingForStates status)
            then do
              $logInfo ("Slave " ++ tshow (unSlaveId slaveId) ++ " is done")
              return outputs
            else do
              resp0 <- scRead (slaveConnection slave)
              $logDebug ("Received slave response from slave " ++ tshow (unSlaveId slaveId) ++ ": " ++ displayResp resp0)
              resp' <- case resp0 of
                SRespResetState -> throwIO (UnexpectedResponse "updateSlaves" (displayResp resp0))
                SRespGetStates _ -> throwIO (UnexpectedResponse "updateSlaves" (displayResp resp0))
                SRespQuit -> throwIO (UnexpectedResponse "updateSlaves" (displayResp resp0))
                SRespError err -> throwIO (ExceptionFromSlave err)
                SRespAddStates addedStates -> return (USRespAddStates addedStates)
                SRespRemoveStates requestingSlaveId removedStates ->
                  return (USRespRemoveStates requestingSlaveId removedStates)
                SRespUpdate outputs_ -> return (USRespUpdate outputs_)
              go outputs resp'

        {-
            _ -> go incomingChan outputs =<< atomically (readTChan incomingChan)

        receiveLoop incomingChan = forever $ do
          resp0 <- scRead (slaveConnection slave)
          $logDebug ("Received slave response from slave " ++ tshow (unSlaveId slaveId) ++ ": " ++ displayResp resp0)
          resp' <- case resp0 of
            SRespResetState -> throwIO (UnexpectedResponse (displayResp resp0))
            SRespGetStates _ -> throwIO (UnexpectedResponse (displayResp resp0))
            SRespQuit -> throwIO (UnexpectedResponse (displayResp resp0))
            SRespError err -> throwIO (ExceptionFromSlave err)
            SRespAddStates addedStates -> return (USRespAddStates addedStates)
            SRespRemoveStates requestingSlaveId removedStates ->
              return (USRespRemoveStates requestingSlaveId removedStates)
            SRespUpdate outputs -> return (USRespUpdate outputs)
          atomically (writeTChan incomingChan resp')
        -}

-- | Send an update request to all the slaves. This will cause each of
-- the slaves to apply 'saUpdate' to each of its states. The outputs of
-- these invocations are returned in a 'HashMap'.
--
-- It rebalances the states among the slaves during this process. This
-- way, if some of the slaves finish early, they can help the others
-- out, by moving their workitems elsewhere.
--
-- NOTE: it is up to the client to not run this concurrently with
-- another 'update' or 'resetStates'.
--
-- NOTE: if this throws an exception, then `MasterHandle` may be in an
-- inconsistent state, and the whole computation should be aborted.
{-# INLINE update #-}
update :: forall state context input output m.
     (MonadConnect m, NFData input, NFData output)
  => MasterHandle m state context input output
  -> context
  -> HMS.HashMap StateId [input]
  -> m (HMS.HashMap StateId (HMS.HashMap StateId output))
update (MasterHandle mv) context inputs0 = modifyMVar mv $ \mh -> do
  -- TODO add checks for well-formedness of inputs'
  -- Give state ids to each of the inputs, which will be used to label the
  -- state resulting from invoking saUpdate with that input.
  let sortedInputs = sortBy (comparing fst) (HMS.toList inputs0)
  inputMap :: HMS.HashMap StateId [(StateId, input)] <-
    withSupplyM (mhStateIdSupply mh) $
    fmap HMS.fromList $
    forM sortedInputs $ \(stateId, inps) ->
      (stateId,) <$> mapM (\input -> (, input) <$> askSupplyM) inps
  case mhSlaves mh of
    NoSlavesYet states -> do
      $logWarn "Executing update without any slaves"
      (states', outputs) <- statefulUpdate (maUpdate (mhArgs mh)) states context (HMS.fromList <$> inputMap)
      let mh' = mh{ mhSlaves = NoSlavesYet states' }
      return (mh', outputs)
    Slaves slaves -> do
      $logInfo ("Executing update with " ++ tshow (HMS.size slaves) ++ " slaves")
      -- Update the slave states.
      slaveIdsAndInputs <- either throwAndLog return $
        assignInputsToSlaves (slaveStates <$> slaves) inputMap
      outputs :: [(SlaveId, HMS.HashMap StateId (HMS.HashMap StateId output))] <-
        case maMaxBatchSize (mhArgs mh) of
          Nothing -> do
            forM_ slaveIdsAndInputs $ \(slaveId, inps) -> do
              scWrite
                (slaveConnection (slaves HMS.! slaveId))
                (SReqUpdate context (HMS.fromList <$> HMS.fromList inps))
            forM slaveIdsAndInputs $ \(slaveId, _) ->
              liftM (slaveId,) $ readExpect "update (1)" (slaveConnection (slaves HMS.! slaveId)) $ \case
                SRespUpdate outputs -> Just outputs
                _ -> Nothing
          Just maxBatchSize -> updateSlaves maxBatchSize slaves context (HMS.fromList <$> inputMap)
      let mh' = mh
            { mhSlaves = Slaves $
                integrateNewStates slaves (second (fmap (const ()) . mconcat . HMS.elems) <$> outputs)
            }
      return (mh', foldMap snd outputs)

-- | Adds a connection to a slave. Unlike 'update', 'resetStates', etc,
-- this can be called concurrently with the other operations.
--
-- NOTE: this being able to be called concurrently is why we need to be
-- careful about atomically updating 'mhSlaveInfoRef'.
addSlaveConnection ::
     (MonadConnect m)
  => MasterHandle m state context input output
  -> SlaveConn m state context input output
  -> m ()
addSlaveConnection (MasterHandle mhv) conn = modifyMVar_ mhv $ \mh -> do
  slaveId <- askSupply (mhSlaveIdSupply mh)
  case mhSlaves mh of
    NoSlavesYet states -> do
      let slaves = HMS.fromList [(slaveId, Slave conn (const () <$> states))]
      sendStates [(conn, states)]
      return mh{mhSlaves = Slaves slaves}
    Slaves slaves -> do
      return mh{mhSlaves = Slaves (HMS.insert slaveId (Slave conn mempty) slaves)}

sendStates ::
     (Monad m, MonadBaseControl IO m, MonadIO m)
  => [(SlaveConn m state context input output, HMS.HashMap StateId state)]
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
-- If no slaves are connected, this throws an exception.
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
      return (mh{mhSlaves = NoSlavesYet (HMS.fromList allStates)}, HMS.fromList allStates)
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
        [ (slaveConnection slave, HMS.fromList (slavesStates HMS.! slaveId))
        | (slaveId, slave) <- HMS.toList slaves
        ]
      return (mh{mhSlaves = Slaves slaves'}, HMS.fromList allStates)

getStateIds ::
     (MonadConnect m)
  => MasterHandle m state context input output
  -> m (HS.HashSet StateId)
getStateIds (MasterHandle mhv) = do
  mh <- readMVar mhv
  return $ case mhSlaves mh of
    NoSlavesYet states -> HS.fromList (HMS.keys states)
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
    NoSlavesYet states -> return states
    Slaves slaves -> do
      forM_ slaves (\slave_ -> scWrite (slaveConnection slave_) SReqGetStates)
      responses <- forM (HMS.elems slaves) $ \slave_ -> do
        readExpect "getStates" (slaveConnection slave_) $ \case
          SRespGetStates states -> Just states
          _ -> Nothing
      return (mconcat responses)

getNumSlaves ::
     (MonadConnect m)
  => MasterHandle m state context input output
  -> m Int
getNumSlaves (MasterHandle mhv) = do
  mh <- readMVar mhv
  return $ case mhSlaves mh of
    NoSlavesYet _ -> 0
    Slaves slaves -> HMS.size slaves
