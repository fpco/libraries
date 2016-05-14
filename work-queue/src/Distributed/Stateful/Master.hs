{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE GADTs #-}
module Distributed.Stateful.Master
    ( MasterArgs(..)
    , MasterHandle
    , MasterException(..)
    , StateId
    , mkMasterHandle
    , update
    , resetStates
    , getStateIds
    , getStates
    , addSlaveConnection
    , SlaveConn
    , StatefulConn(..)
    ) where

import           ClassyPrelude
import           Control.Concurrent.Async.Lifted.Safe (mapConcurrently)
import qualified Control.Concurrent.Async.Lifted.Safe as Async
import           Control.Monad.Logger (logDebugNS, MonadLogger)
import           Control.Monad.Trans.Control (MonadBaseControl)
import qualified Data.HashMap.Strict as HMS
import qualified Data.HashSet as HS
import           Data.List.Split (chunksOf)
import           Data.SimpleSupply
import           Distributed.Stateful.Internal
import           Text.Printf (printf)
import           FP.Redis (MonadConnect)
import           Data.Void (absurd)

-- | Arguments for 'mkMasterHandle'
data MasterArgs = MasterArgs
  { maMaxBatchSize :: !(Maybe Int)
    -- ^ The maximum amount of states that will be transferred at once. If 'Nothing', they
    -- will be all transferred at once, and no "rebalancing" will ever happen.
    -- Moreover, if 'Nothing', 'maMinBatchSize' will be ignored.
  , maMinBatchSize :: !(Maybe Int)
    -- ^ The minimum amount of states that will be transferred at once. 'Nothing' is equivalent to
    -- @'Just' 0@.
  }

newtype MasterHandle m state context input output =
  MasterHandle {unMasterHandle :: MVar (MasterHandle_ m state context input output)}

data MasterHandle_ m state context input output = MasterHandle_
  { mhSlaves :: !(HMS.HashMap SlaveId (Slave m state context input output))
  , mhSlaveIdSupply :: !(Supply SlaveId)
  , mhStateIdSupply :: !(Supply StateId)
  , mhArgs :: !MasterArgs
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
  | UnexpectedResponse Text
  | ExceptionFromSlave Text
  deriving (Eq, Show, Typeable)

instance Exception MasterException

-- | Create a new 'MasterHandle' based on the 'MasterArgs'. This
-- 'MasterHandle' should be used for all interactions with this
-- particular stateful computation (via 'update' and 'resample').
mkMasterHandle
  :: (MonadBaseControl IO m, MonadIO m, MonadLogger m)
  => MasterArgs
  -> m (MasterHandle m state context input output)
mkMasterHandle ma@MasterArgs{..} = do
  let throw = throwAndLog . MasterException . pack
  case (maMinBatchSize, maMaxBatchSize) of
    (_, Just maxBatchSize) | maxBatchSize < 1 ->
      throw (printf "Distribute.master: maMaxBatchSize must be > 0 (got %d)" maxBatchSize)
    (Just minBatchSize, _) | minBatchSize < 0 ->
      throw (printf "Distribute.master: maMinBatchSize must be >= 0 (got %d)" minBatchSize)
    (Just minBatchSize, Just maxBatchSize) | minBatchSize > maxBatchSize ->
      throw (printf "Distribute.master: maMinBatchSize can't be greater then maMaxBatchSize (got %d and %d)" minBatchSize maxBatchSize)
    _ ->
      return ()
  slaveIdSupply <- newSupply (SlaveId 0) (\(SlaveId n) -> SlaveId (n + 1))
  stateIdSupply <- newSupply (StateId 0) (\(StateId n) -> StateId (n + 1))
  let mh = MasterHandle_
        { mhSlaves = mempty
        , mhSlaveIdSupply = slaveIdSupply
        , mhStateIdSupply = stateIdSupply
        , mhArgs = ma
        }
  MasterHandle <$> newMVar mh

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
     (MonadConnect m)
  => MasterHandle m state context input output
  -> context
  -> HMS.HashMap StateId [input]
  -> m (HMS.HashMap StateId (HMS.HashMap StateId output))
update (MasterHandle mv) context inputs0 = modifyMVar mv $ \mh -> do
  let slaves = mhSlaves mh
  when (HMS.null slaves) $
    throwIO (MasterException "No slaves, cannot update")
  -- Give state ids to each of the inputs, which will be used to label the
  -- state resulting from invoking saUpdate with that input.
  let sortedInputs = sortBy (comparing fst) (HMS.toList inputs0)
  inputMap :: HMS.HashMap StateId [(StateId, input)] <-
    withSupplyM (mhStateIdSupply mh) $
    fmap HMS.fromList $
    forM sortedInputs $ \(stateId, inps) ->
      (stateId,) <$> mapM (\input -> (, input) <$> askSupplyM) inps
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
          liftM (slaveId,) $ readExpect (slaveConnection (slaves HMS.! slaveId)) $ \case
            SRespUpdate outputs -> Just outputs
            _ -> Nothing
      Just maxBatchSize -> do
        error "TODO"
        {-
        slaveStatesVar :: MVar (HMS.HashMap SlaveId [StateId]) <-
          newMVar (HMS.fromList (map (second (map fst)) slaveIdsAndInputs))
        let slaveThread slaveId = updateSlaveThread
              maxBatchSize (maMinBatchSize (mhArgs mh)) slaveId (slaveConnection <$> slaves) slaveStatesVar context inputMap
        mapConcurrently (\slaveId -> (slaveId, ) <$> slaveThread slaveId) (HMS.keys slaves)
        -}
  let mh' = mh
        { mhSlaves =
            integrateNewStates slaves (second (fmap (const ()) . mconcat . HMS.elems) <$> outputs)
        }
  return (mh', foldMap snd outputs)

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

assignInputsToSlaves ::
     HMS.HashMap SlaveId (HMS.HashMap StateId ())
  -> HMS.HashMap StateId [(StateId, input)]
  -> Either MasterException ([(SlaveId, [(StateId, [(StateId, input)])])])
assignInputsToSlaves slaveStates inputMap = do
  let go (rs, inputs) (slaveId, states) = do
        -- TODO: Other Map + Set datatypes have functions for doing this
        -- more efficiently / concisely.
        r <- forM (HMS.keys states) $ \k ->
          case HMS.lookup k inputs of
            Nothing -> Left (InputMissingException k)
            Just input -> return (k, input)
        let inputs' = inputs `HMS.difference` states
        return ((slaveId, r) : rs, inputs')
  (slavesIdsAndInputs, unusedInputs) <- foldM go ([], inputMap) (HMS.toList slaveStates)
  when (not (HMS.null unusedInputs)) $
    Left (UnusedInputsException (HMS.keys unusedInputs))
  return slavesIdsAndInputs

{-
updateStatusAndOutputs ::
     SlaveId
  -> SlaveResp state output 
  -> HMS.HashMap SlaveId SlaveStatus
  -> HMS.HashMap StateId (HMS.HashMap StateId output)
  -> Either MasterException
      ( HMS.HashMap SlaveId SlaveStatus
      , HMS.HashMap StateId (HMS.HashMap StateId output)
      , Maybe (UpdateSlaveReq state)
      )
updateStatusAndOutputs slaveId resp statuses outputs = do
  let ss = statuses HMS.! slaveId
  (statuses', slaveOutputs, reqs) <- case resp of
    SRespResetState -> Left (UnexpectedResponse (displayResp resp))
    SRespGetStates _ -> Left (UnexpectedResponse (displayResp resp))
    SRespError err -> Left (ExceptionFromSlave err)
    SRespAddStates addedStates -> do
      remainingAdding <- ssAdding ss `strictDifference` addedStates
      return
        ( HMS.insert slaveId ss{ssAdding = remainingAdding} statuses
        , mempty
        , Nothing
        )
    SRespRemoveStates requestingSlaveId removedStates -> do
      let removedStatesIds = HS.fromList (HMS.keys removedStates)
      remainingRemoving <- ssRemoving ss `strictDifference` removedStatesIds
      let requestingSS = statuses HMS.! requestingSlaveId
      unless (HS.null (ssRemaining requestingSS)) $
        Left $ MasterException $ pack $
          printf "Expected no states in slave %d when receiving states for it."
      return
        ( HMS.insert slaveId ss{ssRemoving = remainingRemoving} $
          HMS.insert requestingSlaveId requestingSS{ssRemaining = removedStatesIds} $
          statuses
        , mempty
        , Nothing
        )
    SRespUpdate outputs -> do
      let updatedStates = HS.fromList (HMS.keys outputs)
      remainingUpdating <- ssUpdating ss `strictDifference` updatedStates
      return
        ( HMS.insert slaveId ss{ssUpdating = remainingUpdating} statuses
        , outputs
        , mempty
        )
  return (statuses', outputs <> slaveOutputs, reqs)
-}

strictSetDifference :: (Show a, Eq a, Hashable a) => HS.HashSet a -> HS.HashSet a -> Either MasterException (HS.HashSet a)
strictSetDifference a b = do
  let c = a `HS.difference` b
  unless (HS.size c == HS.size a - HS.size b) $
    Left $ MasterException $ pack $
      printf "Bad set difference (strictDifference), %s - %s" (show a) (show b)
  return c

strictMapDifference :: (Show a, Eq a, Hashable a) => HMS.HashMap a b -> HMS.HashMap a c -> Either MasterException (HMS.HashMap a b)
strictMapDifference a b = do
  let c = a `HMS.difference` b
  unless (HMS.size c == HMS.size a - HMS.size b) $
    Left $ MasterException $ pack $
      printf "Bad set difference (strictDifference), %s - %s" (show (HMS.keys a)) (show (HMS.keys b))
  return c

{-
slavesDone :: HMS.HashMap StateId SlaveStatus -> Bool
slavesDone = all slaveDone
  where
    slaveDone SlaveStatus{..} =
      null ssRemaining && null ssUpdating &&
      null ssRemoving && null ssAdding

reqsToSend ::
     Int -- ^ Max batch size
  -> Maybe Int -- ^ Maybe min batch size
  -> HMS.HashMap StateId SlaveStatus
  -> (HMS.HashMap StateId SlaveStatus, [(StateId, UpdateSlaveReq state)])
reqsToSend = error "TODO"
-}

data SlaveStatus = SlaveStatus
  { ssRemaining :: !(HS.HashSet StateId) -- ^ Free states that remain
  , ssUpdating :: !(HS.HashSet StateId) -- ^ States we're currently updating
  , ssRemoving :: !(HS.HashSet StateId) -- ^ States we're currently removing
  , ssAdding :: !(HS.HashSet StateId) -- ^ States we're currently adding
  } deriving (Eq, Show)

data UpdateSlaveResp output
  = USRespAddStates !(HS.HashSet StateId)
  | USRespRemoveStates !SlaveId (HMS.HashMap StateId ByteString)
  | USRespUpdate !(HMS.HashMap StateId (HMS.HashMap StateId output))
  | USRespInit
  deriving (Eq, Show)

data UpdateSlaveReq
  = USReqUpdate ![StateId]
  | USReqAddStates !(HMS.HashMap StateId ByteString)
  | USReqRemoveStates !SlaveId !(HS.HashSet StateId)
  deriving (Eq, Show)

data UpdateSlavesStatus output = UpdateSlavesStatus
  { ussSlaves :: !(HMS.HashMap SlaveId SlaveStatus)
  , ussOutputs :: !(HMS.HashMap SlaveId (HMS.HashMap StateId (HMS.HashMap StateId output)))
  }

data UpdateSlaveStep
  = USSDone
  | USSNotDone ![(SlaveId, UpdateSlaveReq)]
  deriving (Eq, Show)

{-# INLINE updateSlavesStep #-}
updateSlavesStep ::
     Int -- ^ Max batch size
  -> Maybe Int -- ^ Maybe min batch size
  -> SlaveId
  -> UpdateSlaveResp output
  -> UpdateSlavesStatus output
  -> Either MasterException (UpdateSlavesStatus output, UpdateSlaveStep)
updateSlavesStep = error "TODO"

{-# INLINE updateSlaves #-}
updateSlaves :: forall state context input output m.
     (MonadConnect m)
  => Int -- ^ Max batch size
  -> Maybe Int -- ^ Maybe min batch size
  -> HMS.HashMap SlaveId (Slave m state context input output)
  -- ^ Slaves connections
  -> context
  -- ^ Context to the computation
  -> HMS.HashMap StateId [(StateId, input)]
  -- ^ Inputs to the computation
  -> m [(SlaveId, HMS.HashMap StateId (HMS.HashMap StateId output))]
updateSlaves maxBatchSize mbMinBatchSize slaves context inputMap = do
  incomingChan :: TChan (SlaveId, UpdateSlaveResp output) <- liftIO newTChanIO
  -- Write one init message per slave
  forM_ (HMS.keys slaves) $ \slaveId ->
    atomically (writeTChan incomingChan (slaveId, USRespInit))
  outgoingChan :: TChan (SlaveId, UpdateSlaveReq) <- liftIO newTChanIO
  let slaveStatus0 = error "TODO"
      status = UpdateSlavesStatus
        { ussSlaves = const slaveStatus0 <$> slaves
        , ussOutputs = mempty
        }
  status' <- fmap (either (either absurd absurd) id) $ Async.race
    (Async.race
      (receiveLoops incomingChan (HMS.toList slaves))
      (sendLoop outgoingChan inputMap))
    (go incomingChan outgoingChan status)
  return (HMS.toList (ussOutputs status'))
  where
    go incomingChan outgoingChan status = do
      (slaveId, resp) <- atomically (readTChan incomingChan)
      (status', res) <- either throwIO return $
        updateSlavesStep maxBatchSize mbMinBatchSize slaveId resp status
      case res of
        USSDone -> return status'
        USSNotDone reqs -> do
          mapM_ (atomically . writeTChan outgoingChan) reqs
          go incomingChan outgoingChan status'

    receiveLoops slaveRespsChan = \case
      [] -> throwIO (MasterException "No slaves in updateSlaves, this should never happen")
      [(slId, sl)] -> receiveLoop slaveRespsChan slId sl
      (slId, sl) : sls ->
        fmap (either absurd absurd) $
        Async.race (receiveLoop slaveRespsChan slId sl) (receiveLoops slaveRespsChan sls)

    receiveLoop incomingChan slaveId slave = forever $ do
      resp0 <- scRead (slaveConnection slave)
      resp <- case resp0 of
        SRespResetState -> throwIO (UnexpectedResponse (displayResp resp0))
        SRespGetStates _ -> throwIO (UnexpectedResponse (displayResp resp0))
        SRespError err -> throwIO (ExceptionFromSlave err)
        SRespAddStates addedStates -> return (USRespAddStates addedStates)
        SRespRemoveStates requestingSlaveId removedStates ->
          return (USRespRemoveStates requestingSlaveId removedStates)
        SRespUpdate outputs -> return (USRespUpdate outputs)
      atomically (writeTChan incomingChan (slaveId, resp))

    sendLoop ::
         TChan (SlaveId, UpdateSlaveReq)
      -> HMS.HashMap StateId [(StateId, input)]
      -> m void
    sendLoop outgoingChan inputs = do
      (slaveId, req0) <- atomically (readTChan outgoingChan)
      (req, inputs') <- either throwIO return $ case req0 of
        USReqUpdate stateIds -> do
          let updateStates = HMS.fromList (zip stateIds (repeat ()))
          let updateInputs = inputs `HMS.intersection` updateStates
          remainingInputs <- inputs `strictMapDifference` updateStates
          return (SReqUpdate context (HMS.fromList <$> updateInputs), remainingInputs)
        USReqRemoveStates x y -> return (SReqRemoveStates x y, inputs)
        USReqAddStates y -> return (SReqAddStates y, inputs)
      scWrite (slaveConnection (slaves HMS.! slaveId)) req
      sendLoop outgoingChan inputs'

{-
updateSlaveThread :: forall state context input output m.
     (MonadConnect m)
  => Int -- ^ Max batch size
  -> Maybe Int -- ^ Maybe min batch size
  -> SlaveId -- ^ The slave we're operating on
  -> HMS.HashMap SlaveId (SlaveConn m state context input output)
  -- ^ Slaves connections
  -> MVar (HMS.HashMap SlaveId [StateId])
  -- ^ 'MVar's holding the remaining states for each slave
  -> context
  -- ^ Context to the computation
  -> HMS.HashMap StateId [(StateId, input)]
  -- ^ Inputs to the computation
  -> m (HMS.HashMap StateId (HMS.HashMap StateId output))
updateSlaveThread maxBatchSize mbMinBatchSize thisSlaveId slaves slaveStatesVar context inputMap = go
  where
    debug msg = logDebugNS "Distributed.Stateful.Master.updateSlaveThread" (pack msg)
    thisSlave = slaves HMS.! thisSlaveId

    go :: m (HMS.HashMap StateId (HMS.HashMap StateId output))
    go = do
      status <- modifyMVar slaveStatesVar $ \slaveStates0 -> do
        let thisRemainingStates = slaveStates0 HMS.! thisSlaveId
        if null thisRemainingStates -- TODO do something smarter here
          then do
            mbRequested <- stealStatesForSlave maxBatchSize slaveStates0
            return $ case mbRequested of
              Just (requestedFrom, requestedStates, slaveStates) ->
                (slaveStates, STSRequestStates requestedFrom requestedStates)
              Nothing ->
                (slaveStates0, STSStop)
          else return $ let
            (roundStates, thisRemainingStates') = splitAt maxBatchSize thisRemainingStates
            slaveStates = HMS.insert thisSlaveId thisRemainingStates' slaveStates0
            in (slaveStates, STSOk roundStates)
      let broadcastAndContinue roundStates = do
            let inputs = map (\k -> (k, HMS.fromList (inputMap HMS.! k))) roundStates
            scWrite thisSlave (SReqUpdate context (HMS.fromList inputs))
            roundOutputs <- readSelect thisSlave $ \case
              SRespUpdate outputs -> Just outputs
              _ -> Nothing
            moreOutputs <- go
            return (roundOutputs <> moreOutputs)
      case status of
        STSOk roundStates -> broadcastAndContinue roundStates
        STSStop -> return HMS.empty
        STSRequestStates requestFrom statesToRequest -> do
          debug (printf ("Transferring states from %d to %d %s\n")
              requestFrom thisSlaveId (show (map unStateId statesToRequest)))
          requestStatesForSlave requestFrom statesToRequest
          broadcastAndContinue statesToRequest

    -- | This is 'IO' just because we need to modify the 'IORef' holding
    -- the map recording which states are held by which slave.
    stealStatesForSlave ::
         Int
      -> HMS.HashMap SlaveId [StateId]
      -> m (Maybe (SlaveId, [StateId], HMS.HashMap SlaveId [StateId]))
      -- ^ If we could find some slave to transfer from, return its id, and the states we
      -- requested.
    stealStatesForSlave batchSize remainingStates = do
      let thisSlaveStates = remainingStates HMS.! thisSlaveId
      when (not (null thisSlaveStates)) $ throwAndLog $
        MasterException "broadcast.stealStatesForSlave: Expecting no states in thisSlaveId"
      let goodCandidate (slaveId, slaveRemainingStates) = do
            guard (slaveId /= thisSlaveId)
            let numSlaveRemainingStates = length slaveRemainingStates
            guard (numSlaveRemainingStates > batchSize)
            guard (min batchSize (numSlaveRemainingStates - batchSize) >= fromMaybe 0 mbMinBatchSize)
            return (slaveId, numSlaveRemainingStates, slaveRemainingStates)
      let candidates :: [(SlaveId, Int, [StateId])]
          candidates = catMaybes (map goodCandidate (HMS.toList remainingStates))
      if null candidates
        then return Nothing
        else do
          -- Pick candidate with highest number of states to steal states from
          let (candidateSlaveId, numCandidateStates, candidateStates) = maximumByEx (comparing (\(_, x, _) -> x)) candidates
          let statesToBeTransferredList = take (min batchSize (numCandidateStates - batchSize)) candidateStates
          let statesToBeTransferred = HS.fromList statesToBeTransferredList
          -- Bookkeep the remaining states
          let remainingCandidateStatesBefore = HS.fromList (remainingStates HMS.! candidateSlaveId)
          let remainingCandidateStatesNow = HS.toList (HS.difference remainingCandidateStatesBefore statesToBeTransferred)
          let remainingStates' = HMS.insert candidateSlaveId remainingCandidateStatesNow remainingStates
          return (Just (candidateSlaveId, statesToBeTransferredList, remainingStates'))

    requestStatesForSlave ::
         SlaveId
      -> [StateId]
      -> m ()
    requestStatesForSlave otherSlaveId statesToRequest = do
      let otherSlave = slaves HMS.! otherSlaveId
      -- Request the states
      scWrite otherSlave (SReqRemoveStates thisSlaveId (HS.fromList statesToRequest))
      -- Receive the states
      states <- readSelect otherSlave $ \case
        SRespRemoveStates slaveRequesting states | slaveRequesting == thisSlaveId -> Just states
        _ -> Nothing
      -- Upload states
      scWrite thisSlave (SReqAddStates states)
      readSelect thisSlave $ \case
        SRespAddStates -> Just ()
        _ -> Nothing
      return ()
-}

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
  return mh
    { mhSlaves = HMS.insert slaveId (Slave conn mempty) (mhSlaves mh) }

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
  -- Divide up the states among the slaves
  let slaves = mhSlaves mh
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
  -- Send states
  forM_ (HMS.toList slavesStates) $ \(slaveId, states) -> do
    scWrite (slaveConnection (slaves HMS.! slaveId)) (SReqResetState (HMS.fromList states))
  forM_ (HMS.elems slaves') $ \slave_ -> do
    readExpect (slaveConnection slave_) $ \case
      SRespResetState -> Just ()
      _ -> Nothing
  return (mh{mhSlaves = slaves'}, HMS.fromList allStates)

getStateIds ::
     (MonadConnect m)
  => MasterHandle m state context input output
  -> m (HMS.HashMap StateId ())
getStateIds = fmap (fold . fmap slaveStates . mhSlaves) . readMVar . unMasterHandle

-- | Fetches current states stored in the slaves.
{-# INLINE getStates #-}
getStates ::
     (MonadConnect m)
  => MasterHandle m state context input output
  -> m (HMS.HashMap StateId state)
getStates (MasterHandle mhv) = withMVar mhv $ \mh -> do
  -- Send states
  let slaves = mhSlaves mh
  forM_ slaves (\slave_ -> scWrite (slaveConnection slave_) SReqGetStates)
  responses <- forM (HMS.elems slaves) $ \slave_ -> do
    readExpect (slaveConnection slave_) $ \case
      SRespGetStates states -> Just states
      _ -> Nothing
  return (mconcat responses)

{-# INLINE readExpect #-}
readExpect ::
     (MonadIO m, MonadBaseControl IO m)
  => SlaveConn m state context input output
  -> (SlaveResp state output -> Maybe a)
  -> m a
readExpect slave f = do
  resp <- scRead slave
  case resp of
    SRespError err -> throwIO (ExceptionFromSlave err)
    _ -> case f resp of
      Nothing -> throwIO (UnexpectedResponse (displayResp resp))
      Just x -> return x
