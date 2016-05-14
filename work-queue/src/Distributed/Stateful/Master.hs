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
import           Control.Lens (makeLenses, set, at, _Just, over)
import           Control.Exception (throw)

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
  let throwME = throwAndLog . MasterException . pack
  case (maMinBatchSize, maMaxBatchSize) of
    (_, Just maxBatchSize) | maxBatchSize < 1 ->
      throwME (printf "Distribute.master: maMaxBatchSize must be > 0 (got %d)" maxBatchSize)
    (Just minBatchSize, _) | minBatchSize < 0 ->
      throwME (printf "Distribute.master: maMinBatchSize must be >= 0 (got %d)" minBatchSize)
    (Just minBatchSize, Just maxBatchSize) | minBatchSize > maxBatchSize ->
      throwME (printf "Distribute.master: maMinBatchSize can't be greater then maMaxBatchSize (got %d and %d)" minBatchSize maxBatchSize)
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
  => HS.HashSet a -> HS.HashSet a -> m (HS.HashSet a)
strictSetDifference a b = do
  let c = a `HS.difference` b
  unless (HS.size c == HS.size a - HS.size b) $
    throwM $ MasterException $ pack $
      printf "Bad set difference (strictDifference), %s - %s" (show a) (show b)
  return c

{-# INLINE strictMapDifference #-}
strictMapDifference ::
     (Show a, Eq a, Hashable a, MonadThrow m)
  => HMS.HashMap a b -> HMS.HashMap a c -> m (HMS.HashMap a b)
strictMapDifference a b = do
  let c = a `HMS.difference` b
  unless (HMS.size c == HMS.size a - HMS.size b) $
    throwM $ MasterException $ pack $
      printf "Bad set difference (strictDifference), %s - %s" (show (HMS.keys a)) (show (HMS.keys b))
  return c

data SlaveStatus = SlaveStatus
  { _ssRemaining :: ![StateId] -- ^ Free states that remain
  , _ssUpdating :: !(HS.HashSet StateId) -- ^ States we're currently updating
  , _ssRemoving :: !(HS.HashSet StateId) -- ^ States we're currently removing
  , _ssAdding :: !(HS.HashSet StateId) -- ^ States we're currently adding
  } deriving (Eq, Show)
makeLenses ''SlaveStatus

data UpdateSlaveResp output
  = USRespAddStates !(HS.HashSet StateId)
  | USRespRemoveStates !SlaveId (HMS.HashMap StateId ByteString)
  | USRespUpdate !(HMS.HashMap StateId (HMS.HashMap StateId output))
  | USRespInit
  deriving (Eq, Show)

data UpdateSlaveReq input
  = USReqUpdate !(HMS.HashMap StateId (HMS.HashMap StateId input))
  | USReqAddStates !(HMS.HashMap StateId ByteString)
  | USReqRemoveStates !SlaveId !(HS.HashSet StateId)
  deriving (Eq, Show)

data UpdateSlavesStatus input output = UpdateSlavesStatus
  { _ussSlaves :: !(HMS.HashMap SlaveId SlaveStatus)
  , _ussInputs :: !(HMS.HashMap StateId [(StateId, input)])
  , _ussOutputs :: !(HMS.HashMap SlaveId (HMS.HashMap StateId (HMS.HashMap StateId output)))
  }
makeLenses ''UpdateSlavesStatus

data UpdateSlaveStep input
  = USSDone
  | USSNotDone ![(SlaveId, UpdateSlaveReq input)]
  deriving (Eq, Show)

{-# INLINE updateSlavesStep #-}
updateSlavesStep :: forall m input output.
     (MonadThrow m)
  => Int -- ^ Max batch size
  -> Maybe Int -- ^ Maybe min batch size
  -> SlaveId
  -> UpdateSlaveResp output
  -> UpdateSlavesStatus input output
  -> m (UpdateSlavesStatus input output, UpdateSlaveStep input)
updateSlavesStep maxBatchSize mbMinBatchSize respSlaveId resp statuses = do
  case resp of
    USRespInit -> do
      findSomethingToUpdate respSlaveId statuses
    USRespUpdate outputs -> do
      -- Remove from ssUpdating in thisSlaveId, update the outputs, and then find something
      -- else to do.
      let removeUpdating updating =
            either throw id (strictSetDifference updating (HS.fromList (HMS.keys outputs)))
      -- HMS.union is not efficient with small thing into big things
      let insertAll xs m = case xs of
            [] -> m
            (k, v) : xs' -> insertAll xs' (HMS.insert k v m)
      let insertOutputs allOutputs = case HMS.lookup respSlaveId allOutputs of
            Nothing -> HMS.insert respSlaveId outputs allOutputs
            Just existingOutputs -> HMS.insert respSlaveId (insertAll (HMS.toList outputs) existingOutputs) allOutputs
      let statuses' =
            over (ussSlaves . at respSlaveId . _Just . ssUpdating) removeUpdating $
            over ussOutputs insertOutputs $
            statuses
      findSomethingToUpdate respSlaveId statuses'
    USRespAddStates statesIds -> do
      -- Move from ssAdding to ssRemaining in thisSlaveId, and then
      -- find something else to do.
      let removeAdding adding =
            either throw id (strictSetDifference adding statesIds)
      let moveToRemaining = over ssAdding removeAdding . over ssRemaining (++ HS.toList statesIds)
      let statuses' =
            over (ussSlaves . at respSlaveId . _Just) moveToRemaining statuses'
      findSomethingToUpdate respSlaveId statuses'
    USRespRemoveStates requestingSlaveId states -> do
      -- Move from ssRemoving in thisSlaveId to ssAdding in requestingSlaveId,
      -- and issue a request adding the slaves to requestingSlaveId
      let removeRemoving removing =
            either throw id (strictSetDifference removing (HS.fromList (HMS.keys states)))
      let addAdding = HS.union (HS.fromList (HMS.keys states))
      let statuses' =
            over (ussSlaves . at respSlaveId . _Just . ssRemoving) removeRemoving $
            over (ussSlaves . at requestingSlaveId . _Just . ssAdding) addAdding $
            statuses
      return (statuses', USSNotDone [(requestingSlaveId, USReqAddStates states)])
  where
    {-# INLINE findSomethingToUpdate #-}
    findSomethingToUpdate ::
         SlaveId
      -> UpdateSlavesStatus input output
      -> m (UpdateSlavesStatus input output, UpdateSlaveStep input)
    findSomethingToUpdate slaveId uss = do
      -- If we have some states in ssRemaining, just update those. Otherwise,
      -- try to steal from another slave.
      let ss = _ussSlaves uss HMS.! slaveId
      if null (_ssRemaining ss)
        then do
          -- We might be done, check
          if HMS.null (_ussInputs uss)
            then return (uss, USSDone)
            else do -- Steal slaves
              let mbStolen = stealSlavesFromSomebody slaveId uss
              case mbStolen of
                Nothing -> do -- Not done yet, but nothing to do for this resp
                  return (uss, USSNotDone [])
                Just (uss', stolenFrom, stolenSlaves) -> do -- Request stolen slaves
                  return (uss', USSNotDone [(stolenFrom, USReqRemoveStates slaveId stolenSlaves)])
        else do -- Send update command
          let (toUpdate0, remaining) = splitAt maxBatchSize (_ssRemaining ss)
          let toUpdateInputs = HMS.fromList
                [(k, HMS.fromList (_ussInputs uss HMS.! k)) | k <- toUpdate0]
          remainingInputs <- strictMapDifference
            (_ussInputs uss) (HMS.fromList (zip remaining (repeat ())))
          let uss' = set (ussSlaves . at slaveId . _Just . ssRemaining) remaining $
                     set ussInputs remainingInputs uss
          return (uss', USSNotDone [(slaveId, USReqUpdate toUpdateInputs)])

    {-# INLINE stealSlavesFromSomebody #-}
    stealSlavesFromSomebody ::
         SlaveId -- Requesting the slaves
      -> UpdateSlavesStatus input output
      -> Maybe (UpdateSlavesStatus input output, SlaveId, HS.HashSet StateId)
    stealSlavesFromSomebody requestingSlaveId uss = do
      let goodCandidate (slaveId, ss) = do
            guard (slaveId /= requestingSlaveId)
            let numRemaining = length (_ssRemaining ss)
            guard (numRemaining > 0)
            guard (max numRemaining maxBatchSize >= fromMaybe 0 mbMinBatchSize)
            return (slaveId, numRemaining, _ssRemaining ss)
      let candidates :: [(SlaveId, Int, [StateId])]
          candidates = catMaybes (map goodCandidate (HMS.toList (_ussSlaves uss)))
      guard (not (null candidates))
      -- Pick candidate with highest number of states to steal states from
      let (candidateSlaveId, numCandidateStates, candidateStates) = maximumByEx (comparing (\(_, x, _) -> x)) candidates
      let (statesToBeTransferred, remainingStates) = splitAt (max maxBatchSize numCandidateStates) candidateStates
      -- Bookkeep the remaining states
      let moveStatesToRemoving =
            set ssRemaining remainingStates .
            over ssRemoving (HS.union (HS.fromList statesToBeTransferred))
      let uss' = over (ussSlaves . at candidateSlaveId . _Just) moveStatesToRemoving uss
      return (uss', candidateSlaveId, HS.fromList statesToBeTransferred)

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
  outgoingChan :: TChan (SlaveId, UpdateSlaveReq input) <- liftIO newTChanIO
  let slaveStatus0 stateIds = SlaveStatus
        { _ssRemaining = HMS.keys stateIds
        , _ssAdding = mempty
        , _ssUpdating = mempty
        , _ssRemoving = mempty
        }
  let status = UpdateSlavesStatus
        { _ussSlaves = slaveStatus0 . slaveStates <$> slaves
        , _ussOutputs = mempty
        , _ussInputs = inputMap
        }
  status' <- fmap (either (either absurd absurd) id) $ Async.race
    (Async.race
      (receiveLoops incomingChan (HMS.toList slaves))
      (sendLoop outgoingChan))
    (go incomingChan outgoingChan status)
  return (HMS.toList (_ussOutputs status'))
  where
    go incomingChan outgoingChan status = do
      (slaveId, resp) <- atomically (readTChan incomingChan)
      (status', res) <-
        updateSlavesStep maxBatchSize mbMinBatchSize slaveId resp status
      case res of
        USSDone -> return status'
        USSNotDone reqs -> do
          mapM_ (atomically . writeTChan outgoingChan) reqs
          go incomingChan outgoingChan status'

    {-# INLINE receiveLoops #-}
    receiveLoops slaveRespsChan = \case
      [] -> throwIO (MasterException "No slaves in updateSlaves, this should never happen")
      [(slId, sl)] -> receiveLoop slaveRespsChan slId sl
      (slId, sl) : sls ->
        fmap (either absurd absurd) $
        Async.race (receiveLoop slaveRespsChan slId sl) (receiveLoops slaveRespsChan sls)

    {-# INLINE receiveLoop #-}
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

    {-# INLINE sendLoop #-}
    sendLoop outgoingChan = forever $ do
      (slaveId, req0) <- atomically (readTChan outgoingChan)
      let req = case req0 of
            USReqUpdate inputs -> SReqUpdate context inputs
            USReqRemoveStates x y -> SReqRemoveStates x y
            USReqAddStates y -> SReqAddStates y
      scWrite (slaveConnection (slaves HMS.! slaveId)) req

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
      Just maxBatchSize ->
        updateSlaves maxBatchSize (maMinBatchSize (mhArgs mh)) slaves context inputMap
  let mh' = mh
        { mhSlaves =
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
