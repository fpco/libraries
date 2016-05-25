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
  | UnexpectedResponse Text
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
            readExpect (slaveConnection slave) $ \case
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

data SlaveStatus = SlaveStatus
  { _ssRemaining :: ![StateId] -- ^ Free states that remain
  , _ssUpdating :: !(HS.HashSet StateId) -- ^ States we're currently updating
  , _ssRemoving :: !(HS.HashSet StateId) -- ^ States we're currently removing
  , _ssAdding :: !(HS.HashSet StateId) -- ^ States we're currently adding
  } deriving (Eq, Show, Generic)
makeLenses ''SlaveStatus
instance NFData SlaveStatus

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

data UpdateSlavesStatus input output = UpdateSlavesStatus
  { _ussSlaves :: !(HMS.HashMap SlaveId SlaveStatus)
  , _ussInputs :: !(HMS.HashMap StateId (HMS.HashMap StateId input))
  , _ussOutputs :: !(HMS.HashMap SlaveId (HMS.HashMap StateId (HMS.HashMap StateId output)))
  } deriving (Generic)
makeLenses ''UpdateSlavesStatus
instance (NFData input, NFData output) => NFData (UpdateSlavesStatus input output)

data UpdateSlaveStep input
  = USSDone
  | USSNotDone !(Maybe (SlaveId, UpdateSlaveReq input))
  deriving (Eq, Show, Generic)
instance NFData input => NFData (UpdateSlaveStep input)

{-# INLINE updateSlavesStep #-}
updateSlavesStep :: forall m input output.
     (MonadThrow m, MonadLogger m)
  => Int -- ^ Max batch size
  -> SlaveId
  -> UpdateSlaveResp output
  -> UpdateSlavesStatus input output
  -> m (UpdateSlavesStatus input output, UpdateSlaveStep input)
updateSlavesStep maxBatchSize respSlaveId resp statuses = case resp of
  USRespInit -> do
    findSomethingToUpdate respSlaveId statuses
  USRespUpdate outputs -> do
    -- Remove from ssUpdating in thisSlaveId, update the outputs, and then find something
    -- else to do.
    let removeUpdating updating =
          either throw id (strictSetDifference "USRespUpdate" updating (HS.fromList (HMS.keys outputs)))
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
          either throw id (strictSetDifference "USRespAddStates" adding statesIds)
    let moveToRemaining = over ssAdding removeAdding . over ssRemaining (++ HS.toList statesIds)
    let statuses' =
          over (ussSlaves . at respSlaveId . _Just) moveToRemaining statuses
    findSomethingToUpdate respSlaveId statuses'
  USRespRemoveStates requestingSlaveId states -> do
    -- Move from ssRemoving in thisSlaveId to ssAdding in requestingSlaveId,
    -- and issue a request adding the slaves to requestingSlaveId
    let removeRemoving removing =
          either throw id (strictSetDifference "USRespRemoveStates" removing (HS.fromList (HMS.keys states)))
    let addAdding = HS.union (HS.fromList (HMS.keys states))
    let statuses' =
          over (ussSlaves . at respSlaveId . _Just . ssRemoving) removeRemoving $
          over (ussSlaves . at requestingSlaveId . _Just . ssAdding) addAdding $
          statuses
    return (statuses', USSNotDone (Just (requestingSlaveId, USReqAddStates states)))
  where
    {-# INLINE areWeDone #-}
    areWeDone :: UpdateSlavesStatus input output -> Bool
    areWeDone uss =
      HMS.null (_ussInputs uss) && all (\ss -> HS.null (_ssUpdating ss)) (_ussSlaves uss)

    {-# INLINE findSomethingToUpdate #-}
    findSomethingToUpdate ::
         SlaveId
      -> UpdateSlavesStatus input output
      -> m (UpdateSlavesStatus input output, UpdateSlaveStep input)
    findSomethingToUpdate slaveId uss = do
      -- If we have some states in ssRemaining, just update those. Otherwise,
      -- try to steal from another slave.
      let ss = _ussSlaves uss HMS.! slaveId
      let (toUpdateInputs, remaining) = takeEnoughStatesToUpdate (_ussInputs uss) (_ssRemaining ss)
      if null toUpdateInputs
        then do
          -- We might be done, check
          if areWeDone uss
            then return (uss, USSDone)
            else do -- Steal slaves
              let mbStolen = stealSlavesFromSomebody slaveId uss
              case mbStolen of
                Nothing -> do -- We're done for this slave.
                  $logInfo ("Tried to get something to do for slave " ++ tshow (unSlaveId slaveId) ++ ", but couldn't find anything")
                  return (uss, USSNotDone Nothing)
                Just (uss', stolenFrom, stolenStates) -> do -- Request stolen slaves
                  $logInfo ("Stealing " ++ tshow (HS.size stolenStates) ++ " states from slave " ++ tshow (unSlaveId stolenFrom) ++ " for slave " ++ tshow (unSlaveId slaveId))
                  return (uss', USSNotDone (Just (stolenFrom, USReqRemoveStates slaveId stolenStates)))
        else do -- Send update command
          let toUpdateInputsMap = HMS.fromList toUpdateInputs
          remainingInputs <- strictMapDifference "findSomethingToUpdate" (_ussInputs uss) toUpdateInputsMap
          let moveToUpdating = \case
                Nothing -> throw (MasterException (pack (printf "findSomethingToUpdate: Couldn't find slave %d" slaveId)))
                Just ss_ -> if HS.null (_ssUpdating ss_)
                  then Just ss_{_ssRemaining = remaining, _ssUpdating = HS.fromList (map fst toUpdateInputs)}
                  else throw (MasterException (pack (printf "findSomethingToUpdate: ssUpdating wasn't null (%s)" slaveId (show (_ssUpdating ss)))))
          let uss' = over (ussSlaves . at slaveId) moveToUpdating $
                     set ussInputs remainingInputs uss
          return (uss', USSNotDone (Just (slaveId, USReqUpdate toUpdateInputsMap)))

    {-# INLINE takeEnoughStatesToUpdate #-}
    takeEnoughStatesToUpdate ::
         HMS.HashMap StateId (HMS.HashMap StateId input)
      -> [StateId]
      -> ([(StateId, HMS.HashMap StateId input)], [StateId])
    takeEnoughStatesToUpdate inputs remaining00 = go remaining00 0
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
      -> UpdateSlavesStatus input output
      -> Maybe (UpdateSlavesStatus input output, SlaveId, HS.HashSet StateId)
    stealSlavesFromSomebody requestingSlaveId uss = do
      let goodCandidate (slaveId, ss) = do
            guard (slaveId /= requestingSlaveId)
            guard (not (null (_ssRemaining ss)))
            let (toTransfer, remaining) = takeEnoughStatesToUpdate (_ussInputs uss) (_ssRemaining ss)
            return (slaveId, map fst toTransfer, sum (map (HMS.size . snd) toTransfer), remaining)
      let candidates :: [(SlaveId, [StateId], Int, [StateId])]
          candidates = catMaybes (map goodCandidate (HMS.toList (_ussSlaves uss)))
      guard (not (null candidates))
      -- Pick candidate with highest number of states to steal states from
      let (candidateSlaveId, statesToBeTransferred, _, remainingStates) =
            maximumByEx (comparing (\(_, _, x, _) -> x)) candidates
      -- Bookkeep the remaining states
      let moveStatesToRemoving =
            set ssRemaining remainingStates .
            over ssRemoving (HS.union (HS.fromList statesToBeTransferred))
      let uss' = over (ussSlaves . at candidateSlaveId . _Just) moveStatesToRemoving uss
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
  outgoingChan :: TChan (Maybe (SlaveConn m state context input output, UpdateSlaveReq input)) <- liftIO newTChanIO
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
  statusVar :: MVar (UpdateSlavesStatus input output) <- newMVar status
  void $ Async.concurrently
    (sendLoop outgoingChan)
    (finally
      (Async.runConcurrently (asum (map (Async.Concurrently . slaveLoop outgoingChan statusVar) (HMS.toList slaves))))
      (atomically (writeTChan outgoingChan Nothing)))
  HMS.toList . _ussOutputs <$> takeMVar statusVar
  where
    slaveLoop outgoingChan statusVar (slaveId, slave) = do
      incomingChan :: TChan (UpdateSlaveResp output) <- liftIO newTChanIO
      fmap (either absurd id) (Async.race (receiveLoop incomingChan) (go incomingChan USRespInit))
      where
        go incomingChan resp = do
          res <- modifyMVar statusVar (\status -> updateSlavesStep maxBatchSize slaveId resp status)
          case res of
            USSDone -> do
              $logInfo ("Slave " ++ tshow slaveId ++ " received USSDone, quitting")
              return ()
            USSNotDone reqs -> do
              forM_ reqs $ \(slaveId_, req) -> do
                atomically (writeTChan outgoingChan (Just (slaveConnection (slaves HMS.! slaveId_), req)))
              go incomingChan =<< atomically (readTChan incomingChan)

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

    sendLoop outgoingChan = do
      mbReq <- atomically (readTChan outgoingChan)
      case mbReq of
        Nothing -> return ()
        Just (slaveConn, req0) -> do
          let req = case req0 of
                USReqUpdate inputs -> SReqUpdate context inputs
                USReqRemoveStates x y -> SReqRemoveStates x y
                USReqAddStates y -> SReqAddStates y
          $logDebug ("Sending slave request to slave : " ++ displayReq req)
          scWrite slaveConn req
          sendLoop outgoingChan

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
              liftM (slaveId,) $ readExpect (slaveConnection (slaves HMS.! slaveId)) $ \case
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
    readExpect conn $ \case
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
        readExpect (slaveConnection slave_) $ \case
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


