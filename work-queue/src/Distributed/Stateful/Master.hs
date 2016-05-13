{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
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
    , getSlaveCount
    , addSlaveConnection
    , SlaveConn
    ) where

import           ClassyPrelude
import           Control.Concurrent.Async.Lifted.Safe (mapConcurrently)
import qualified Control.Concurrent.Async.Lifted.Safe as Async
import           Control.Monad.Logger (logDebugNS, MonadLogger)
import           Control.Monad.Trans.Control (MonadBaseControl)
import           Data.Foldable (foldlM)
import qualified Data.HashMap.Strict as HMS
import qualified Data.HashSet as HS
import           Data.List.Split (chunksOf)
import           Data.SimpleSupply
import           Distributed.Stateful.Internal
import           Text.Printf (printf)
import           Data.Mailbox

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

data MasterHandle m state context input output = MasterHandle
  { mhSlaveInfoRef :: !(IORef (SlaveInfo m state context input output))
  , mhSlaveCountTVar :: !(TVar Int)
  , mhSlaveIdSupply :: !(Supply SlaveId)
  , mhStateIdSupply :: !(Supply StateId)
  , mhArgs :: !MasterArgs
  }

type SlaveConn m state context input output =
  Mailbox m (SlaveReq state context input) (SlaveResp state output)

-- TODO: consider unifying these maps? Seemed like more refactoring work
-- than it's worth, but would reduce the usage of HMS.!
data SlaveInfo m state context input output = SlaveInfo
  { siConnections :: !(HMS.HashMap SlaveId (SlaveConn m state context input output))
  , siStates :: !(HMS.HashMap SlaveId (HS.HashSet StateId))
  }

data MasterException
  = MasterException Text
  | InputMissingException StateId
  | UnusedInputsException [StateId]
  | NoSlavesConnectedException
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
  slaveInfoRef <- newIORef SlaveInfo
    { siConnections = HMS.empty
    , siStates = HMS.empty
    }
  connectionCountVar <- liftIO $ newTVarIO 0
  slaveIdSupply <- newSupply (SlaveId 0) (\(SlaveId n) -> SlaveId (n + 1))
  stateIdSupply <- newSupply (StateId 0) (\(StateId n) -> StateId (n + 1))
  return MasterHandle
    { mhSlaveInfoRef = slaveInfoRef
    , mhSlaveCountTVar = connectionCountVar
    , mhSlaveIdSupply = slaveIdSupply
    , mhStateIdSupply = stateIdSupply
    , mhArgs = ma
    }

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
update :: forall state context input output m.
     (MonadIO m, MonadLogger m, MonadBaseControl IO m, Async.Forall (Async.Pure m))
  => MasterHandle m state context input output
  -> context
  -> HMS.HashMap StateId [input]
  -> m (HMS.HashMap StateId (HMS.HashMap StateId output))
update MasterHandle{..} context inputs0 = do
  -- Give state ids to each of the inputs, which will be used to label the
  -- state resulting from invoking saUpdate with that input.
  let sortedInputs = sortBy (comparing fst) (HMS.toList inputs0)
  inputMap <-
    withSupplyM mhStateIdSupply $
    fmap HMS.fromList $
    forM sortedInputs $ \(stateId, inps) ->
      (stateId,) <$> mapM (\input -> (, input) <$> askSupplyM) inps
  -- Update the slave states.
  si0 <- readIORef mhSlaveInfoRef
  let slaves = siConnections si0
  let go (rs, inputs) (slaveId, states) = do
        -- TODO: Other Map + Set datatypes have functions for doing this
        -- more efficiently / concisely.
        r <- forM (HS.toList states) $ \k ->
          case HMS.lookup k inputs of
            Nothing -> throwAndLog (InputMissingException k)
            Just input -> return (k, input)
        let inputs' = inputs `HMS.difference` (HMS.fromList (map (,()) (HS.toList states)))
        return ((slaveId, r):rs, inputs')
  (slaveIdsAndInputs :: [(SlaveId, [(StateId, [(StateId, input)])])], unusedInputs)
    <- foldlM go ([], inputMap) (HMS.toList (siStates si0))
  when (not (HMS.null unusedInputs)) $
    throwAndLog (UnusedInputsException (HMS.keys unusedInputs))
  outputs <- case maMaxBatchSize mhArgs of
    Nothing -> do
      forM_ slaveIdsAndInputs $ \(slaveId, inps) -> do
        mailboxWrite (slaves HMS.! slaveId) (SReqUpdate context (fmap HMS.fromList $ HMS.fromList inps))
      forM slaveIdsAndInputs $ \(slaveId, _) ->
        liftM (slaveId,) $ readSelect (slaves HMS.! slaveId) $ \case
          SRespUpdate outputs -> Just outputs
          _ -> Nothing
    Just maxBatchSize -> do
      slaveStatesVar <- newMVar (HMS.fromList (map (second (map fst)) slaveIdsAndInputs))
      let slaveThread slaveId = updateSlaveThread mhArgs context maxBatchSize slaveId slaves slaveStatesVar inputMap
      mapConcurrently (\slaveId -> (slaveId, ) <$> slaveThread slaveId) (HMS.keys slaves)
  atomicModifyIORef' mhSlaveInfoRef $ \si -> (, ()) $ si
    { siStates =
         HMS.fromList (map (second (foldMap (HS.fromList . HMS.keys))) outputs)
      -- Ensures that we have entries in siStates for every connection.
      <> HMS.map (\_ -> HS.empty) (siConnections si)
    }
  return $ foldMap snd outputs

data SlaveThreadStatus
  = STSOk
      ![StateId] -- States to execute
  | STSRequestStates
      !SlaveId -- Slave to request the states below from
      ![StateId]
  | STSStop
  deriving (Eq, Show)

updateSlaveThread
  :: forall state context input output m.
     (MonadIO m, MonadBaseControl IO m, MonadLogger m)
  => MasterArgs
  -> context
  -> Int -- ^ Max batch size
  -> SlaveId -- ^ The slave we're operating on
  -> HMS.HashMap SlaveId (SlaveConn m state context input output) -- ^ Slave connections
  -> MVar (HMS.HashMap SlaveId [StateId]) -- ^ 'MVar's holding the remaining states for each slave
  -> HMS.HashMap StateId [(StateId, input)] -- Inputs to the computation
  -> m (HMS.HashMap StateId (HMS.HashMap StateId output))
updateSlaveThread MasterArgs{..} context maxBatchSize thisSlaveId slaves slaveStatesVar inputMap = go
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
            mailboxWrite thisSlave (SReqUpdate context (HMS.fromList inputs))
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
            guard (min batchSize (numSlaveRemainingStates - batchSize) >= fromMaybe 0 maMinBatchSize)
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
      mailboxWrite otherSlave (SReqRemoveStates thisSlaveId (HS.fromList statesToRequest))
      -- Receive the states
      states <- readSelect otherSlave $ \case
        SRespRemoveStates slaveRequesting states | slaveRequesting == thisSlaveId -> Just states
        _ -> Nothing
      -- Upload states
      mailboxWrite thisSlave (SReqAddStates states)
      readSelect thisSlave $ \case
        SRespAddStates -> Just ()
        _ -> Nothing
      return ()

-- | Adds a connection to a slave. Unlike 'update', 'resetStates', etc,
-- this can be called concurrently with the other operations.
--
-- NOTE: this being able to be called concurrently is why we need to be
-- careful about atomically updating 'mhSlaveInfoRef'.
addSlaveConnection :: (MonadBaseControl IO m, MonadIO m)
  => MasterHandle m state context input output
  -> SlaveConn m state context input output
  -> m ()
addSlaveConnection MasterHandle{..} conn = do
  slaveId <- askSupply mhSlaveIdSupply
  atomicModifyIORef' mhSlaveInfoRef (\si -> (, ()) $ si
    { siConnections = HMS.insert slaveId conn (siConnections si)
    , siStates = HMS.insert slaveId HS.empty (siStates si)
    })
  atomically $ modifyTVar mhSlaveCountTVar (+1)

-- | This sets the states stored in the slaves. It distributes the
-- states among the currently connected slaves.
--
-- If no slaves are connected, this throws an exception.
resetStates :: forall state context input output m.
     (MonadBaseControl IO m, MonadIO m, MonadLogger m)
  => MasterHandle m state context input output
  -> [state]
  -> m (HMS.HashMap StateId state)
resetStates MasterHandle{..} states0 = do
  -- Get a list of StateIds for states
  allStates <- withSupplyM mhStateIdSupply $ mapM (\state -> (, state) <$> askSupplyM) states0
  -- Divide up the states among the slaves
  mres <- atomicModifyIORef' mhSlaveInfoRef $ \si ->
    let slaves = siConnections si
        numStatesPerSlave =
          let (chunkSize, leftover) = length states0 `quotRem` length slaves
          in if leftover == 0 then chunkSize else chunkSize + 1
        slavesStates :: HMS.HashMap SlaveId [(StateId, state)]
        slavesStates = HMS.fromList $ zip (HMS.keys slaves) $
          -- Note that some slaves might be initially empty. That's fine and inevitable.
          chunksOf numStatesPerSlave allStates ++ repeat []
        slaveStateIds :: HMS.HashMap SlaveId (HS.HashSet StateId)
        slaveStateIds = HMS.map (HS.fromList . map fst) slavesStates
    in if HMS.null slaves
       then (si, Nothing)
       else (si { siStates = slaveStateIds }, Just (slaves, slavesStates))
  case mres of
    Nothing -> throwAndLog NoSlavesConnectedException
    Just (slaves, slavesStates) -> do
      -- Send states
      forM_ (HMS.toList slavesStates) $ \(slaveId, states) -> do
        mailboxWrite (slaves HMS.! slaveId) (SReqResetState (HMS.fromList states))
      forM_ (HMS.elems slaves) $ \slave_ -> do
        readSelect slave_ $ \case
          SRespResetState -> Just ()
          _ -> Nothing
      return (HMS.fromList allStates)

getStateIds ::
     MasterHandle m state context input output
  -> IO (HS.HashSet StateId)
getStateIds = fmap (fold . siStates) . readIORef . mhSlaveInfoRef

-- | Fetches current states stored in the slaves.
getStates ::
     (MonadIO m, MonadBaseControl IO m)
  => MasterHandle m state context input output
  -> m (HMS.HashMap StateId state)
getStates MasterHandle{..} = do
  -- Send states
  slaves <- siConnections <$> readIORef mhSlaveInfoRef
  forM_ slaves (\slave_ -> mailboxWrite slave_ SReqGetStates)
  responses <- forM (HMS.elems slaves) $ \slave_ -> do
    readSelect slave_ $ \case
      SRespGetStates states -> Just states
      _ -> Nothing
  return (mconcat responses)

getSlaveCount
  :: MasterHandle m state context input output
  -> STM Int
getSlaveCount = readTVar . mhSlaveCountTVar

readSelect
  :: (MonadIO m, MonadBaseControl IO m)
  => SlaveConn m state context input output
  -> (SlaveResp state output -> Maybe a)
  -> m a
readSelect slave f = do
  eres <- atomically $ mailboxSelect slave $ \x -> case f x of
    Just y -> Just (Right y)
    Nothing -> case x of
      SRespError err -> Just (Left err)
      _ -> Nothing
  case eres of
    Left exc -> throwIO exc
    Right (Left err) -> throwIO (ExceptionFromSlave err)
    Right (Right res) -> return res
