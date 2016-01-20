{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE PackageImports #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TupleSections #-}
module Distributed.Stateful
  ( -- * Master
    MasterArgs(..)
  , MasterHandle
  , mkMasterHandle
    -- ** Operations
  , StateId(..)
  , update
  , resetStates
  , getStateIds
  , getStates
  , addSlaveConnection
  , SlaveNMAppData
    -- * Slave
  , SlaveArgs(..)
  , runSlave
  ) where

import           ClassyPrelude
import qualified Data.HashMap.Strict as HMS
import qualified Data.HashSet as HS
import           Data.SimpleSupply
import           Text.Printf (printf, PrintfArg(..))
import           Control.DeepSeq (force, NFData)
import           Control.Exception (evaluate)
import qualified Data.Streaming.NetworkMessage as NM
import qualified Data.Conduit.Network as CN
import qualified Data.Binary as B
import           Data.Binary.Orphans ()
import           Data.List.Split (chunksOf)
import           Control.Concurrent.Async (mapConcurrently)
import           Control.Monad.Trans.Control (MonadBaseControl)

-- | Arguments for 'runSlave'.
data SlaveArgs state context input output = SlaveArgs
  { saUpdate :: !(context -> input -> state -> IO (state, output))
    -- ^ Function run on the slave when 'update' is invoked on the
    -- master.
  , saInit :: !(IO ())
    -- ^ Action to run upon initial connection.
  , saClientSettings :: !CN.ClientSettings
    -- ^ Settings for the slave's TCP connection.
  , saNMSettings :: !NM.NMSettings
    -- ^ Settings for the connection to the master.
  }

newtype StateId = StateId {unStateId :: Int}
  deriving (Generic, Eq, Ord, Show, Hashable, NFData, B.Binary)
instance PrintfArg StateId where
  formatArg = formatArg . unStateId
  parseFormat = parseFormat . unStateId

data SlaveState state
  = SSNotInitialized
  | SSInitialized !(HMS.HashMap StateId state)
  deriving (Generic, Eq, Show, NFData, B.Binary)

data SlaveReq state context input
  = SReqResetState
      !(HMS.HashMap StateId state) -- New states
  | SReqAddStates
      !(HMS.HashMap StateId state) -- States to add
  | SReqRemoveStates
      !SlaveId
      !(HS.HashSet StateId) -- States to get
  | SReqUpdate
      !context
      !(HMS.HashMap StateId (HMS.HashMap StateId input))
      -- The outer map tells us which states to update. The inner map
      -- provides the new StateIds, and the inputs which should be
      -- provided to 'saUpdate'.
  | SReqGetStates
  deriving (Generic, Eq, Show, NFData, B.Binary)

data SlaveResp state output
  = SRespResetState
  | SRespAddStates
  | SRespRemoveStates
      !SlaveId
      !(HMS.HashMap StateId state)
  | SRespUpdate !(HMS.HashMap StateId (HMS.HashMap StateId output)) -- TODO consider making this a simple list -- we don't really need it to be a HMS.
  | SRespGetStates !(HMS.HashMap StateId state)
  deriving (Generic, Eq, Show, NFData, B.Binary)

-- | Runs a stateful slave, and never returns (though may throw
-- exceptions).
runSlave :: forall state context input output m a.
     ( B.Binary state, NFData state, B.Binary context, B.Binary input, B.Binary output
     , MonadIO m
     )
  => SlaveArgs state context input output -> m a
runSlave SlaveArgs{..} = liftIO $ do
    liftIO $ CN.runTCPClient saClientSettings $
      NM.generalRunNMApp saNMSettings (const "") (const "") $ \nm -> do
        saInit
        go (NM.nmRead nm) (NM.nmWrite nm) SSNotInitialized
  where
    go :: forall b.
         IO (SlaveReq state context input)
      -> (SlaveResp state output -> IO ())
      -> SlaveState state
      -> IO b
    go recv send = let
      loop slaveState = do
        req <- recv
        let getStates' :: String -> HMS.HashMap StateId state
            getStates' err = case slaveState of
              SSNotInitialized -> error ("slave: state not initialized (" ++ err ++ ")")
              SSInitialized states0 -> states0
        case req of
          SReqResetState states -> do
            send SRespResetState
            loop (SSInitialized states)
          SReqAddStates newStates -> do
            let states' = HMS.unionWith (error "slave: adding existing states") (getStates' "SReqAddStates") newStates
            send SRespAddStates
            loop (SSInitialized states')
          SReqRemoveStates slaveRequesting stateIdsToDelete -> do
            let states = getStates' "SReqRemoveStates"
            let states' = foldl' (flip HMS.delete) states stateIdsToDelete
            let statesToSend = HMS.fromList
                  [(stateId, states HMS.! stateId) | stateId <- HS.toList stateIdsToDelete]
            send (SRespRemoveStates slaveRequesting statesToSend)
            loop (SSInitialized states')
          SReqUpdate context inputs -> do
            let states0 = getStates' "SReqBroadcast"
            results <- fmap HMS.fromList $ forM (HMS.toList inputs) $ \(oldStateId, innerInputs) -> do
              let state = case HMS.lookup oldStateId states0 of
                    Nothing -> error (printf "slave: Could not find state %d (SReqBroadcast)" oldStateId)
                    Just state0 -> state0
              fmap ((oldStateId, ) . HMS.fromList) $ forM (HMS.toList innerInputs) $ \(newStateId, input) ->
                fmap (newStateId, ) $ saUpdate context input state
            let states = foldMap (fmap fst) results
            let outputs = fmap (fmap snd) results
            void (evaluate (force states))
            send (SRespUpdate outputs)
            loop (SSInitialized states)
          SReqGetStates -> do
            let states = getStates' "SReqGetStates"
            send (SRespGetStates states)
            loop (SSInitialized states)
      in loop

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

newtype SlaveId = SlaveId {unSlaveId :: Int}
  deriving (Generic, Eq, Ord, Show, Hashable, NFData, B.Binary)
instance PrintfArg SlaveId where
  formatArg = formatArg . unSlaveId
  parseFormat = parseFormat . unSlaveId

type SlaveNMAppData state context input output =
  NM.NMAppData (SlaveReq state context input) (SlaveResp state output)

data MasterHandle state context input output = MasterHandle
  { mhSlaveInfoRef :: !(IORef (SlaveInfo state context input output))
  , mhSlaveIdSupply :: !(Supply SlaveId)
  , mhStateIdSupply :: !(Supply StateId)
  , mhArgs :: !MasterArgs
  }

-- TODO: consider unifying these maps? Seemed like more refactoring work
-- than it's worth, but would reduce the usage of HMS.!
data SlaveInfo state context input output = SlaveInfo
  { siConnections :: HMS.HashMap SlaveId (SlaveNMAppData state context input output)
  , siStates :: HMS.HashMap SlaveId (HS.HashSet StateId)
  }

-- | Create a new 'MasterHandle' based on the 'MasterArgs'. This
-- 'MasterHandle' should be used for all interactions with this
-- particular stateful computation (via 'update' and 'resample').
mkMasterHandle
  :: (MonadBaseControl IO m, MonadIO m)
  => MasterArgs
  -> m (MasterHandle state context input output)
mkMasterHandle ma@MasterArgs{..}= do
  case (maMinBatchSize, maMaxBatchSize) of
    (_, Just maxBatchSize) | maxBatchSize < 1 ->
      fail (printf "Distribute.master: maMaxBatchSize must be > 0 (got %d)" maxBatchSize)
    (Just minBatchSize, _) | minBatchSize < 0 ->
      fail (printf "Distribute.master: maMinBatchSize must be >= 0 (got %d)" minBatchSize)
    (Just minBatchSize, Just maxBatchSize) | minBatchSize > maxBatchSize ->
      fail (printf "Distribute.master: maMinBatchSize can't be greater then maMaxBatchSize (got %d and %d)" minBatchSize maxBatchSize)
    _ ->
      return ()
  slaveInfoRef <- newIORef SlaveInfo
    { siConnections = HMS.empty
    , siStates = HMS.empty
    }
  slaveIdSupply <- newSupply (SlaveId 0) (\(SlaveId n) -> SlaveId (n + 1))
  stateIdSupply <- newSupply (StateId 0) (\(StateId n) -> StateId (n + 1))
  return MasterHandle
    { mhSlaveInfoRef = slaveInfoRef
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
     (MonadIO m)
  => MasterHandle state context input output
  -> context
  -> HMS.HashMap StateId [input]
  -> m (HMS.HashMap StateId (HMS.HashMap StateId output))
update MasterHandle{..} context inputs = liftIO $ do
  -- Give state ids to each of the inputs, which will be used to label the
  -- state resulting from invoking saUpdate with that input.
  let sortedInputs = sortBy (comparing fst) (HMS.toList inputs)
  inputMap <-
    withSupplyM mhStateIdSupply $
    fmap HMS.fromList $
    forM sortedInputs $ \(stateId, inps) ->
      (stateId,) <$> mapM (\input -> (, input) <$> askSupplyM) inps
  -- Update the slave states.
  si0 <- readIORef mhSlaveInfoRef
  let slaves = siConnections si0
  let slaveIdsAndInputs :: [(SlaveId, [(StateId, [(StateId, input)])])]
      slaveIdsAndInputs =
        map (\(slaveId, states) ->
              (slaveId, mapMaybe (\k -> (k,) <$> HMS.lookup k inputMap) (HS.toList states)))
            (HMS.toList (siStates si0))
  outputs <- case maMaxBatchSize mhArgs of
    Nothing -> do
      forM_ slaveIdsAndInputs $ \(slaveId, inps) -> do
        NM.nmWrite (slaves HMS.! slaveId) (SReqUpdate context (fmap HMS.fromList $ HMS.fromList inps))
      forM slaveIdsAndInputs $ \(slaveId, _) ->
        liftM (slaveId,) $ NM.nmReadSelect (slaves HMS.! slaveId) $ \case
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
  :: forall state context input output.
     MasterArgs
  -> context
  -> Int -- ^ Max batch size
  -> SlaveId -- ^ The slave we're operating on
  -> HMS.HashMap SlaveId (SlaveNMAppData state context input output) -- ^ Slave connections
  -> MVar (HMS.HashMap SlaveId [StateId]) -- ^ 'MVar's holding the remaining states for each slave
  -> HMS.HashMap StateId [(StateId, input)] -- Inputs to the computation
  -> IO (HMS.HashMap StateId (HMS.HashMap StateId output))
updateSlaveThread MasterArgs{..} context maxBatchSize thisSlaveId slaves slaveStatesVar inputMap = go
  where
    thisSlave = slaves HMS.! thisSlaveId

    go :: IO (HMS.HashMap StateId (HMS.HashMap StateId output))
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
            NM.nmWrite thisSlave (SReqUpdate context (HMS.fromList inputs))
            roundOutputs <- NM.nmReadSelect thisSlave $ \case
              SRespUpdate outputs -> Just outputs
              _ -> Nothing
            moreOutputs <- go
            return (roundOutputs <> moreOutputs)
      case status of
        STSOk roundStates -> broadcastAndContinue roundStates
        STSStop -> return HMS.empty
        STSRequestStates requestFrom statesToRequest -> do
          -- printf ("Transferring states from %d to %d %s\n")
          --   requestFrom thisSlaveId (show (map unStateId (HS.toList statesToRequest)))
          requestStatesForSlave requestFrom statesToRequest
          broadcastAndContinue statesToRequest

    -- | This is 'IO' just because we need to modify the 'IORef' holding
    -- the map recording which states are held by which slave.
    stealStatesForSlave ::
         Int
      -> HMS.HashMap SlaveId [StateId]
      -> IO (Maybe (SlaveId, [StateId], HMS.HashMap SlaveId [StateId]))
      -- ^ If we could find some slave to transfer from, return its id, and the states we
      -- requested.
    stealStatesForSlave batchSize remainingStates = do
      let thisSlaveStates = remainingStates HMS.! thisSlaveId
      when (not (null thisSlaveStates)) $
        fail "broadcast.stealStatesForSlave: Expecting no states in thisSlaveId"
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
      -> IO ()
    requestStatesForSlave otherSlaveId statesToRequest = do
      let otherSlave = slaves HMS.! otherSlaveId
      -- Request the states
      NM.nmWrite otherSlave (SReqRemoveStates thisSlaveId (HS.fromList statesToRequest))
      -- Receive the states
      states <- NM.nmReadSelect otherSlave $ \case
        SRespRemoveStates slaveRequesting states | slaveRequesting == thisSlaveId -> Just states
        _ -> Nothing
      -- Upload states
      NM.nmWrite thisSlave (SReqAddStates states)
      NM.nmReadSelect thisSlave $ \case
        SRespAddStates -> Just ()
        _ -> Nothing
      return ()

-- | Adds a connection to a slave. Unlike 'update', 'resetStates', etc,
-- this can be called concurrently with the other operations.
--
-- NOTE: this being able to be called concurrently is why we need to be
-- careful about atomically updating 'mhSlaveInfoRef'.
addSlaveConnection :: (MonadBaseControl IO m, MonadIO m)
  => MasterHandle state context input output
  -> SlaveNMAppData state context input output
  -> m ()
addSlaveConnection MasterHandle{..} conn = do
  slaveId <- askSupply mhSlaveIdSupply
  atomicModifyIORef' mhSlaveInfoRef (\si -> (, ()) $ si
    { siConnections = HMS.insert slaveId conn (siConnections si)
    , siStates = HMS.insert slaveId HS.empty (siStates si)
    })

-- | This sets the states stored in the slaves. It distributes the
-- states among the currently connected slaves.
--
-- If no slaves are connected, this throws an exception.
resetStates :: forall state context input output m.
     ( MonadBaseControl IO m, MonadIO m
     , B.Binary state, B.Binary context, B.Binary input, B.Binary output
     )
  => MasterHandle state context input output
  -> [state]
  -> m (HMS.HashMap StateId state)
resetStates MasterHandle{..} states0 = do
  -- Get a list of StateIds for states
  let numStates = length states0
  allStates <- withSupplyM mhStateIdSupply $ mapM (\state -> (, state) <$> askSupplyM) states0
  -- Divide up the states among the slaves
  mres <- atomicModifyIORef' mhSlaveInfoRef $ \si ->
    let slaves = siConnections si
        numStatesPerSlave = getNumStatesPerSlave (length slaves) numStates
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
    Nothing -> fail "Can't resetStates unless some slaves are connected."
    Just (slaves, slavesStates) -> do
      -- Send states
      forM_ (HMS.toList slavesStates) $ \(slaveId, states) -> do
        NM.nmWrite (slaves HMS.! slaveId) (SReqResetState (HMS.fromList states))
      forM_ (HMS.elems slaves) $ \slave_ -> do
        NM.nmReadSelect slave_ $ \case
          SRespResetState -> Just ()
          _ -> Nothing
      return (HMS.fromList allStates)

getNumStatesPerSlave ::
     Int -- ^ Number of nodes
  -> Int -- ^ Number of particles
  -> Int -- ^ Number of particles per node (rounded up)
getNumStatesPerSlave numNodes numParticles = if
  | numNodes < 1 -> error "numStatesPerSlave: length iaNodes < 1"
  | numParticles < 1 -> error "numStatesPerSlave: length iaInitialParticles < 1"
  | otherwise -> let
      (chunkSize, leftover) = numParticles `quotRem` numNodes
      in if leftover == 0 then chunkSize else chunkSize + 1

getStateIds ::
     MasterHandle state context input output
  -> IO (HS.HashSet StateId)
getStateIds = fmap (fold . siStates) . readIORef . mhSlaveInfoRef

-- | Fetches current states stored in the slaves.
getStates ::
     MasterHandle state context input output
  -> IO (HMS.HashMap StateId state)
getStates MasterHandle{..} = do
  -- Send states
  slaves <- siConnections <$> readIORef mhSlaveInfoRef
  forM_ slaves (\slave_ -> NM.nmWrite slave_ SReqGetStates)
  fmap mconcat $ forM (HMS.elems slaves) $ \slave_ -> do
    NM.nmReadSelect slave_ $ \case
      SRespGetStates states -> Just states
      _ -> Nothing
