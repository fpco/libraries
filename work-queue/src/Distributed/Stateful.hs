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
  , StateId
  , update
  , resetStates
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
data SlaveArgs state updatePayload updateOutput resampleContext resamplePayload resampleOutput = SlaveArgs
  { saUpdate :: !(updatePayload -> state -> IO (updateOutput, state))
    -- ^ Function run on the slave when 'update' is invoked on the
    -- master.
  , saResample :: !(resampleContext -> resamplePayload -> state -> IO (resampleOutput, state))
    -- ^ Function run on the slave when 'resample' is invoked on the
    -- master.
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

data SlaveReq state updatePayload resampleContext resamplePayload
  = SReqResetState
      !(HMS.HashMap StateId state) -- New states
  | SReqAddStates
      !(HMS.HashMap StateId state) -- States to add
  | SReqRemoveStates
      !SlaveId
      !(HS.HashSet StateId) -- States to get
  | SReqBroadcast
      !updatePayload -- With the given payload
      !(HS.HashSet StateId) -- Update the given states
  | SReqResample
      !resampleContext
      !(HMS.HashMap StateId (HMS.HashMap StateId resamplePayload))
      -- This map tells us to reasample the 'StateId' in the keys using the 'resamplePayload's
      -- in the value. The 'StateId' in the key for the value indicates what the new 'StateId'
      -- shall be.
  | SReqGetStates
  deriving (Generic, Eq, Show, NFData, B.Binary)

data SlaveResp state updateOutput resampleOutput
  = SRespResetState
  | SRespAddStates
  | SRespRemoveStates
      !SlaveId
      !(HMS.HashMap StateId state)
  | SRespBroadcast !(HMS.HashMap StateId updateOutput) -- TODO consider making this a simple list -- we don't really need it to be a HMS.
  | SRespResample !(HMS.HashMap StateId resampleOutput)
  | SRespGetStates !(HMS.HashMap StateId state)
  deriving (Generic, Eq, Show, NFData, B.Binary)

-- | Runs a stateful slave, and never returns (though may throw
-- exceptions).
runSlave :: forall state updatePayload updateOutput resampleContext resamplePayload resampleOutput m a.
     ( B.Binary state, NFData state, B.Binary updatePayload, B.Binary updateOutput, B.Binary resampleContext, B.Binary resamplePayload, B.Binary resampleOutput
     , MonadIO m
     )
  => SlaveArgs state updatePayload updateOutput resampleContext resamplePayload resampleOutput -> m a
runSlave SlaveArgs{..} = liftIO $ do
    liftIO $ CN.runTCPClient saClientSettings $
      NM.generalRunNMApp saNMSettings (const "") (const "") $ \nm -> do
        go (NM.nmRead nm) (NM.nmWrite nm) SSNotInitialized
  where
    go :: forall b.
         IO (SlaveReq state updatePayload resampleContext resamplePayload)
      -> (SlaveResp state updateOutput resampleOutput -> IO ())
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
          SReqBroadcast payload stateIds -> do
            let states0 = getStates' "SReqBroadcast"
            let foldStep getResults stateId = do
                  (outputs, states) <- getResults
                  let state = case HMS.lookup stateId states0 of
                        Nothing -> error (printf "slave: Could not find state %d (SReqBroadcast)" stateId)
                        Just state0 -> state0
                  (output, newState) <- saUpdate payload state
                  return (HMS.insert stateId output outputs, HMS.insert stateId newState states)
            (outputs, states) <- foldl' foldStep (return (HMS.empty, states0)) stateIds
            void (evaluate (force states))
            send (SRespBroadcast outputs)
            loop (SSInitialized states)
          SReqResample context payloadss -> do
            let states0 = getStates' "SReqResample"
            let foldStep getResults oldStateId resamples = do
                  results <- getResults
                  let state = case HMS.lookup oldStateId states0 of
                        Nothing -> error (printf "slave: Could not find state %d (SReqResample)" oldStateId)
                        Just state0 -> state0
                  newResults <- HMS.fromList <$> sequence
                    [ (newStateId, ) <$> saResample context payload state
                    | (newStateId, payload) <- HMS.toList resamples
                    ]
                  return (newResults <> results)
            results <- HMS.foldlWithKey' foldStep (return HMS.empty) payloadss
            let states = HMS.map snd results
            void (evaluate (force states))
            send (SRespResample (HMS.map fst results))
            loop (SSInitialized states)
          SReqGetStates -> do
            let states = getStates' "SReqGetStates"
            send (SRespGetStates states)
            loop (SSInitialized states)
      in loop

-- | Arguments for 'mkMasterHandle'
data MasterArgs state = MasterArgs
  { maMaxBatchSize :: !(Maybe Int)
    -- ^ The maximum amount of states that will be transferred at once. If 'Nothing', they
    -- will be all transferred at once, and no "rebalancing" will ever happen.
    -- Moreover, if 'Nothing', 'maMinBatchSize' will be ignored.
  , maMinBatchSize :: !(Maybe Int)
    -- ^ The minimum amount of states that will be transferred at once. 'Nothing' is equivalent to
    -- @'Just' 0@.
  , maNMSettings :: !NM.NMSettings
    -- ^ Settings for the connection with the slaves.
  }

newtype SlaveId = SlaveId {unSlaveId :: Int}
  deriving (Generic, Eq, Ord, Show, Hashable, NFData, B.Binary)
instance PrintfArg SlaveId where
  formatArg = formatArg . unSlaveId
  parseFormat = parseFormat . unSlaveId

type SlaveNMAppData state updatePayload updateOutput resampleContext resamplePayload resampleOutput =
  NM.NMAppData (SlaveReq state updatePayload resampleContext resamplePayload) (SlaveResp state updateOutput resampleOutput)

data MasterHandle state updatePayload updateOutput resampleContext resamplePayload resampleOutput = MasterHandle
  { mhSlavesRef :: !(IORef (HMS.HashMap SlaveId (SlaveNMAppData state updatePayload updateOutput resampleContext resamplePayload resampleOutput)))
  , mhSlavesStatesRef :: !(IORef (HMS.HashMap SlaveId (HS.HashSet StateId)))
  , mhSlaveIdSupply :: !(Supply SlaveId)
  , mhStateIdSupply :: !(Supply StateId)
  , mhArgs :: MasterArgs state
  }

-- | Create a new 'MasterHandle' based on the 'MasterArgs'. This
-- 'MasterHandle' should be used for all interactions with this
-- particular stateful computation (via 'update' and 'resample').
mkMasterHandle
  :: (MonadBaseControl IO m, MonadIO m)
  => MasterArgs state
  -> m (MasterHandle state updatePayload updateOutput resampleContext resamplePayload resampleOutput)
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
  slavesRef <- newIORef HMS.empty
  slavesStatesRef <- newIORef HMS.empty
  slaveIdSupply <- newSupply (SlaveId 0) (\(SlaveId n) -> SlaveId (n + 1))
  stateIdSupply <- newSupply (StateId 0) (\(StateId n) -> StateId (n + 1))
  return MasterHandle
    { mhSlavesRef = slavesRef
    , mhSlavesStatesRef = slavesStatesRef
    , mhSlaveIdSupply = slaveIdSupply
    , mhStateIdSupply = stateIdSupply
    , mhArgs = ma
    }

data SlaveThreadStatus
  = STSOk
      !(HS.HashSet StateId) -- States to execute
  | STSRequestStates
      !SlaveId -- Slave to request the states below from
      !(HS.HashSet StateId)
  | STSStop
  deriving (Eq, Show)

-- | Send an update request to all the slaves. This will cause each of
-- the slaves to apply 'saUpdate' to each of its states. The outputs of
-- these invocations are returned in a 'HashMap'.
--
-- It rebalances the states among the slaves during this process. This
-- way, if some of the slaves finish early, they can help the others
-- out, by moving their workitems elsewhere.
--
-- NOTE: it is up to the client to not run this concurrently with
-- another 'update' or 'resample'.
update :: forall state updatePayload updateOutput resampleContext resamplePayload resampleOutput m.
     (MonadIO m)
  => MasterHandle state updatePayload updateOutput resampleContext resamplePayload resampleOutput
  -> updatePayload
  -> m (HMS.HashMap StateId updateOutput)
update mh@MasterHandle{..} payload = liftIO $ case maMaxBatchSize mhArgs of
  Nothing -> do
    slaves <- readIORef mhSlavesRef
    slavesStates <- readIORef mhSlavesStatesRef
    forM_ (HMS.toList slavesStates) $ \(slaveId, slaveStatesIds) -> do
      NM.nmWrite (slaves HMS.! slaveId) (SReqBroadcast payload slaveStatesIds)
    fmap mconcat $ forM (HMS.elems slaves) $ \slave_ -> do
      NM.nmReadSelect slave_ $ \case
        SRespBroadcast outputs -> Just outputs
        _ -> Nothing
  Just maxBatchSize -> do
    slaves <- readIORef mhSlavesRef
    slavesStates <- readIORef mhSlavesStatesRef
    remainingStates <- newMVar (HS.toList <$> slavesStates)
    mconcat <$> mapConcurrently (\slaveId -> updateSlaveThread mh payload maxBatchSize slaveId slaves remainingStates) (HMS.keys slaves)

updateSlaveThread
  :: forall state updatePayload updateOutput resampleContext resamplePayload resampleOutput.
     MasterHandle state updatePayload updateOutput resampleContext resamplePayload resampleOutput
  -> updatePayload
  -> Int -- ^ Max batch size
  -> SlaveId -- ^ The slave we're operating on
  -> HMS.HashMap SlaveId (SlaveNMAppData state updatePayload updateOutput resampleContext resamplePayload resampleOutput) -- ^ Slave connections
  -> MVar (HMS.HashMap SlaveId [StateId]) -- ^ 'MVar's holding the remaining states for each slave
  -> IO (HMS.HashMap StateId updateOutput)
updateSlaveThread MasterHandle{..} payload maxBatchSize thisSlaveId slaves slaveStatesVar = go
  where
    thisSlave = slaves HMS.! thisSlaveId

    go :: IO (HMS.HashMap StateId updateOutput)
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
            in (slaveStates, STSOk (HS.fromList roundStates))
      let broadcastAndContinue roundStates = do
            NM.nmWrite thisSlave (SReqBroadcast payload roundStates)
            roundOutputs <- NM.nmReadSelect thisSlave $ \case
              SRespBroadcast outputs -> Just outputs
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
      -> IO (Maybe (SlaveId, HS.HashSet StateId, HMS.HashMap SlaveId [StateId]))
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
            guard (min batchSize (numSlaveRemainingStates - batchSize) >= fromMaybe 0 (maMinBatchSize mhArgs))
            return (slaveId, numSlaveRemainingStates, slaveRemainingStates)
      let candidates :: [(SlaveId, Int, [StateId])]
          candidates = catMaybes (map goodCandidate (HMS.toList remainingStates))
      if null candidates
        then return Nothing
        else do
          -- Pick candidate with highest number of states to steal states from
          let (candidateSlaveId, numCandidateStates, candidateStates) = maximumByEx (comparing (\(_, x, _) -> x)) candidates
          let statesToBeTransferred = HS.fromList (take (min batchSize (numCandidateStates - batchSize)) candidateStates)
          -- Bookkeep the remaining states
          let remainingCandidateStatesBefore = HS.fromList (remainingStates HMS.! candidateSlaveId)
          let remainingCandidateStatesNow = HS.toList (HS.difference remainingCandidateStatesBefore statesToBeTransferred)
          let remainingStates' = HMS.insert candidateSlaveId remainingCandidateStatesNow remainingStates
          -- Bookkeep the global states bookkeeping
          modifyIORef' mhSlavesStatesRef $ \slaveStates -> let
            candidateStatesNow = HS.difference (slaveStates HMS.! candidateSlaveId) statesToBeTransferred
            thisStatesNow = HS.union (slaveStates HMS.! thisSlaveId) statesToBeTransferred
            in HMS.insert candidateSlaveId candidateStatesNow (HMS.insert thisSlaveId thisStatesNow slaveStates)
          return (Just (candidateSlaveId, statesToBeTransferred, remainingStates'))

    requestStatesForSlave ::
         SlaveId
      -> HS.HashSet StateId
      -> IO ()
    requestStatesForSlave otherSlaveId statesToRequest = do
      -- Request the states
      NM.nmWrite (slaves HMS.! otherSlaveId) (SReqRemoveStates thisSlaveId statesToRequest)
      -- Receive the states
      states <- NM.nmReadSelect (slaves HMS.! otherSlaveId) $ \case
        SRespRemoveStates slaveRequesting states | slaveRequesting == thisSlaveId -> Just states
        _ -> Nothing
      -- Upload states
      NM.nmWrite thisSlave (SReqAddStates states)
      NM.nmReadSelect thisSlave $ \case
        SRespAddStates -> Just ()
        _ -> Nothing
      return ()

addSlaveConnection :: (MonadBaseControl IO m, MonadIO m)
  => MasterHandle state updatePayload updateOutput resampleContext resamplePayload resampleOutput
  -> SlaveNMAppData state updatePayload updateOutput resampleContext resamplePayload resampleOutput
  -> m ()
addSlaveConnection MasterHandle{..} conn = do
    slaveId <- askSupply mhSlaveIdSupply
    atomicModifyIORef' mhSlavesStatesRef (\mp -> (HMS.insert slaveId HS.empty mp, ()))
    atomicModifyIORef' mhSlavesRef (\mp -> (HMS.insert slaveId conn mp, ()))

-- | This sets the states stored in the slaves. It distributes the
-- states among the currently connected slaves.
--
-- If no slaves are connected, this throws an exception.
resetStates :: forall state updatePayload updateOutput resampleContext resamplePayload resampleOutput m.
     ( MonadBaseControl IO m, MonadIO m
     , B.Binary state, B.Binary updatePayload, B.Binary updateOutput, B.Binary resampleContext, B.Binary resamplePayload, B.Binary resampleOutput
     )
  => MasterHandle state updatePayload updateOutput resampleContext resamplePayload resampleOutput
  -> [state]
  -> m (HMS.HashMap StateId state)
resetStates MasterHandle{..} states0 = do
  slavesMap <- readIORef mhSlavesRef
  when (HMS.null slavesMap) $ fail "Can't resetStates unless some slaves are connected."
  -- Get a list of StateIds for states
  let numStates = length states0
  statesIds <- withSupplyM mhStateIdSupply $ mapM (\_ -> askSupplyM) [1..numStates]
  -- Divide up the states among the slaves
  let slaves = HMS.toList slavesMap
  let numStatesPerSlave = getNumStatesPerSlave (length slaves) numStates
  let slavesStates = HMS.fromList $ zip (map fst slaves) $ map HS.fromList $
        -- Note that some slaves might be initially empty. That's fine and inevitable.
        chunksOf numStatesPerSlave statesIds ++ repeat []
  writeIORef mhSlavesStatesRef slavesStates
  let states :: HMS.HashMap StateId state
      states = HMS.fromList (zip statesIds states0)
  -- Send states
  forM_ (HMS.toList slavesStates) $ \(slaveId, stateIds) -> do
    let slaveStates = HMS.intersection states
          (HMS.fromList (zip (HS.toList stateIds) (repeat ())))
    NM.nmWrite (slavesMap HMS.! slaveId) (SReqResetState slaveStates)
  forM_ slaves $ \(_, slave_) -> do
    NM.nmReadSelect slave_ $ \case
      SRespResetState -> Just ()
      _ -> Nothing
  return states

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

-- | Fetches current states stored in the slaves.
getStates ::
     MasterHandle state updatePayload updateOutput resampleContext resamplePayload resampleOutput
  -> IO (HMS.HashMap StateId state)
getStates MasterHandle{..} = do
  -- Send states
  slaves <- readIORef mhSlavesRef
  forM_ slaves (\slave_ -> NM.nmWrite slave_ SReqGetStates)
  fmap mconcat $ forM (HMS.elems slaves) $ \slave_ -> do
    NM.nmReadSelect slave_ $ \case
      SRespGetStates states -> Just states
      _ -> Nothing

{-

-- This sends a @[resamplePayload]@ to each specified state. The
-- 'saResample' function is then run on the slaves, once for each
-- @resamplePayload@, generating new states which have new IDs. States
-- not specified will have their values removed.
--
-- The @resampleOutput@s yielded by 'saResample' are returned in a
-- HashMap which associates the new StateIds with the result.
--
-- @resampleContext@ allows you to pass some data which is provided to
-- this computation. This way, if there is some shared information, we
-- only need to send it once.
--
-- NOTE: it is up to the client to not run this concurrently with
-- another 'resample' or 'update'.
resample :: forall state updatePayload updateOutput resampleContext resamplePayload resampleOutput m.
     (MonadIO m)
  => MasterHandle state updatePayload updateOutput resampleContext resamplePayload resampleOutput
  -> resampleContext
  -> HMS.HashMap StateId [resamplePayload]
  -> m (HMS.HashMap StateId resampleOutput)
resample MasterHandle{..} context resamples = liftIO $ do
  -- TODO we might want to bracket here when writing these...
  slavesStates <- readIORef mhSlavesStatesRef
  stateIdsCount <- readIORef mhStateIdsCountRef
  let allStates = mconcat (toList slavesStates)
  forM_ allStates $ \stateId -> case HMS.lookup stateId resamples of
    Nothing -> fail (printf "resample: State %d not present!" stateId)
    Just _ -> return ()
  when (HS.size allStates /= HMS.size resamples) $
    fail "resample: Spurious states present"
  let (newSlavesStatesAndPayloads, newStateIdsCount) = reassignStates slavesStates resamples stateIdsCount
  forM_ (HMS.toList newSlavesStatesAndPayloads) $ \(slaveId, payload) -> do
    NM.nmWrite (mhSlaves HMS.! slaveId) (SReqResample context payload)
  results <- fmap mconcat $ forM (HMS.elems mhSlaves) $ \slave_ -> do
    NM.nmReadSelect slave_ $ \case
      SRespResample outputs -> Just outputs
      _ -> Nothing
  -- TODO: make this debug-time check?
  let newSlaveStates = fmap
        (HS.fromList . concatMap (map fst . HMS.toList) . HMS.elems)
        newSlavesStatesAndPayloads
  _ <- assert (concat newSlaveStates == hashMapKeySet results)
              (fail "Didn't get expected resample outputs")
  writeIORef mhSlavesStatesRef newSlaveStates
  writeIORef mhStateIdsCountRef newStateIdsCount
  return results

generateNewStates ::
     HMS.HashMap StateId [resamplePayload]
  -> Int -- ^ StateId count
  -> (HMS.HashMap StateId (HMS.HashMap StateId resamplePayload), Int)
generateNewStates resamples count = flip runState count $ do
  -- We sort for the assigment to be deterministic
  fmap HMS.fromList $ forM (sortBy (comparing fst) (HMS.toList resamples)) $ \(stateId, payloads) -> do
    newStates <- fmap HMS.fromList $ forM payloads $ \payload -> do
      newStateId <- gets StateId
      modify' (+ 1)
      return (newStateId, payload)
    return (stateId, newStates)

reassignStates :: forall resamplePayload.
     HMS.HashMap SlaveId (HS.HashSet StateId)
  -> HMS.HashMap StateId [resamplePayload]
  -> Int
  -> (HMS.HashMap SlaveId (HMS.HashMap StateId (HMS.HashMap StateId resamplePayload)), Int)
reassignStates oldSlaveStates resamples count0 = let
  (newStates, count) = generateNewStates resamples count0
  pickStates slaveStates =
    HMS.intersection newStates (HMS.fromList (zip (HS.toList slaveStates) (repeat ())))
  in (pickStates <$> oldSlaveStates, count)

-- TODO: More efficient version of this?
hashMapKeySet :: (Eq k, Hashable k) => HMS.HashMap k v -> HS.HashSet k
hashMapKeySet = HS.fromList . HMS.keys

data WorkerArgs state updatePayload updateOutput resampleContext resamplePayload resampleOutput = WorkerArgs
  { waConfig :: WorkerConfig
    -- ^ Configuration for the worker. Note that configuration options
    -- relevant to the work-queue portions of things will be ignored.
    -- In particular, 'workerMasterLocalSlaves'.
  , waSlaveArgs :: SlaveArgs state updatePayload updateOutput resampleContext resamplePayload resampleOutput
    -- ^ Configuration to use when this node is a slave.
  , waMasterArgs :: MasterArgs state
    -- ^ Configuration to use when this node is a master.
  , waRequestSlaveCount :: Int
    -- ^ How many slaves to request. Note that this is not a guaranteed
    -- number of slaves, just a suggestion.
  , waMasterWaitTime :: Seconds
    -- ^ How long to wait for slaves to connect (if no slaves connect in
    -- this time, then the work gets aborted). If all the requested
    -- slaves connect, then waiting is aborted.
  }

type MasterFunc state updatePayload updateOutput resampleContext resamplePayload resampleOutput request response =
  RedisInfo ->
  MasterConnectInfo ->
  RequestId ->
  request ->
  MasterHandle state updatePayload updateOutput resampleContext resamplePayload resampleOutput ->
  IO response

-- | Runs a job-queue worker which implements a stateful distributed
-- computation.
runWorker
    :: ( MonadIO m, MonadBaseControl IO m
     , B.Binary state, B.Binary updatePayload, B.Binary updateOutput, B.Binary resampleContext, B.Binary resamplePayload, B.Binary resampleOutput, B.Binary request, B.Binary response
     )
    => WorkerArgs state updatePayload updateOutput resampleContext resamplePayload resampleOutput
    -> MasterFunc state updatePayload updateOutput resampleContext resamplePayload resampleOutput request response
    -- ^ This function runs on the master after it's received a
    -- reqeuest, and after some slaves have connected. The function is
    -- expected to use functions which take 'MasterHandle' to send work
    -- to its slaves. In particular:
    --
    -- 'resetStates' should be used to initialize the states.
    --
    -- 'update' and 'resample' should be invoked in order to execute the
    -- computation.
    --
    -- 'getStates' should be used to fetch result states (if necessary).
    -> m ()
-}
