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
  , resample
  , resetStates
  , getStates
    -- * Slave
  , SlaveArgs(..)
  , runSlave
  ) where

import           ClassyPrelude
import qualified Data.HashMap.Strict as HMS
import qualified Data.HashSet as HS
import           Text.Printf (printf, PrintfArg(..))
import           Control.DeepSeq (force, NFData)
import           Control.Exception (evaluate)
import qualified Data.Streaming.NetworkMessage as NM
import qualified Data.Conduit.Network as CN
import qualified Data.Binary as B
import           Data.Binary.Orphans ()
import           Data.List.Split (chunksOf)
import           Control.Concurrent.Async (mapConcurrently)
import           Control.Monad.State (runState, gets, modify')
import           Control.Monad.Trans.Control (MonadBaseControl)

data SlaveArgs state broadcastPayload broadcastOutput resampleContext resamplePayload resampleOutput = SlaveArgs
  { saUpdate :: !(broadcastPayload -> state -> IO (broadcastOutput, state))
  , saResample :: !(resampleContext -> resamplePayload -> state -> IO (resampleOutput, state))
  , saClientSettings :: !CN.ClientSettings
  , saNMSettings :: !NM.NMSettings
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

data SlaveReq state broadcastPayload resampleContext resamplePayload
  = SReqResetState
      !(HMS.HashMap StateId state) -- New states
  | SReqAddStates
      !(HMS.HashMap StateId state) -- States to add
  | SReqRemoveStates
      !SlaveId
      !(HS.HashSet StateId) -- States to get
  | SReqBroadcast
      !broadcastPayload -- With the given payload
      !(HS.HashSet StateId) -- Update the given states
  | SReqResample
      !resampleContext
      !(HMS.HashMap StateId (HMS.HashMap StateId resamplePayload))
      -- This map tells us to reasample the 'StateId' in the keys using the 'resamplePayload's
      -- in the value. The 'StateId' in the key for the value indicates what the new 'StateId'
      -- shall be.
  | SReqGetStates
  deriving (Generic, Eq, Show, NFData, B.Binary)

data SlaveResp state broadcastOutput resampleOutput
  = SRespResetState
  | SRespAddStates
  | SRespRemoveStates
      !SlaveId
      !(HMS.HashMap StateId state)
  | SRespBroadcast !(HMS.HashMap StateId broadcastOutput) -- TODO consider making this a simple list -- we don't really need it to be a HMS.
  | SRespResample !(HMS.HashMap StateId resampleOutput)
  | SRespGetStates !(HMS.HashMap StateId state)
  deriving (Generic, Eq, Show, NFData, B.Binary)

runSlave :: forall state broadcastPayload broadcastOutput resampleContext resamplePayload resampleOutput m a.
     ( B.Binary state, NFData state, B.Binary broadcastPayload, B.Binary broadcastOutput, B.Binary resampleContext, B.Binary resamplePayload, B.Binary resampleOutput
     , MonadIO m
     )
  => SlaveArgs state broadcastPayload broadcastOutput resampleContext resamplePayload resampleOutput -> m a
runSlave SlaveArgs{..} = liftIO $ do
    liftIO $ CN.runTCPClient saClientSettings $
      NM.generalRunNMApp saNMSettings (const "") (const "") $ \nm -> do
        go (NM.nmRead nm) (NM.nmWrite nm) SSNotInitialized
  where
    go :: forall b.
         IO (SlaveReq state broadcastPayload resampleContext resamplePayload)
      -> (SlaveResp state broadcastOutput resampleOutput -> IO ())
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

data MasterArgs state = MasterArgs
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

type SlaveNMAppData state broadcastPayload broadcastOutput resampleContext resamplePayload resampleOutput =
  NM.NMAppData (SlaveReq state broadcastPayload resampleContext resamplePayload) (SlaveResp state broadcastOutput resampleOutput)

data MasterHandle state broadcastPayload broadcastOutput resampleContext resamplePayload resampleOutput = MasterHandle
  { mhSlaves :: !(HMS.HashMap SlaveId (SlaveNMAppData state broadcastPayload broadcastOutput resampleContext resamplePayload resampleOutput))
  , mhSlavesStatesRef :: !(IORef (HMS.HashMap SlaveId (HS.HashSet StateId)))
  , mhStateIdsCountRef :: !(IORef Int)
  , mhArgs :: MasterArgs state
  }

mkMasterHandle
  :: (MonadBaseControl IO m, MonadIO m)
  => MasterArgs state
  -> m (MasterHandle state broadcastPayload broadcastOutput resampleContext resamplePayload resampleOutput)
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
  stateIdsCountRef <- newIORef 0
  return MasterHandle
    { mhSlaves = HMS.empty
    , mhSlavesStatesRef = slavesStatesRef
    , mhStateIdsCountRef = stateIdsCountRef
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

update :: forall state broadcastPayload broadcastOutput resampleContext resamplePayload resampleOutput m.
     (MonadIO m)
  => MasterHandle state broadcastPayload broadcastOutput resampleContext resamplePayload resampleOutput
  -> broadcastPayload
  -> m (HMS.HashMap StateId broadcastOutput)
update MasterHandle{..} payload = liftIO $ case maMaxBatchSize of
  Nothing -> do
    slavesStates <- readIORef mhSlavesStatesRef
    forM_ (HMS.toList slavesStates) $ \(slaveId, slaveStatesIds) -> do
      NM.nmWrite (mhSlaves HMS.! slaveId) (SReqBroadcast payload slaveStatesIds)
    fmap mconcat $ forM (HMS.elems mhSlaves) $ \slave_ -> do
      NM.nmReadSelect slave_ $ \case
        SRespBroadcast outputs -> Just outputs
        _ -> Nothing
  Just maxBatchSize -> do
    slavesStates <- readIORef mhSlavesStatesRef
    remainingStates <- newMVar (HS.toList <$> slavesStates)
    mconcat <$> mapConcurrently (\slaveId -> slaveThread maxBatchSize slaveId remainingStates) (HMS.keys mhSlaves)
  where
    MasterArgs {..} = mhArgs
    slaveThread ::
         Int -- ^ Max batch size
      -> SlaveId -- ^ The slave we're operating on
      -> MVar (HMS.HashMap SlaveId [StateId]) -- ^ 'MVar's holding the remaining states for each slave
      -> IO (HMS.HashMap StateId broadcastOutput)
    slaveThread maxBatchSize thisSlaveId slaveStatesVar = do
      let thisSlave = mhSlaves HMS.! thisSlaveId
      let go :: IO (HMS.HashMap StateId broadcastOutput)
          go = do
            status <- modifyMVar slaveStatesVar $ \slaveStates0 -> do
              let thisRemainingStates = slaveStates0 HMS.! thisSlaveId
              if null thisRemainingStates -- TODO do something smarter here
                then do
                  mbRequested <- stealStatesForSlave maxBatchSize thisSlaveId slaveStates0
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
                requestStatesForSlave thisSlaveId requestFrom statesToRequest
                broadcastAndContinue statesToRequest
      go

    -- | This is 'IO' just because we need to modify the 'IORef' holding
    -- the map recording which states are held by which slave.
    stealStatesForSlave ::
         Int
      -> SlaveId
      -> HMS.HashMap SlaveId [StateId]
      -> IO (Maybe (SlaveId, HS.HashSet StateId, HMS.HashMap SlaveId [StateId]))
      -- ^ If we could find some slave to transfer from, return its id, and the states we
      -- requested.
    stealStatesForSlave batchSize thisSlaveId remainingStates = do
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
      -> SlaveId
      -> HS.HashSet StateId
      -> IO ()
    requestStatesForSlave thisSlaveId otherSlaveId statesToRequest = do
      -- Request the states
      NM.nmWrite (mhSlaves HMS.! otherSlaveId) (SReqRemoveStates thisSlaveId statesToRequest)
      -- Receive the states
      states <- NM.nmReadSelect (mhSlaves HMS.! otherSlaveId) $ \case
        SRespRemoveStates slaveRequesting states | slaveRequesting == thisSlaveId -> Just states
        _ -> Nothing
      -- Upload states
      let thisSlave = mhSlaves HMS.! thisSlaveId
      NM.nmWrite thisSlave (SReqAddStates states)
      NM.nmReadSelect thisSlave $ \case
        SRespAddStates -> Just ()
        _ -> Nothing
      return ()

resample :: forall state broadcastPayload broadcastOutput resampleContext resamplePayload resampleOutput m.
     (MonadIO m)
  => MasterHandle state broadcastPayload broadcastOutput resampleContext resamplePayload resampleOutput
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

resetStates :: forall state broadcastPayload broadcastOutput resampleContext resamplePayload resampleOutput m.
     ( MonadBaseControl IO m, MonadIO m
     , B.Binary state, B.Binary broadcastPayload, B.Binary broadcastOutput, B.Binary resampleContext, B.Binary resamplePayload, B.Binary resampleOutput
     )
  => MasterHandle state broadcastPayload broadcastOutput resampleContext resamplePayload resampleOutput
  -> [state]
  -> m (HMS.HashMap StateId state)
resetStates MasterHandle{..} states0 = do
  -- Get a list of StateIds for states
  stateIdsCount <- readIORef mhStateIdsCountRef
  let numStates = length states0
  let statesIds = map StateId [stateIdsCount..stateIdsCount + numStates - 1]
  writeIORef mhStateIdsCountRef (stateIdsCount + numStates)
  -- Divide up the states among the slaves
  let slaves = HMS.toList mhSlaves
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
    NM.nmWrite (mhSlaves HMS.! slaveId) (SReqResetState slaveStates)
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

getStates ::
     MasterHandle state broadcastPayload broadcastOutput resampleContext resamplePayload resampleOutput
  -> IO (HMS.HashMap StateId state)
getStates MasterHandle{..} = do
  -- Send states
  forM_ mhSlaves (\slave_ -> NM.nmWrite slave_ SReqGetStates)
  fmap mconcat $ forM (HMS.elems mhSlaves) $ \slave_ -> do
    NM.nmReadSelect slave_ $ \case
      SRespGetStates states -> Just states
      _ -> Nothing

-- TODO: More efficient version of this?
hashMapKeySet :: (Eq k, Hashable k) => HMS.HashMap k v -> HS.HashSet k
hashMapKeySet = HS.fromList . HMS.keys

-- runWorker
--      :: forall m context input state output.
--         ( MonadConnect m
--        , Sendable context
--        , Sendable input
--        , Sendable state
--        , Sendable output
--        )
--     => WorkerConfig
--     -- ^ Configuration for the worker. Note that configuration options
--     -- relevant to the work-queue portions of things will be ignored.
--     -- In particular, 'workerMasterLocalSlaves'.
--     -> Int
--     -- ^ How many slaves to request (this count is not guaranteed,
--     -- though).
--     -> (context -> input -> StateId -> state -> (output, HMS.HashMap StateId state))
--     -- ^ This is the update function run by slaves.
--     -> (RedisInfo -> MasterConnectInfo -> RequestId -> request -> MasterHandle state context input output -> IO response)
--     -- ^ This function runs on the master after it's received a
--     -- reqeuest. The function is expected to use functions which take
--     -- 'MasterHandle' to send work to its slaves. In particular,
--     -- 'update'.
--     -> m ()
-- runWorker config slaveCount update inner = do
--     unless (slaveCount > 0) $ error "For stateful worker, slave count must be > 0"
--     jobQueueNode config slave' master'
--   where
--     slave' wp mci = slave SlaveArgs
--         { saUpdate = update
--         , saServerSettings = clientSettingsTCP (mciPort mci) (mciHost mci)
--         , saNMSettings = wpNMSettings wp
--         }
--     master' wp ss rid r mci = do
--         let redis = wpRedis w
--         forM_ [1..slaveCount] $ requestSlave redis mci
--         runTCPServer ss $ runNMApp (wpNMSettings wp) $ \nm -> do
--             let args = MasterArgs
--                     {
