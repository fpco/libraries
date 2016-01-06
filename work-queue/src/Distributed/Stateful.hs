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
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ImplicitParams #-}
module Distributed.Stateful
  ( -- * Master
    MasterArgs(..)
  , MasterHandle
  , master
    -- ** Operations
  , StateId
  , update
  , getStates
  , getStateIds
  , withStateIdSupply
  , newStateId
    -- * Slave
  , SlaveArgs(..)
  , slave
  ) where

import           ClassyPrelude
import           Control.Concurrent.Async (mapConcurrently)
import           Control.DeepSeq (force, NFData)
import           Control.Exception (evaluate)
import           Control.Monad.State (State, runState, gets, modify', state)
import           Control.Monad.Trans.Control (MonadBaseControl)
import qualified Data.Binary as B
import           Data.Binary.Orphans ()
import qualified Data.Conduit.Network as CN
import qualified Data.HashMap.Strict as HMS
import qualified Data.HashSet as HS
import           Data.List.Split (chunksOf)
import qualified Data.Streaming.NetworkMessage as NM
import qualified Data.Vector as V
import           Text.Printf (printf, PrintfArg(..))
import           GHC.Stack (CallStack, getCallStack)

data SlaveArgs state context input output = SlaveArgs
  { saUpdate :: !(context -> input -> StateId -> state -> (output, HMS.HashMap StateId state))
  , saServerSettings :: !CN.ServerSettings
  , saNMSettings :: !NM.NMSettings
  }

newtype StateId = StateId {unStateId :: Int}
  deriving (Generic, Eq, Ord, Show, Hashable, NFData, B.Binary)
instance PrintfArg StateId where
  formatArg = formatArg . unStateId
  parseFormat = parseFormat . unStateId

data SlaveState state context
  = SlaveState
  { ssStates :: !(Maybe (HMS.HashMap StateId state))
  , ssContext :: !(Maybe context)
  }
  deriving (Generic, Eq, Show, NFData, B.Binary)

data SlaveReq state context input
  = SReqResetState
      !(HMS.HashMap StateId state) -- New states
  | SReqAddStates
      !(HMS.HashMap StateId state) -- States to add
  | SReqRemoveStates
      !SlaveId
      !(HS.HashSet StateId) -- States to get
  | SReqSetContext
      !context
  | SReqUpdate
      !(HMS.HashMap StateId input) -- Update the given states
  | SReqGetStates
  deriving (Generic, Eq, Show, NFData, B.Binary)

-- TODO: SReqUpdate variant which doesn't send back results?

data SlaveResp state output
  = SRespResetState
  | SRespAddStates
  | SRespRemoveStates
      !SlaveId
      !(HMS.HashMap StateId state)
  | SRespSetContext
  -- TODO consider making this a simple list
  -- we don't really need it to be a HMS.
  | SRespUpdate !(HMS.HashMap StateId (output, HS.HashSet StateId))
  | SRespGetStates !(HMS.HashMap StateId state)
  deriving (Generic, Eq, Show, NFData, B.Binary)

slave :: forall state context input output m a.
     (B.Binary state, NFData state, B.Binary context, B.Binary input, B.Binary output, MonadIO m)
  => SlaveArgs state context input output -> m a
slave SlaveArgs{..} = liftIO $ CN.runGeneralTCPServer saServerSettings $
  NM.generalRunNMApp (saNMSettings) (const "") (const "") $ \nm -> do
    go (NM.nmRead nm) (NM.nmWrite nm) (SlaveState Nothing Nothing)
  where
    go :: forall b.
         IO (SlaveReq state context input)
      -> (SlaveResp state output -> IO ())
      -> SlaveState state context
      -> IO b
    go recv send = let
      loop ss = do
        req <- recv
        let getStates :: String -> HMS.HashMap StateId state
            getStates err = case ssStates ss of
              Nothing -> error ("slave: state not initialized (" ++ err ++ ")")
              Just states -> states
            getContext :: String -> context
            getContext err = case ssContext ss of
              Nothing -> error ("context: state not initialized (" ++ err ++ ")")
              Just context -> context
        case req of
          SReqResetState states -> do
            send SRespResetState
            loop ss { ssStates = Just states }
          SReqAddStates newStates -> do
            let states = HMS.unionWith (error "slave: adding existing states") (getStates "SReqAddStates") newStates
            send SRespAddStates
            loop ss { ssStates = Just states }
          SReqRemoveStates slaveRequesting stateIdsToDelete -> do
            let states = getStates "SReqRemoveStates"
            let states' = foldl' (flip HMS.delete) states stateIdsToDelete
            let statesToSend = HMS.fromList
                  [(stateId, states ! stateId) | stateId <- HS.toList stateIdsToDelete]
            send (SRespRemoveStates slaveRequesting statesToSend)
            loop ss { ssStates = Just states' }
          SReqSetContext context -> do
            send SRespSetContext
            loop ss { ssContext = Just context }
          SReqUpdate inputs -> do
            let states0 = getStates "SReqUpdate"
            let context = getContext "SReqUpdate"
            let foldStep (outputs, updatedStates) stateId input = let
                  state = case HMS.lookup stateId states0 of
                    Nothing -> error (printf "slave: Could not find state %d (SReqUpdate)" stateId)
                    Just state0 -> state0
                  -- TODO: the old version of this for SReqBroadcast
                  -- left the current stateId in the map. That was a
                  -- bug, right?
                  !(!output, !states') = saUpdate context input stateId state
                  in ( HMS.insert stateId (output, hashMapKeySet states') outputs
                     , states' <>
                       HMS.delete stateId updatedStates
                     )
            let (outputs, states) = HMS.foldlWithKey' foldStep (HMS.empty, states0) inputs
            void (evaluate (force states))
            send (SRespUpdate outputs)
            loop ss { ssStates = Just states }
          SReqGetStates -> do
            let states = getStates "SReqGetStates"
            send (SRespGetStates states)
            loop ss
      in loop

data MasterArgs state = MasterArgs
  { maInitialStates :: !(V.Vector state)
  , maSlaves :: !(V.Vector (CN.ClientSettings, NM.NMSettings))
  , maMaxBatchSize :: !(Maybe Int)
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
  { mhSlaves :: !(HMS.HashMap SlaveId (SlaveNMAppData state context input output))
  , mhSlavesStatesRef :: !(IORef (HMS.HashMap SlaveId (HS.HashSet StateId)))
  , mhStateIdsCountRef :: !(IORef Int)
  , mhMaxBatchSize :: !(Maybe Int)
  , mhMinBatchSize :: !(Maybe Int)
  }

withStateIdSupply
  :: (MonadBaseControl IO m, MonadIO m)
  => MasterHandle state context input output
  -> State StateId a
  -> m a
withStateIdSupply MasterHandle{..} f = do
  atomicModifyIORef' mhStateIdsCountRef $ \count0 ->
    let (x, StateId count) = runState f (StateId count0) in (count, x)

newStateId :: State StateId StateId
newStateId = state (\r@(StateId x) -> (r, StateId (x + 1)))

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

master :: forall state context input output m a.
     ( B.Binary state, B.Binary context, B.Binary input, B.Binary output
     , MonadBaseControl IO m, MonadIO m)
  => MasterArgs state
  -> (V.Vector StateId -> MasterHandle state context input output -> m a)
  -> m a
master MasterArgs{..} cont0 = do
  case (maMinBatchSize, maMaxBatchSize) of
    (_, Just maxBatchSize) | maxBatchSize < 1 ->
      fail (printf "Distribute.master: maMaxBatchSize must be > 0 (got %d)" maxBatchSize)
    (Just minBatchSize, _) | minBatchSize < 0 ->
      fail (printf "Distribute.master: maMinBatchSize must be >= 0 (got %d)" minBatchSize)
    (Just minBatchSize, Just maxBatchSize) | minBatchSize > maxBatchSize ->
      fail (printf "Distribute.master: maMinBatchSize can't be greater then maMaxBatchSize (got %d and %d)" minBatchSize maxBatchSize)
    _ ->
      return ()
  connectToSlaves (V.toList maSlaves) $ \slaves0 -> do
    let numSlaves = length slaves0
    let slavesIds = take numSlaves (map SlaveId [0..])
    let slaves = HMS.fromList (zip slavesIds slaves0)
    let numStates = length maInitialStates
    let statesIds = take numStates (map StateId [0..])
    stateIdsCountRef <- liftIO $ newIORef numStates
    let numStatesPerSlave = getNumStatesPerSlave numSlaves numStates
        slavesStates = HMS.fromList $ zip slavesIds $ map HS.fromList $
          -- Note that some slaves might be initially empty. That's fine and inevitable.
          chunksOf numStatesPerSlave statesIds ++ repeat []
    slavesStatesRef <- newIORef slavesStates
    let states :: HMS.HashMap StateId state
        states = HMS.fromList (zip statesIds (V.toList maInitialStates))
    -- Send states
    forM_ (HMS.toList slavesStates) $ \(slaveId, stateIds) -> do
      let slaveStates = HMS.intersection states
            (HMS.fromList (zip (HS.toList stateIds) (repeat ())))
          slaveConn = slaves ! slaveId
      NM.nmWrite slaveConn (SReqResetState slaveStates)
    forM_ slaves $ \slave_ -> do
      NM.nmReadSelect slave_ $ \case
        SRespResetState -> Just ()
        _ -> Nothing
    cont0 (V.fromList statesIds) MasterHandle
      { mhSlaves = slaves
      , mhSlavesStatesRef = slavesStatesRef
      , mhStateIdsCountRef = stateIdsCountRef
      , mhMaxBatchSize = maMaxBatchSize
      , mhMinBatchSize = maMinBatchSize
      }
  where
    connectToSlaves ::
         [(CN.ClientSettings, NM.NMSettings)]
      -> ([SlaveNMAppData state context input output] -> m a)
      -> m a
    connectToSlaves settings0 cont = case settings0 of
      [] -> cont []
      (cs, nms) : settings ->
        CN.runGeneralTCPClient cs $ NM.generalRunNMApp nms (const "") (const "") $ \nm ->
          connectToSlaves settings (\nodes -> cont (nm : nodes))

data SlaveThreadStatus input
  = STSOk
      !(HMS.HashMap StateId input) -- States to execute
  | STSRequestStates
      !SlaveId -- Slave to request the states below from
      !(HMS.HashMap StateId input)
  | STSStop
  deriving (Eq, Show)

update :: forall state context input output m.
     (B.Binary state, B.Binary context, B.Binary input, B.Binary output, MonadIO m)
  => MasterHandle state context input output
  -> context
  -> HMS.HashMap StateId input
  -> m (HMS.HashMap StateId (output, HS.HashSet StateId))
update MasterHandle{..} context inputs = liftIO $ do
  slavesStates <- readIORef mhSlavesStatesRef
  -- FIXME: Should check that all of the inputs are used.
  let statesAndInputs =
        map (\(slaveId, states) ->
              (slaveId, mapMaybe (\k -> (k,) <$> HMS.lookup k inputs) (HS.toList states)))
            (HMS.toList slavesStates)
  forM_ (HMS.keys mhSlaves) $ \slaveId -> do
    NM.nmWrite (mhSlaves ! slaveId) (SReqSetContext context)
  case mhMaxBatchSize of
    Nothing -> do
      forM_ statesAndInputs $ \(slaveId, inputs) -> do
        NM.nmWrite (mhSlaves ! slaveId) (SReqUpdate (HMS.fromList inputs))
      fmap mconcat $ forM statesAndInputs $ \(slaveId, inputs) -> do
        outputs <- NM.nmReadSelect (mhSlaves ! slaveId) $ \case
          SRespUpdate outputs -> Just outputs
          _ -> Nothing
        updateStates slaveId (HMS.fromList inputs) outputs
        return outputs
    Just maxBatchSize -> do
      remainingStatesVar <- newMVar (HMS.fromList statesAndInputs)
      mconcat <$> mapConcurrently (\slaveId -> slaveThread maxBatchSize slaveId remainingStatesVar) (HMS.keys mhSlaves)
  where
    slaveThread ::
         Int -- ^ Max batch size
      -> SlaveId -- ^ The slave we're operating on
      -> MVar (HMS.HashMap SlaveId [(StateId, input)]) -- ^ 'MVar's holding the remaining states for each slave
      -> IO (HMS.HashMap StateId (output, HS.HashSet StateId))
    slaveThread maxBatchSize thisSlaveId remainingStatesVar = do
      let thisSlave = mhSlaves ! thisSlaveId
      let go :: IO (HMS.HashMap StateId (output, HS.HashSet StateId))
          go = do
            status <- modifyMVar remainingStatesVar $ \slaveStates0 -> do
              let thisRemainingStates =
                    slaveStates0 ! thisSlaveId
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
                  in (slaveStates, STSOk (HMS.fromList roundStates))
            let broadcastAndContinue roundStates = do
                  NM.nmWrite thisSlave (SReqUpdate roundStates)
                  roundOutputs <- NM.nmReadSelect thisSlave $ \case
                    SRespUpdate outputs -> Just outputs
                    _ -> Nothing
                  updateStates thisSlaveId roundStates roundOutputs
                  moreOutputs <- go
                  return (roundOutputs <> moreOutputs)
            case status of
              STSOk roundStates -> broadcastAndContinue roundStates
              STSStop -> return HMS.empty
              STSRequestStates requestFrom statesToRequest -> do
                -- printf ("Transferring states from %d to %d %s\n")
                --   requestFrom thisSlaveId (show (map unStateId (HS.toList statesToRequest)))
                requestStatesForSlave thisSlaveId requestFrom (hashMapKeySet statesToRequest)
                broadcastAndContinue statesToRequest
      go

    updateStates ::
         SlaveId
      -> HMS.HashMap StateId input
      -> HMS.HashMap StateId (output, HS.HashSet StateId)
      -> IO ()
    updateStates slaveId removals additions = do
      let f ss = (ss `difference` hashMapKeySet removals) <> foldMap snd additions
      atomicModifyIORef' mhSlavesStatesRef $ \slaveStates -> (,()) $
        HMS.adjust f slaveId slaveStates

    -- | This is 'IO' just because we need to modify the 'IORef' holding
    -- the map recording which states are held by which slave.
    stealStatesForSlave ::
         Int
      -> SlaveId
      -> HMS.HashMap SlaveId [(StateId, input)]
      -> IO (Maybe (SlaveId, HMS.HashMap StateId input, HMS.HashMap SlaveId [(StateId, input)]))
      -- ^ If we could find some slave to transfer from, return its id, and the states we
      -- requested.
    stealStatesForSlave batchSize thisSlaveId remainingStates = do
      let thisSlaveStates = remainingStates ! thisSlaveId
      when (not (null thisSlaveStates)) $
        fail "broadcast.stealStatesForSlave: Expecting no states in thisSlaveId"
      let goodCandidate (slaveId, slaveRemainingStates) = do
            guard (slaveId /= thisSlaveId)
            let numSlaveRemainingStates = length slaveRemainingStates
            guard (numSlaveRemainingStates > batchSize)
            guard (min batchSize (numSlaveRemainingStates - batchSize) >= fromMaybe 0 mhMinBatchSize)
            return (slaveId, numSlaveRemainingStates, slaveRemainingStates)
      let candidates :: [(SlaveId, Int, [(StateId, input)])]
          candidates = catMaybes (map goodCandidate (HMS.toList remainingStates))
      if null candidates
        then return Nothing
        else do
          -- Pick candidate with highest number of states to steal states from
          let (candidateSlaveId, numCandidateStates, candidateStates) = maximumByEx (comparing (\(_, x, _) -> x)) candidates
          let statesToBeTransferred = HMS.fromList (take (min batchSize (numCandidateStates - batchSize)) candidateStates)
          -- Bookkeep the remaining states
          let remainingCandidateStatesBefore = HMS.fromList (remainingStates HMS.! candidateSlaveId)
          let remainingCandidateStatesNow = HMS.toList (HMS.difference remainingCandidateStatesBefore statesToBeTransferred)
          let remainingStates' = HMS.insert candidateSlaveId remainingCandidateStatesNow remainingStates
          -- Bookkeep the global states bookkeeping
          atomicModifyIORef' mhSlavesStatesRef $ \slaveStates -> (,()) $ let
            transferredIds = hashMapKeySet statesToBeTransferred
            candidateStatesNow = HS.difference (slaveStates HMS.! candidateSlaveId) transferredIds
            thisStatesNow = HS.union (slaveStates HMS.! thisSlaveId) transferredIds
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

getStates
  :: MonadIO m
  => MasterHandle state context input output
  -> m (HMS.HashMap StateId state)
getStates MasterHandle{..} = do
  -- Send states
  forM_ mhSlaves (\slave_ -> NM.nmWrite slave_ SReqGetStates)
  fmap mconcat $ forM (HMS.elems mhSlaves) $ \slave_ -> do
    NM.nmReadSelect slave_ $ \case
      SRespGetStates states -> Just states
      _ -> Nothing

getStateIds
  :: MonadIO m
  => MasterHandle state context input output
  -> m (HS.HashSet StateId)
getStateIds MasterHandle{..} = liftIO $ fold <$> readIORef mhSlavesStatesRef

-- TODO: More efficient version of this?
hashMapKeySet :: (Eq k, Hashable k) => HMS.HashMap k v -> HS.HashSet k
hashMapKeySet = HS.fromList . HMS.keys

setToHashMap :: (Eq k, Hashable k) => HS.HashSet k -> HMS.HashMap k ()
setToHashMap = HMS.fromList . map (,()) . HS.toList

(!) :: (Eq k, Hashable k, ?stk :: CallStack) => HMS.HashMap k a -> k -> a
(!) mp k =
   fromMaybe (error ("key not found. Context: " ++ show (getCallStack ?stk))) (HMS.lookup k mp)
