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
  , runMaster
    -- ** Operations
  , StateId
  , update
  , getStates
  , getStateIds
  , newStateIdSupply
    -- * Slave
  , SlaveArgs(..)
  , runSlave
    -- * Job-queue integration
  -- , worker
  ) where

import           ClassyPrelude
import           Control.Concurrent.Async (mapConcurrently, withAsync)
import           Control.DeepSeq (force, NFData)
import           Control.Exception (evaluate)
import           Control.Monad.Trans.Control
import qualified Data.Binary as B
import           Data.Binary.Orphans ()
import qualified Data.Conduit.Network as CN
import qualified Data.HashMap.Strict as HMS
import qualified Data.HashSet as HS
import           Data.SimpleSupply
import qualified Data.Streaming.NetworkMessage as NM
import           Text.Printf (printf, PrintfArg(..))
import           GHC.Stack (CallStack, getCallStack)

data SlaveArgs state context input output = SlaveArgs
  { saUpdate :: !(context -> input -> StateId -> state -> (output, HMS.HashMap StateId state))
  , saClientSettings :: CN.ClientSettings
  , saNMSettings :: NM.NMSettings
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

runSlave :: forall state context input output m a.
     (B.Binary state, NFData state, B.Binary context, B.Binary input, B.Binary output, MonadIO m)
  => SlaveArgs state context input output -> m a
runSlave SlaveArgs{..} =
  liftIO $ CN.runTCPClient saClientSettings $
    NM.generalRunNMApp saNMSettings (const "") (const "") $ \nm -> do
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
        let getStates' :: String -> HMS.HashMap StateId state
            getStates' err = case ssStates ss of
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
            let states = HMS.unionWith (error "slave: adding existing states") (getStates' "SReqAddStates") newStates
            send SRespAddStates
            loop ss { ssStates = Just states }
          SReqRemoveStates slaveRequesting stateIdsToDelete -> do
            let states = getStates' "SReqRemoveStates"
            let states' = foldl' (flip HMS.delete) states stateIdsToDelete
            let statesToSend = HMS.fromList
                  [(stateId, states ! stateId) | stateId <- HS.toList stateIdsToDelete]
            send (SRespRemoveStates slaveRequesting statesToSend)
            loop ss { ssStates = Just states' }
          SReqSetContext context -> do
            send SRespSetContext
            loop ss { ssContext = Just context }
          SReqUpdate inputs -> do
            let states0 = getStates' "SReqUpdate"
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
            let states = getStates' "SReqGetStates"
            send (SRespGetStates states)
            loop ss
      in loop

data MasterArgs = MasterArgs
  { maMaxBatchSize :: !(Maybe Int)
    -- ^ The maximum amount of states that will be transferred at once. If 'Nothing', they
    -- will be all transferred at once, and no "rebalancing" will ever happen.
    -- Moreover, if 'Nothing', 'maMinBatchSize' will be ignored.
  , maMinBatchSize :: !(Maybe Int)
    -- ^ The minimum amount of states that will be transferred at once. 'Nothing' is equivalent to
    -- @'Just' 0@.
  , maServerSettings :: CN.ServerSettings
    -- ^ Settings for the TCP server
  , maNMSettings :: NM.NMSettings
  }

newtype SlaveId = SlaveId {unSlaveId :: Int}
  deriving (Generic, Eq, Ord, Show, Hashable, NFData, B.Binary)
instance PrintfArg SlaveId where
  formatArg = formatArg . unSlaveId
  parseFormat = parseFormat . unSlaveId

data Slave state context input output = Slave
  { slaveConnection :: SlaveNMAppData state context input output
  , slaveStates :: HS.HashSet StateId
  }

type SlaveNMAppData state context input output =
  NM.NMAppData (SlaveReq state context input) (SlaveResp state output)

data MasterHandle state context input output = MasterHandle
  { mhSlaves :: !(IORef (HMS.HashMap SlaveId (Slave state context input output)))
  , mhArgs :: !MasterArgs
  , mhReadyVar :: !(MVar ())
  }

runMaster :: forall state context input output m a.
     ( B.Binary state, B.Binary context, B.Binary input, B.Binary output
     , MonadBaseControl IO m, MonadIO m)
  => MasterArgs
  -> (HMS.HashMap StateId state)
  -> (MasterHandle state context input output -> m a)
  -> m a
runMaster ma@MasterArgs{..} initial inner = do
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
  readyVar <- newEmptyMVar
  let mh = MasterHandle
        { mhSlaves = slavesRef
        , mhArgs = ma
        , mhReadyVar = readyVar
        }
  doneVar <- newEmptyMVar
  (control $ \runInBase -> withAsync (server mh doneVar) $ \_ -> runInBase (inner mh))
    `finally` putMVar doneVar ()
  where
    server MasterHandle{..} doneVar = do
      idSupply <- newSupply (SlaveId 0) (\(SlaveId n) -> SlaveId (n + 1))
      CN.runGeneralTCPServer maServerSettings $
        NM.generalRunNMApp maNMSettings (const "") (const "") $ \nm -> do
          slaveId <- askSupply idSupply
          -- The first slave gets all the initial states.
          --
          -- FIXME: should probably have slaves send their states
          -- between eachother, as this means that we're going to be
          -- sending a lot more states through the rebalancing code.
          let isInitial = slaveId == SlaveId 0
          NM.nmWrite nm $ SReqResetState $ if isInitial then initial else HMS.empty
          atomicModifyIORef' mhSlaves $ (,()) .
            HMS.insert slaveId Slave
              { slaveConnection = nm
              , slaveStates = if isInitial then (hashMapKeySet initial) else HS.empty
              }
          -- Unblock 'update', since there's now an available slave.
          when isInitial (putMVar mhReadyVar ())
          -- Wait to exit, so that the connection stays open.
          takeMVar doneVar

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
update MasterHandle{..} context inputs0 = liftIO $ do
  -- Wait for there to be at least one slave.
  withMVar mhReadyVar return
  slaves <- readIORef mhSlaves
  -- FIXME: Should check that all of the inputs are used.
  let slavesAndInputs :: [(SlaveId, (Slave state context input output, [(StateId, input)]))]
      slavesAndInputs =
        map (\(slaveId, slave) ->
              (slaveId, (slave, mapMaybe (\k -> (k,) <$> HMS.lookup k inputs0) (HS.toList (slaveStates slave)))))
            (HMS.toList slaves)
  forM_ slavesAndInputs $ \(_, (slave, _)) -> do
    NM.nmWrite (slaveConnection slave) (SReqSetContext context)
  case maMaxBatchSize of
    Nothing -> do
      forM_ slavesAndInputs $ \(_, (slave, inputs)) -> do
        NM.nmWrite (slaveConnection slave) (SReqUpdate (HMS.fromList inputs))
      fmap mconcat $ forM slavesAndInputs $ \(slaveId, (slave, inputs)) -> do
        outputs <- NM.nmReadSelect (slaveConnection slave) $ \case
          SRespUpdate outputs -> Just outputs
          _ -> Nothing
        updateStates slaveId (HMS.fromList inputs) outputs
        return outputs
    Just maxBatchSize -> do
      remainingStatesVar <- newMVar (HMS.fromList (map (second snd) slavesAndInputs))
      mconcat <$> mapConcurrently (slaveThread maxBatchSize remainingStatesVar) (HMS.toList slaves)
  where
    MasterArgs {..} = mhArgs
    slaveThread ::
         Int -- ^ Max batch size
      -> MVar (HMS.HashMap SlaveId [(StateId, input)]) -- ^ 'MVar's holding the remaining states for each slave
      -> (SlaveId, Slave state context input output) -- ^ The slave we're operating on
      -> IO (HMS.HashMap StateId (output, HS.HashSet StateId))
    slaveThread maxBatchSize remainingStatesVar (slaveId, slave) = do
      let go :: IO (HMS.HashMap StateId (output, HS.HashSet StateId))
          go = do
            status <- modifyMVar remainingStatesVar $ \slaveStates0 -> do
              let thisRemainingStates = slaveStates0 ! slaveId
              if null thisRemainingStates -- TODO do something smarter here
                then do
                  mbRequested <- stealStatesForSlave maxBatchSize slaveId slaveStates0
                  return $ case mbRequested of
                    Just (requestedFrom, requestedStates, slaveStates) ->
                      (slaveStates, STSRequestStates requestedFrom requestedStates)
                    Nothing ->
                      (slaveStates0, STSStop)
                else return $ let
                  (roundStates, thisRemainingStates') = splitAt maxBatchSize thisRemainingStates
                  slaveStates = HMS.insert slaveId thisRemainingStates' slaveStates0
                  in (slaveStates, STSOk (HMS.fromList roundStates))
            let broadcastAndContinue roundStates = do
                  NM.nmWrite (slaveConnection slave) (SReqUpdate roundStates)
                  roundOutputs <- NM.nmReadSelect (slaveConnection slave) $ \case
                    SRespUpdate outputs -> Just outputs
                    _ -> Nothing
                  updateStates slaveId roundStates roundOutputs
                  moreOutputs <- go
                  return (roundOutputs <> moreOutputs)
            case status of
              STSOk roundStates -> broadcastAndContinue roundStates
              STSStop -> return HMS.empty
              STSRequestStates requestFrom statesToRequest -> do
                -- printf ("Transferring states from %d to %d %s\n")
                --   requestFrom thisSlaveId (show (map unStateId (HS.toList statesToRequest)))
                requestStatesForSlave slaveId slave requestFrom (hashMapKeySet statesToRequest)
                broadcastAndContinue statesToRequest
      go

    updateStates ::
         SlaveId
      -> HMS.HashMap StateId input
      -> HMS.HashMap StateId (output, HS.HashSet StateId)
      -> IO ()
    updateStates slaveId removals additions = do
      let f slave = slave
            { slaveStates = (slaveStates slave `difference` hashMapKeySet removals) <> foldMap snd additions
            }
      atomicModifyIORef' mhSlaves $ \slaves -> (,()) $
        HMS.adjust f slaveId slaves

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
            guard (min batchSize (numSlaveRemainingStates - batchSize) >= fromMaybe 0 maMinBatchSize)
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
          atomicModifyIORef' mhSlaves $ \slaves -> (,()) $ let
            transferredIds = hashMapKeySet statesToBeTransferred
            candidateSlave = slaves HMS.! candidateSlaveId
            thisSlave = slaves HMS.! thisSlaveId
            candidateStatesNow = HS.difference (slaveStates candidateSlave) transferredIds
            thisStatesNow = HS.union (slaveStates thisSlave) transferredIds
            in HMS.insert candidateSlaveId (candidateSlave { slaveStates = candidateStatesNow }) $
               HMS.insert thisSlaveId (thisSlave { slaveStates = thisStatesNow }) $
               slaves
          return (Just (candidateSlaveId, statesToBeTransferred, remainingStates'))

    requestStatesForSlave ::
         SlaveId
      -> Slave state context input output
      -> SlaveId
      -> HS.HashSet StateId
      -> IO ()
    requestStatesForSlave thisSlaveId thisSlave otherSlaveId statesToRequest = do
      slaves <- readIORef mhSlaves
      let otherSlave = slaveConnection (slaves HMS.! otherSlaveId)
      -- Request the states
      NM.nmWrite otherSlave (SReqRemoveStates thisSlaveId statesToRequest)
      -- Receive the states
      states <- NM.nmReadSelect otherSlave $ \case
        SRespRemoveStates slaveRequesting states | slaveRequesting == thisSlaveId -> Just states
        _ -> Nothing
      -- Upload states
      NM.nmWrite (slaveConnection thisSlave) (SReqAddStates states)
      NM.nmReadSelect (slaveConnection thisSlave) $ \case
        SRespAddStates -> Just ()
        _ -> Nothing
      return ()

getStates
  :: (MonadBaseControl IO m, MonadIO m)
  => MasterHandle state context input output
  -> m (HMS.HashMap StateId state)
getStates MasterHandle{..} = do
  slaves <- readIORef mhSlaves
  forM_ slaves $ \slave -> NM.nmWrite (slaveConnection slave) SReqGetStates
  fmap (mconcat) $ forM (HMS.elems slaves) $ \slave -> do
    NM.nmReadSelect (slaveConnection slave) $ \case
      SRespGetStates states -> Just states
      _ -> Nothing

getStateIds
  :: MonadIO m
  => MasterHandle state context input output
  -> m (HS.HashSet StateId)
getStateIds MasterHandle{..} = liftIO $ foldMap slaveStates <$> readIORef mhSlaves

newStateIdSupply :: MonadBaseControl IO m => m (Supply StateId)
newStateIdSupply = newSupply (StateId 0) (\(StateId n) -> StateId (n + 1))

-- TODO: More efficient version of this?
hashMapKeySet :: (Eq k, Hashable k) => HMS.HashMap k v -> HS.HashSet k
hashMapKeySet = HS.fromList . HMS.keys

(!) :: (Eq k, Hashable k, ?stk :: CallStack) => HMS.HashMap k a -> k -> a
(!) mp k =
   fromMaybe (error ("key not found. Context: " ++ show (getCallStack ?stk))) (HMS.lookup k mp)

-- Job-queue integration

-- worker
--     :: forall m context input state output.
--        ( MonadConnect m
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
-- worker config slaveCount update inner = do
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
