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
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE ConstraintKinds #-}
{-|
Module: Distributed.Stateful.Master
Description: Master nodes for a distributed stateful computation.

A master coordinates a diatributed stateful computation.

All functions that run or act on a master need a 'MasterHandle' in
order to do so.  'MasterHandle's are created via 'initMaster'.
-}

module Distributed.Stateful.Master
    ( -- * Configuration and creation of master nodes
      MasterArgs(..)
    , defaultMasterArgs
    , Update
    , MasterHandle
    , initMaster
    , DoProfiling (..)
    , StateId
    , SlaveId
    , StatefulConn(..)
      -- * Operations on master nodes
    , closeMaster
    , update
    , resetStates
    , getStateIds
    , getStates
    , addSlaveConnection
    , getNumSlaves
      -- * Types
    , MasterException(..)
      -- * Performance profiling
    , getSlavesProfiling
    , getMasterProfilng
    , getProfiling
    , SlaveProfiling(..), emptySlaveProfiling
    , MasterProfiling(..), emptyMasterProfiling
    , Profiling (..)
    , ProfilingCounter(..)
    , SlaveResp
    , SlaveReq
    , SlaveConn
    ) where

import           ClassyPrelude
import           Control.DeepSeq (NFData)
import           Control.Lens (makeLenses, set, at, _Just, over)
import           Control.Monad.Logger.JSON.Extra (MonadLogger, logWarnJ, logInfoJ, logDebugJ)
import           Control.Monad.Trans.Control (MonadBaseControl)
import qualified Data.HashMap.Strict as HMS
import qualified Data.HashSet as HS
import qualified Data.HashTable.IO as HT
import           Data.List.Split (chunksOf)
import           Data.SimpleSupply
import           Distributed.Stateful.Internal
import           FP.Redis (MonadConnect)
import           Text.Printf (printf)
import qualified Data.Store as S
import qualified Data.Store.Core as S
import qualified Data.Store.Streaming as S
import qualified Data.Store.Streaming.Internal as S
import qualified Data.Vector as V
import           Control.Exception.Lifted (evaluate)
import qualified System.IO.ByteBuffer as BB
import           Foreign.Ptr (plusPtr)

-- | Defines the behaviour of a master.
data MasterArgs m state context input output = MasterArgs
  { maMaxBatchSize :: !(Maybe Int)
    -- ^ The maximum amount of states that will be transferred at once. If 'Nothing', they
    -- will be all transferred at once, and no "rebalancing" will ever happen.
  , maUpdate :: !(Update m state context input output)
    -- ^ This argument will be used if 'update' is invoked when no slaves have
    -- been added yet.
  , maDoProfiling :: !DoProfiling
    -- ^ Specifies whether to perform profiling or not.
  }

defaultMasterArgs :: Update m state context input output -> MasterArgs m state context input output
defaultMasterArgs upd = MasterArgs
  { maMaxBatchSize = Just 5
  , maUpdate = upd
  , maDoProfiling = NoProfiling
  }

-- | Handle to a master.  Can be created using 'initMaster'.
newtype MasterHandle m key state context input output =
  MasterHandle {_unMasterHandle :: MVar (MasterHandle_ m key state context input output)}

data Slaves m key state context input output
  = NoSlavesYet ![(StateId, state)]
  | Slaves !(HMS.HashMap SlaveId (Slave m key state context input output))

data MasterHandle_ m key state context input output = MasterHandle_
  { mhSlaves :: !(Slaves m key state context input output)
  , mhSlaveIdSupply :: !(Supply SlaveId)
  , mhStateIdSupply :: !(Supply StateId)
  , mhEventManager :: !(EventManager m key)
  , mhArgs :: !(MasterArgs m state context input output)
  , mhProfiling :: !(Maybe (IORef MasterProfiling))
  }

-- | Connection to a slave.  Requests to the slave can be written, and
-- responses read, with this.
type SlaveConn m key state context input output =
  StatefulConn m key (SlaveReq state context input) (SlaveResp state output)

data Slave m key state context input output = Slave
  { slaveConnection :: !(SlaveConn m key state context input output)
  , slaveStates :: !(HMS.HashMap StateId ())
  }

-- | Exceptions specific to masters.
data MasterException
  = MasterException Text
  | InputMissingException StateId
  | UnusedInputsException [StateId]
  | NoSlavesConnectedException
  | UnexpectedResponse Text Text
  | ExceptionFromSlave Text
  deriving (Eq, Show, Typeable)

instance Exception MasterException

-- | Create a new 'MasterHandle'.
initMaster ::
     (MonadBaseControl IO m, MonadIO m, MonadLogger m)
  => MasterArgs m state context input output
  -> EventManager m key
  -> m (MasterHandle m key state context input output)
initMaster ma@MasterArgs{..} em = do
  let throwME = throwAndLog . MasterException . pack
  case maMaxBatchSize of
    Just maxBatchSize | maxBatchSize < 1 ->
      throwME (printf "Distribute.master: maMaxBatchSize must be > 0 (got %d)" maxBatchSize)
    _ -> return ()
  slaveIdSupply <- newSupply (SlaveId 0) (\(SlaveId n) -> SlaveId (n + 1))
  stateIdSupply <- newSupply (StateId 0) (\(StateId n) -> StateId (n + 1))
  mmp <- case maDoProfiling of
      DoProfiling -> Just <$> newIORef emptyMasterProfiling
      NoProfiling -> return Nothing
  let mh = MasterHandle_
        { mhSlaves = NoSlavesYet mempty
        , mhSlaveIdSupply = slaveIdSupply
        , mhStateIdSupply = stateIdSupply
        , mhEventManager = em
        , mhArgs = ma
        , mhProfiling = mmp
        }
  MasterHandle <$> newMVar mh

-- | This will shut down all the slaves.
closeMaster ::
     (MonadConnect m, S.Store state, S.Store context, S.Store input, S.Store output)
  => MasterHandle m key state context input output
  -> m ()
closeMaster (MasterHandle mhv) =
  withMVar mhv $ \MasterHandle_{..} -> do
    case mhSlaves of
      NoSlavesYet _ -> return ()
      Slaves slaves ->
        -- Ignore all exceptions we might have when quitting the slaves
        forM_ (HMS.toList slaves) $ \(slaveId, slave) -> do
          mbErr <- tryAny $ do
            scEncodeAndWrite (slaveConnection slave) (SReqQuit :: SlaveReq state context input)
            readExpect "closeMaster" (slaveConnection slave) $ \case
              (SRespQuit :: SlaveResp state output) -> return ()
              _ -> Nothing
          case mbErr of
            Left err -> do
              $logWarnJ (printf
                "Error while quitting slave %d (this can happen when the master is closed because of an error): %s" slaveId (show err) :: String)
            Right () -> return ()

{-# INLINE readExpect #-}
readExpect ::
     (MonadIO m, MonadBaseControl IO m, S.Store state, S.Store output)
  => Text
  -> SlaveConn m key state context input output
  -> (SlaveResp state output -> Maybe a)
  -> m a
readExpect loc slave f = do
  resp <- scDecodeAndRead slave
  case resp of
    SRespError err -> throwIO (ExceptionFromSlave err)
    _ -> case f resp of
      Nothing -> throwIO (UnexpectedResponse loc (displayResp resp))
      Just x -> return x

{-# INLINE integrateNewStates #-}
integrateNewStates ::
     HMS.HashMap SlaveId (Slave m key state context input output)
  -> [(SlaveId, HMS.HashMap StateId ())]
  -> HMS.HashMap SlaveId (Slave m key state context input output)
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
  -> m [(SlaveId, [(StateId, [(StateId, input)])])]
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

data UpdateSlaveResp output
  = USRespAddStates
  | USRespRemoveStates !SlaveId [(StateId, ByteString)]
  | USRespUpdate ![(StateId, [(StateId, output)])]
  | USRespInit
  deriving (Eq, Show, Generic)
instance (NFData output) => NFData (UpdateSlaveResp output)

data UpdateSlaveReq input
  = USReqUpdate ![(StateId, [(StateId, input)])]
  | USReqAddStates ![(StateId, ByteString)]
  | USReqRemoveStates !SlaveId !(HS.HashSet StateId)
  deriving (Eq, Show, Generic)
instance (NFData input) => NFData (UpdateSlaveReq input)

data UpdateSlaveStep input output = UpdateSlaveStep
  { ussReqs :: ![(SlaveId, UpdateSlaveReq input)]
  , ussSlavesStatus :: !(HMS.HashMap SlaveId SlaveStatus)
  , ussOutputs :: ![(StateId, [(StateId, output)])]
  }

type SlavesStatus = HMS.HashMap SlaveId SlaveStatus

data SlaveStatus = SlaveStatus
  { _ssStatesToUpdate :: ![StateId]
  , _ssStatesUpdating :: ![StateId]
  , _ssStatesToReceive :: !(HS.HashSet StateId)
  } deriving (Show)
makeLenses ''SlaveStatus

{-
{-# INLINE _assertEnabled #-}
_assertEnabled :: Bool
_assertEnabled = False
-}

{-# INLINE updateAssert #-}
updateAssert :: String -> Bool -> a -> a
updateAssert s b x = x

{-# INLINE updateSlavesStep #-}
updateSlavesStep :: forall m input output.
     (MonadThrow m, MonadLogger m, MonadIO m)
  => Int -- ^ Max batch size
  -> HMS.HashMap StateId [(StateId, input)]  -- we need lookup here, so this should be a 'HashMap'
  -> SlaveId
  -> UpdateSlaveResp output
  -> SlavesStatus
  -> m (UpdateSlaveStep input output)
updateSlavesStep maxBatchSize inputs respSlaveId resp statuses1 = do
  (statuses2, requests, outputs) <- case resp of
    USRespInit -> do
      -- Starting up, find something to update
      addOutputs_ <$> findSomethingToUpdate respSlaveId statuses1
    USRespUpdate outputs -> do
      -- Just updated, find something else
      let removeUpdating updating = updateAssert
            "updateSlavesStep: got bad updating slaves"
            (updating == map fst outputs)
            []
      let statuses' = over (at respSlaveId . _Just) (over ssStatesUpdating removeUpdating) statuses1
      addOutputs outputs <$> findSomethingToUpdate respSlaveId statuses'
    USRespAddStates -> do
      -- The states were added, nothing to do
      return (statuses1, [], [])
    USRespRemoveStates requestingSlaveId states -> do
      -- Some states we requested arrived, put them in the right place and
      -- request something new to do
      let statesIds = map fst states
      let replaceToUpdate states0 = updateAssert
            "updateSlavesStep: expecting no update states when adding the removed states"
            (null states0) statesIds
      let replaceToReceive states0 = updateAssert
            "updateSlavesStep: wrong to receive states"
            (HS.fromList statesIds == states0) HS.empty
      let statuses' = over
            (at requestingSlaveId . _Just)
            (over ssStatesToUpdate replaceToUpdate . over ssStatesToReceive replaceToReceive)
            statuses1
      (statuses'', reqs) <- findSomethingToUpdate requestingSlaveId statuses'
      return (statuses'', (requestingSlaveId, USReqAddStates states) : reqs, mempty)
  return UpdateSlaveStep
    { ussReqs = requests
    , ussSlavesStatus = statuses2
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
      -- If we have some states in ssStatesToUpdate, just update those. Otherwise,
      -- try to steal from another slave.
      let ss = uss HMS.! slaveId
      let (toUpdateInputs, remaining) = takeEnoughStatesToUpdate (_ssStatesToUpdate ss)
      if null toUpdateInputs
        then do -- Steal slaves
          let mbStolen = stealSlavesFromSomebody slaveId uss
          case mbStolen of
            Nothing -> do -- We're done for this slave.
              $logInfoJ ("Tried to get something to do for slave " ++ tshow (unSlaveId slaveId) ++ ", but couldn't find anything")
              return (uss, [])
            Just (uss', stolenFrom, stolenStates) -> do -- Request stolen slaves
              $logInfoJ ("Stealing " ++ tshow (HS.size stolenStates) ++ " states from slave " ++ tshow (unSlaveId stolenFrom) ++ " for slave " ++ tshow (unSlaveId slaveId))
              return (uss', [(stolenFrom, USReqRemoveStates slaveId stolenStates)])
        else do -- Send update command
          let uss' =
                set (at slaveId . _Just . ssStatesToUpdate) remaining $
                set (at slaveId . _Just . ssStatesUpdating) (map fst toUpdateInputs) uss
          return (uss', [(slaveId, USReqUpdate toUpdateInputs)])

    {-# INLINE takeEnoughStatesToUpdate #-}
    takeEnoughStatesToUpdate ::
         [StateId]
      -> ([(StateId, [(StateId, input)])], [StateId])
    takeEnoughStatesToUpdate remaining00 = go remaining00 0
      where
        go remaining0 grabbed = if grabbed >= maxBatchSize
          then ([], remaining0)
          else case remaining0 of
            [] -> ([], remaining0)
            stateId : remaining -> let
              inps = inputs HMS.! stateId
              in first ((stateId, inps) :) (go remaining (grabbed + length inps))

    {-# INLINE stealSlavesFromSomebody #-}
    stealSlavesFromSomebody ::
         SlaveId -- Requesting the slaves
      -> SlavesStatus
      -> Maybe (SlavesStatus, SlaveId, HS.HashSet StateId)
    stealSlavesFromSomebody requestingSlaveId uss = do
      let goodCandidate (slaveId, ss) = do
            guard (slaveId /= requestingSlaveId)
            guard (not (null (_ssStatesToUpdate ss)))
            let (toTransfer, remaining) = takeEnoughStatesToUpdate (_ssStatesToUpdate ss)
            return (slaveId, map fst toTransfer, sum (map (length . snd) toTransfer), remaining, length remaining)
      let candidates :: [(SlaveId, [StateId], Int, [StateId], Int)]
          candidates = mapMaybe goodCandidate (HMS.toList uss)
      guard (not (null candidates))
      -- Pick candidate with highest number of states to steal states from
      -- then the remaining
      let (candidateSlaveId, statesToBeTransferred0, _, remainingStates, _) =
            maximumByEx (comparing (\(_, _, x, _, y) -> (x, y))) candidates
      let statesToBeTransferred = HS.fromList statesToBeTransferred0
      let uss' =
            set (at candidateSlaveId . _Just . ssStatesToUpdate) remainingStates $
            over (at requestingSlaveId . _Just . ssStatesToReceive) (<> statesToBeTransferred) uss
      return (uss', candidateSlaveId, statesToBeTransferred)

data RespReadStatus
  = RRSReadNothing
      {-# UNPACK #-} !Int -- The missing bytes to decode the header
  | RRSReadHeader
      {-# UNPACK #-} !S.SizeTag -- The message size
      {-# UNPACK #-} !Int -- The missing bytes
  deriving (Eq, Show)

{-# INLINE respRead #-}
respRead :: (MonadConnect m, S.Store state, S.Store output) => SlaveConn m key state context input output -> RespReadStatus -> m (Either RespReadStatus (S.Message (SlaveResp state output)))
respRead StatefulConn{..} = go
  where
    go = \case
      RRSReadNothing missing -> do
        scFillByteBuffer scByteBuffer missing
        mbPtr <- BB.unsafeConsume scByteBuffer S.headerLength
        case mbPtr of
          Left missing' -> return (Left (RRSReadNothing missing'))
          Right ptr -> do
            messageMagic <- decode ptr S.magicLength
            unless (S.messageMagic == messageMagic) $
              liftIO . throwIO $ StatefulConnDecodeFailure $ "updateSlaves: Wrong message magic, " ++ show messageMagic
            s <- decode (ptr `plusPtr` S.magicLength) S.sizeTagLength
            goReadHeader s s
      RRSReadHeader s missing -> goReadHeader s missing

    goReadHeader s missing = do
      scFillByteBuffer scByteBuffer missing
      mbPtr <- BB.unsafeConsume scByteBuffer s
      case mbPtr of
        Left missing' -> return (Left (RRSReadHeader s missing'))
        Right ptr -> Right . S.Message <$> decode ptr s

    decode ptr n = catch
      (liftIO (S.decodeIOWithFromPtr S.peek ptr n))
      (\ ex@(S.PeekException _ _) -> throwIO . StatefulConnDecodeFailure . ("updateSlaves: " ++) . show $ ex)

data SlaveWithConnection m key state context input output =
    SlaveWithConnection
        { swcSlaveId :: !SlaveId
        , swcConn :: !(SlaveConn m key state context input output)
        , swcRespReadStatus :: !(IORef RespReadStatus)
        }

data MasterLoopState output = MasterLoopState
  { _mlsSlavesStatus :: !SlavesStatus
  , _mlsInFlightReqs :: !Int
  , mlsOutputs :: !(HMS.HashMap SlaveId [(StateId, [(StateId, output)])])
  }

type SlavesHashTable m key state context input output =
  HT.BasicHashTable key (SlaveWithConnection m key state context input output)

{-# INLINE updateSlaves #-}
updateSlaves :: forall state context input output m key.
     (MonadConnect m, NFData input, NFData output, S.Store state, S.Store context, S.Store input, S.Store output, Eq key, Hashable key)
  => Maybe (IORef MasterProfiling)
  -> EventManager m key
  -> Int -- ^ Max batch size
  -> HMS.HashMap SlaveId (Slave m key state context input output)
  -- ^ Slaves connections
  -> context
  -- ^ Context to the computation
  -> HMS.HashMap StateId [(StateId, input)]
  -- ^ Inputs to the computation
  -> m [(SlaveId, [(StateId, [(StateId, output)])])]
updateSlaves mp em maxBatchSize slaves context inputMap = withProfiling mp mpUpdateSlaves $ do
  connsHashTable :: SlavesHashTable m key state context input output <- liftIO $ HT.newSized (HMS.size slaves)
  let registerSlaves :: [(SlaveId, Slave m key state context input output)] -> m a -> m a
      registerSlaves [] cont = cont
      registerSlaves ((slaveId, slave) : slaves_) cont = do
        let conn = slaveConnection slave
        bracket
          (emControl em (scConnKey conn) ETRead)
          (\() -> emControlDelete em (scConnKey conn))
          (\() -> do
            contRef <- newIORef (RRSReadNothing S.headerLength)
            let swc = SlaveWithConnection slaveId (slaveConnection slave) contRef
            liftIO $ HT.insert connsHashTable (scConnKey conn) swc
            registerSlaves slaves_ cont)
  withProfilingCont mp mpRegisterSlaves $ \regSlavesEnd -> registerSlaves (HMS.toList slaves) $ regSlavesEnd $ do
    (statuses1, inFlight) <- withProfiling mp mpInitializeSlaves $ do
      let slaveStatus0 slave = SlaveStatus
            { _ssStatesToUpdate = HMS.keys (slaveStates slave)
            , _ssStatesUpdating = []
            , _ssStatesToReceive = mempty
            }
      let statuses0 = slaveStatus0 <$> slaves
      foldM sendRespInit (statuses0, 0) (HMS.keys slaves)
    let loop1 !mls0 = do
          let loop2 !mls = \case
                [] -> loop1 mls
                (key, _) : evts -> do
                  mbSwc <- withProfiling mp mpGetSlaveConnection $ liftIO $ HT.lookup connsHashTable key
                  swc <- case mbSwc of
                    Nothing -> fail "Couldn't find slave in map!"
                    Just swc -> return swc
                  mbRes <- masterLoopEntry swc mls
                  case mbRes of
                    Left mls' -> loop2 mls' evts
                    Right res -> case evts of
                      _:_ -> fail "Leftover events even if we're done"
                      [] -> return res
          -- $logDebugJ ("Waiting for slave sockets" :: Text)
          evts <- withProfiling mp mpWait (emWait em)
          -- $logDebugJ ("Got " <> tshow (V.length evts) <> " ready sockets")
          loop2 mls0 (V.toList evts)
    let mls0 :: MasterLoopState output = MasterLoopState statuses1 inFlight HMS.empty
    loop1 mls0
  where
    masterLoopEntry ::
         SlaveWithConnection m key state context input output
      -> MasterLoopState output
      -> m (Either (MasterLoopState output) ([(SlaveId, [(StateId, [(StateId, output)])])]))
    masterLoopEntry swc mls = do
      (mls', done) <- masterLoop swc mls
      if done
        then return (Right (HMS.toList (mlsOutputs mls')))
        else return (Left mls')

    masterLoop ::
         SlaveWithConnection m key state context input output
      -> MasterLoopState output
      -> m (MasterLoopState output, Bool)
    masterLoop swc@SlaveWithConnection{..} mls = do
      rrs <- readIORef swcRespReadStatus
      mbResp <- withProfiling mp mpReceive (respRead swcConn rrs)
      case mbResp of
        Left rrs' -> do
          writeIORef swcRespReadStatus rrs'
          return (mls, False)
        Right (S.Message resp) -> handleResponse swc mls resp

    handleResponse ::
         SlaveWithConnection m key state context input output
      -> MasterLoopState output
      -> SlaveResp state output
      -> m (MasterLoopState output, Bool)
    handleResponse swc@SlaveWithConnection{..} (MasterLoopState statuses inFlight outputs) resp = do
        $logDebugJ ("Received response from slave " <> tshow swcSlaveId <> ": " <> displayResp resp)
        UpdateSlaveStep requests statuses' newOutputs <- withProfiling mp mpUpdateState $ do
          resp' <- case resp of
              SRespInit -> throwIO (UnexpectedResponse "updateSlaves" (displayResp resp))
              SRespResetState -> throwIO (UnexpectedResponse "updateSlaves" (displayResp resp))
              SRespGetStates _ -> throwIO (UnexpectedResponse "updateSlaves" (displayResp resp))
              SRespQuit -> throwIO (UnexpectedResponse "updateSlaves" (displayResp resp))
              SRespError err -> throwIO (ExceptionFromSlave err)
              SRespAddStates -> return USRespAddStates
              SRespRemoveStates requestingSlaveId removedStates ->
                return (USRespRemoveStates requestingSlaveId removedStates)
              SRespUpdate outputs_ -> return (USRespUpdate outputs_)
              SRespGetProfile _ -> throwIO (UnexpectedResponse "updateSlaves" (displayResp resp))
          updateSlavesStep maxBatchSize inputMap swcSlaveId resp' statuses
        let inFlight' = inFlight - 1 + length requests
        forM_ requests $ \(slaveId, req) -> do
          $logDebugJ ("Sending request to slave " <> tshow swcSlaveId <> ": " <> displayReq (usReqToReq req))
          sendRequest (slaveId, req)
        (outputs', done) <- withProfiling mp mpUpdateOutputs $ do
          outputs' <- evaluate $ HMS.insertWith (<>) swcSlaveId newOutputs outputs
          let slaveDone SlaveStatus{..} = null _ssStatesToUpdate && null _ssStatesUpdating && HS.null _ssStatesToReceive
          let noInFlight = inFlight' == 0
          let done = updateAssert
                "handleResponse: in flight requests and slaves disagree on whether we're done"
                (noInFlight == all slaveDone statuses')
                noInFlight
          return (outputs', done)
        let mls' = MasterLoopState statuses' inFlight' outputs'
        if done
          then return (mls', done)
          else do
            -- see if we can decode another message from the chunk we read
            mbResp <- withProfiling mp mpReceive (respRead swcConn (RRSReadNothing S.headerLength))
            case mbResp of
              Left rrs -> do
                writeIORef swcRespReadStatus rrs
                return (mls', False)
              Right (S.Message newResp) -> handleResponse swc mls' newResp

    sendRespInit (statuses, inFlight) slaveId = do
        UpdateSlaveStep{..} <- withProfiling mp mpUpdateState $ updateSlavesStep maxBatchSize inputMap slaveId USRespInit statuses
        forM_ ussReqs sendRequest
        return (ussSlavesStatus, inFlight + length ussReqs)

    usReqToReq = \case
      USReqUpdate inputs -> SReqUpdate context inputs
      USReqRemoveStates x y -> SReqRemoveStates x y
      USReqAddStates y -> SReqAddStates y

    sendRequest :: (SlaveId, UpdateSlaveReq input) -> m ()
    sendRequest (slaveId, req) = do
        let req' :: SlaveReq state context input
            req' = usReqToReq req
        slave <- withProfiling mp mpGetSlave $ evaluate (slaves HMS.! slaveId)
        withProfiling mp mpSend $ scEncodeAndWrite (slaveConnection slave) req'

-- | Send an update request to all the slaves. This will cause each of
-- the slaves to apply 'Distributed.Stateful.Slave.saUpdate' to each of its states. The outputs of
-- these invocations are returned in a 'HashMap'.
--
-- It rebalances the states among the slaves during this process. This
-- way, if some of the slaves finish early, they can help the others
-- out, by moving their workitems elsewhere.
--
-- NOTE: if this throws an exception, then `MasterHandle` may be in an
-- inconsistent state, and the whole computation should be aborted.
{-# INLINE update #-}
update :: forall state context input output m key.
     (MonadConnect m, NFData input, NFData output, NFData state, NFData context
     , S.Store context, S.Store input, S.Store state, S.Store output, Eq key, Hashable key)
  => MasterHandle m key state context input output
  -> context
  -> HMS.HashMap StateId [input]
  -> m (HMS.HashMap StateId (HMS.HashMap StateId output))
update (MasterHandle mv) context inputs0 = modifyMVar mv $ \mh -> withProfiling (mhProfiling mh) mpUpdate $ do
  -- TODO add checks for well-formedness of inputs'
  -- Give state ids to each of the inputs, which will be used to label the
  -- state resulting from invoking saUpdate with that input.
  let sortedInputs = sortBy (comparing fst) (HMS.toList inputs0)
  inputList :: [(StateId, [(StateId, input)])] <-
    withSupplyM (mhStateIdSupply mh) $
    forM sortedInputs $ \(stateId, inps) ->
      (stateId,) <$> mapM (\input -> (, input) <$> askSupplyM) inps
  case mhSlaves mh of
    NoSlavesYet states -> do
      $logWarnJ ("Executing update without any slaves" :: Text)
      stateMap <- liftIO $ HT.fromList states
      outputs <- statefulUpdate Nothing (maUpdate (mhArgs mh)) stateMap context inputList
      statesList' <- liftIO $ HT.toList stateMap
      let mh' = mh{ mhSlaves = NoSlavesYet statesList' }
      return (mh', fmap HMS.fromList . HMS.fromList $ outputs)
    Slaves slaves -> do
      $logInfoJ ("Executing update with " ++ tshow (HMS.size slaves) ++ " slaves")
      let inputMap = HMS.fromList inputList
      -- Update the slave states.
      outputs :: [(SlaveId, [(StateId, [(StateId, output)])])] <-
        case maMaxBatchSize (mhArgs mh) of
          Nothing -> do
            slaveIdsAndInputs <- either throwAndLog return $
              assignInputsToSlaves (slaveStates <$> slaves) inputMap
            forM_ slaveIdsAndInputs $ \(slaveId, inps) -> do
              scEncodeAndWrite
                (slaveConnection (slaves HMS.! slaveId))
                (SReqUpdate context inps :: SlaveReq state context input)
            forM slaveIdsAndInputs $ \(slaveId, _) ->
              liftM (slaveId,) $ readExpect "update (1)" (slaveConnection (slaves HMS.! slaveId)) $ \case
                (SRespUpdate outputs :: SlaveResp state output) -> Just outputs
                _ -> Nothing
          Just maxBatchSize ->
              updateSlaves (mhProfiling mh) (mhEventManager mh) maxBatchSize slaves context inputMap
      let mh' = mh
            { mhSlaves = Slaves $
                integrateNewStates slaves (second (fmap (const ()) . HMS.fromList . mconcat . map snd) <$> outputs)
            }
      return (mh', fmap HMS.fromList . HMS.fromList $ foldMap snd outputs)

-- | Adds a connection to a slave.
addSlaveConnection :: forall m state context input output key.
     (MonadConnect m, S.Store state, S.Store context, S.Store input, S.Store output)
  => MasterHandle m key state context input output
  -> SlaveConn m key state context input output
  -> m ()
addSlaveConnection (MasterHandle mhv) conn = modifyMVar_ mhv $ \mh -> do
  slaveId <- askSupply (mhSlaveIdSupply mh)
  -- Init slave
  scEncodeAndWrite conn (SReqInit (maDoProfiling (mhArgs mh)) :: SlaveReq state context input)
  readExpect "initSlave" conn $ \case
      (SRespInit :: SlaveResp state output) -> Just ()
      _ -> Nothing
  -- Send states over
  case mhSlaves mh of
    NoSlavesYet states -> do
      let slaves = HMS.fromList [(slaveId, Slave conn (const () <$> HMS.fromList states))]
      sendStates [(conn, states)]
      return mh{mhSlaves = Slaves slaves}
    Slaves slaves -> do
      return mh{mhSlaves = Slaves (HMS.insert slaveId (Slave conn mempty) slaves)}

sendStates :: forall m state context input output key.
       (Monad m, MonadBaseControl IO m, MonadIO m, S.Store state, S.Store context, S.Store input, S.Store output)
    => [(SlaveConn m key state context input output, [(StateId, state)])]
    -> m ()
sendStates slavesWithStates = do
  -- Send states
  forM_ slavesWithStates $ \(conn, states) -> do
    scEncodeAndWrite conn (SReqResetState states :: SlaveReq state context input)
  forM_ slavesWithStates $ \(conn, _) -> do
    readExpect "sendStates" conn $ \case
      (SRespResetState :: SlaveResp state output) -> Just ()
      _ -> Nothing

-- | This sets the states stored in the slaves. It distributes the
-- states among the currently connected slaves.
--
-- If no slaves are connected, this will throw a
-- 'NoSlavesConnectedException'.
{-# INLINE resetStates #-}
resetStates :: forall state context input output m key.
     (MonadBaseControl IO m, MonadIO m, MonadLogger m, S.Store state, S.Store context, S.Store input, S.Store output)
  => MasterHandle m key state context input output
  -> [state]
  -> m (HMS.HashMap StateId state)
resetStates (MasterHandle mhv) states0 = modifyMVar mhv $ \mh -> do
  -- Get a list of StateIds for states
  allStates <- withSupplyM (mhStateIdSupply mh) $ mapM (\state -> (, state) <$> askSupplyM) states0
  case mhSlaves mh of
    NoSlavesYet _oldStates -> do
      return (mh{mhSlaves = NoSlavesYet allStates}, HMS.fromList allStates)
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
        [ (slaveConnection slave, slavesStates HMS.! slaveId)
        | (slaveId, slave) <- HMS.toList slaves
        ]
      return (mh{mhSlaves = Slaves slaves'}, HMS.fromList allStates)

-- | Get all the 'StateId's of states of the computation handled by a
-- master.
getStateIds ::
     (MonadConnect m)
  => MasterHandle m key state context input output
  -> m (HS.HashSet StateId)
getStateIds (MasterHandle mhv) = do
  mh <- readMVar mhv
  return $ case mhSlaves mh of
    NoSlavesYet states -> HS.fromList (fst <$> states)
    Slaves slaves -> HS.fromList (HMS.keys (fold (fmap slaveStates slaves)))

-- | Fetches current states stored in the slaves.
{-# INLINE getStates #-}
getStates :: forall m state context input output key.
     (MonadConnect m, S.Store state, S.Store context, S.Store input, S.Store output)
  => MasterHandle m key state context input output
  -> m (HMS.HashMap StateId state)
getStates (MasterHandle mhv) = withMVar mhv $ \mh -> do
  -- Send states
  case mhSlaves mh of
    NoSlavesYet states -> return $ HMS.fromList states
    Slaves slaves -> do
      forM_ slaves (\slave_ -> scEncodeAndWrite (slaveConnection slave_) (SReqGetStates :: SlaveReq state context input))
      responses <- forM (HMS.elems slaves) $ \slave_ -> do
        readExpect "getStates" (slaveConnection slave_) $ \case
          (SRespGetStates states :: SlaveResp state output) -> Just states
          _ -> Nothing
      return (HMS.fromList $ mconcat responses)

-- | Retrieves performance profiling data for each slave.
getSlavesProfiling :: forall m state context input output key.
       (MonadConnect m, S.Store state, S.Store context, S.Store input, S.Store output)
    => MasterHandle m key state context input output
    -> m (HMS.HashMap SlaveId (Maybe SlaveProfiling))
getSlavesProfiling (MasterHandle mhv) = withMVar mhv $ \mh ->
    case mhSlaves mh of
        NoSlavesYet _states -> return HMS.empty
        Slaves slaves -> do
            forM_ slaves $ \slave -> scEncodeAndWrite (slaveConnection slave) (SReqGetProfile :: SlaveReq state context input)
            forM slaves $ \slave ->
                readExpect "getSlavesProfiling" (slaveConnection slave) $ \case
                    (SRespGetProfile sp :: SlaveResp state output) -> return sp
                    _ -> Nothing

-- | Retrieves performance profiling data for the master.
getMasterProfilng ::
       MonadConnect m
    => MasterHandle m key state context input output
    -> m (Maybe MasterProfiling)
getMasterProfilng (MasterHandle mhv) = readMVar mhv >>= \mh ->
    case mhProfiling mh of
        Nothing -> return Nothing
        Just mp -> Just <$> readIORef mp

-- | Retrieves performance profiling data for the whole computation, master and slaves.
--
-- The profiling data for the slaves is summed.
getProfiling :: forall m state context input output key.
       (MonadConnect m, S.Store state, S.Store context, S.Store input, S.Store output)
    => MasterHandle m key state context input output
    -> m (Maybe Profiling)
getProfiling mhandle@(MasterHandle mhv) = readMVar mhv >>= \mh ->
    case mhProfiling mh of
        Nothing -> return Nothing
        Just mpref -> do
            sp <- catMaybes . HMS.elems <$> getSlavesProfiling mhandle >>= \case
                [] -> return Nothing
                sp:sps -> return . Just $ foldl' (<>) sp sps
            mp <- readIORef mpref
            return . Just $ Profiling mp sp

-- | Get the number of slaves connected to a master.
getNumSlaves ::
     (MonadConnect m)
  => MasterHandle m conn state context input output
  -> m Int
getNumSlaves (MasterHandle mhv) = do
  mh <- readMVar mhv
  return $ case mhSlaves mh of
    NoSlavesYet _ -> 0
    Slaves slaves -> HMS.size slaves
