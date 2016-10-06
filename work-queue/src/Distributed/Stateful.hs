{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE BangPatterns #-}
{-|
Module: Distributed.Stateful
Description: Distribute stateful computations.

Distribute stateful computations.  Each step in a computation is of
type 'Update', which is an alias for

@
context -> input -> state -> m (state, output)
@

Each computation has its own @state@, which is updated along
with producing some @output@ from a given @input@.  There is also a
@context@ which is the same for all computations.

Computations with different state can be distributed to multiple
slaves (see "Distributed.Stateful.Slave"), and are coordinated by a
master.  There is no one-to-one correspondence between states and
slaves; a single slave can handle multiple computations with different
states.  If there are no slaves available, the master will perform the
computation by itself.

Masters and slaves that communicate over an abstract communication
channel ('StatefulConn') are defined in "Distributed.Stateful.Master"
and "Distributed.Stateful.Slave", respectively.  This module uses
"Data.Streaming.NetworkMessage" for communication.  On top of that, it
also provides 'runJobQueueStatefulWorker', which uses a job queue in
order to run multiple distributed stateful computations.

A reference implementation using a single process, and 'TMChan's for
communication between the threads, is also provided.
-}



module Distributed.Stateful where
    {-
    ( -- * Re-exported types
      Update
      -- * NetworkMessage backend
    , runNMStatefulSlave
    , runNMStatefulMaster
    , runSimpleNMStateful
      -- * NetworkMessage backend with automatic slave request
    , NMStatefulMasterArgs(..)
    , runRequestedStatefulSlave
    , runRequestingStatefulMaster
      -- * Use a job queue to schedule multiple stateful computations
    , runJobQueueStatefulWorker
      -- * Reference implementation using one process
    , runPureStatefulSlave
    , runSimplePureStateful
    ) where
    -}

import           ClassyPrelude
import           Control.DeepSeq (NFData)
import qualified Control.Concurrent.Mesosync.Lifted.Safe as Async
import           Distributed.Heartbeat
import           Distributed.Stateful.Slave
import           Distributed.Stateful.Internal
import           Distributed.Stateful.Master
import           FP.Redis (MonadConnect)
import           Data.Store (Store)
import qualified Data.Store.Streaming as S
import           Control.Concurrent.STM.TMChan
import           Data.Streaming.NetworkMessage
import           Control.Monad.Logger.JSON.Extra (logErrorJ, logWarnJ, logDebugJ)
import           Control.Concurrent (threadDelay)
import qualified Control.Concurrent.STM as STM
import qualified Data.Conduit.Network as CN
import           Control.Monad.Trans.Control (control)
import           Data.Void (absurd)
import           Data.Store.TypeHash (HasTypeHash)
import           Distributed.Redis (Redis, withRedis)
import           Distributed.RequestSlaves
import qualified Data.Streaming.Network.Internal as CN
import           Distributed.JobQueue.Worker
import           Distributed.Types
import           Distributed.JobQueue.MasterOrSlave
import           FP.Redis (Milliseconds)
import qualified System.IO.ByteBuffer as BB
import           Data.Streaming.Network (AppData, appRead, appWrite)
import           Data.Streaming.Network.Internal (AppData(appRawSocket'))
import           Network.Socket (Socket(..))
import           System.Posix.Types (Fd(..))
import           GHC.Event (getSystemEventManager, registerFd, unregisterFd, evtRead, Lifetime(..), IOCallback)
import           Control.Monad.Trans.Unlift (askUnliftBase, unliftBase)
import           GHC.Conc (threadWaitRead)

-- * Pure version, useful for testing, debugging, etc.
-----------------------------------------------------------------------

-- | Run a slave that communicates with a master via 'TMChan's.
runPureStatefulSlave :: forall m context input state output a.
       (MonadConnect m, NFData state, NFData output, Store state, Store context, Store output, Store input, NFData input, NFData context)
    => (context -> input -> state -> m (state, output))
    -> (SlaveConn m state context input output -> m a)
    -> m a
runPureStatefulSlave update_ cont = do
    reqChan :: TMChan ByteString <- liftIO newTMChanIO
    respChan :: TMChan ByteString <- liftIO newTMChanIO
    chanStatefulConn respChan reqChan $ \slaveConn ->
        chanStatefulConn reqChan respChan $ \masterConn ->
            fmap snd $ Async.concurrently
                (runSlave (SlaveArgs update_ slaveConn))
                (finally (cont masterConn) $ atomically $ do
                    closeTMChan reqChan
                    closeTMChan respChan)
  where
    chanStatefulConn :: forall req resp. Store resp =>
        TMChan ByteString -> TMChan ByteString -> (StatefulConn m req resp -> m a) -> m a
    chanStatefulConn reqChan respChan cont = BB.with Nothing $ \bb ->
        let bareRead = do
                mbX <- atomically $ do
                    closed <- isClosedTMChan respChan
                    if closed
                        then return Nothing
                        else readTMChan respChan
                case mbX of
                    Nothing -> fail "runPureStatefulSlave: trying to read on closed chan"
                    Just x -> return x
        in cont StatefulConn
            { scWrite = \x -> do
                closed <- atomically $ do
                    closed <- isClosedTMChan reqChan
                    if closed
                        then return True
                        else do
                            writeTMChan reqChan x
                            return False
                when closed $
                    fail "runPureStatefulSlave: trying to write to closed chan"
            , scRead = bareRead
            , scRegisterCanRead = \callback cont -> do
                let loop :: m void
                    loop = do
                        closed <- atomically $ do
                            let waitUntilClosed = do
                                    closed <- isClosedTMChan respChan
                                    unless closed STM.retry
                            (True <$ waitUntilClosed) <|> (False <$ void (peekTMChan respChan))
                        unless closed $ do
                            callback
                            loop
                        fail "scRegisterCanRead read input from closed channel."
                -- either absurd id <$> Async.race loop cont
                Async.withAsync loop (\_ -> cont) -- TODO fix this, re-throw exceptions in the loop
            , scByteBuffer = bb
            , scPeek = S.peekMessage' bb
            , scFillByteBuffer = do
                    bs <- bareRead
                    BB.copyByteString bb bs
                    return True
            , scWaitRead = void . atomically $ peekTMChan respChan
            }

-- | Run a computation, where the slaves run as separate threads
-- within the same process, and communication is performed via
-- 'TMChan's.
runSimplePureStateful :: forall m context input state output a.
       (MonadConnect m, NFData state, NFData output, Store state, Store context, Store input, Store output, NFData input, NFData context)
    => MasterArgs m state context input output
    -> Int -- ^ Desired slaves. Must be >= 0
    -> (MasterHandle m state context input output -> m a)
    -> m a
runSimplePureStateful ma slavesNum0 cont = if slavesNum0 < 0
    then fail "runSimplePureStateful: slavesNum0 < 0"
    else do
        mh <- initMaster ma
        go mh slavesNum0
  where
    go mh slavesNum = if slavesNum == 0
        then finally (cont mh) (closeMaster mh)
        else runPureStatefulSlave (maUpdate ma) $ \conn -> do
          addSlaveConnection mh conn
          go mh (slavesNum - 1)

nmStatefulConn :: (MonadConnect m, Store a, Store b) => NMAppData a b -> StatefulConn m a b
nmStatefulConn ad = StatefulConn
    { scWrite = nmRawWrite ad
    , scRegisterCanRead = \callback cont ->
          let fd = nmFileDescriptor ad
              loop = do
                  liftIO $ threadWaitRead fd
                  callback
                  loop

          in either id absurd <$> Async.race cont loop
    , scRead = nmRawRead ad
    , scByteBuffer = nmByteBuffer ad
    , scPeek = nmPeek ad
    , scFillByteBuffer = nmFillByteBuffer ad
    , scWaitRead = nmWaitRead ad
    }

-- | Run a slave that uses "Data.Streaming.NetworkMessage" for sending
-- and receiving data.
runNMStatefulSlave ::
       (MonadConnect m, NFData state, Store state, NFData output, Store output, Store context, Store input, NFData input, NFData context)
    => Update m state context input output
    -> NMApp (SlaveResp state output) (SlaveReq state context input) m ()
runNMStatefulSlave update_ ad = runSlave SlaveArgs
    { saUpdate = update_
    , saConn = nmStatefulConn ad
    }

-- | Connection preferences for a master that communicates with slaves
-- via "Data.Streaming.NetworkMessage".
data NMStatefulMasterArgs = NMStatefulMasterArgs
    { nmsmaMinimumSlaves :: !(Maybe Int)
    -- ^ The minimum amount of slaves master needs to proceed. If present, must be
    -- greater or equal to 0. With no slaves will be waited for and
    -- master will do all the work if no slaves connect.
    , nmsmaMaximumSlaves :: !(Maybe Int)
    -- ^ The maximum amount of slaves to accept. If present, must be >= 0.
    , nmsmaSlavesWaitingTime :: !Int
    -- ^ How much to wait for slaves, in microseconds.
    }

-- | Run a master that uses "Data.Streaming.NetworkMessage" for sending
-- and receiving data.
runNMStatefulMaster :: forall m state output input context b.
       (MonadConnect m, NFData state, Store state, NFData output, Store output, Store context, Store input)
    => MasterArgs m state context input output
    -> NMStatefulMasterArgs
    -> (forall void. NMApp (SlaveReq state context input) (SlaveResp state output) m () -> m void)
    -- ^ Function providing access to running a NMApp server. The first argument
    -- is the handler that gets called on each connection. The second argument
    -- is a continuation that will run alongside with the server, when the continuation
    -- quits the server will quit too.
    -> (MasterHandle m state context input output -> m () -> m b)
    -- ^ The second argument is a function that will return when the maximum number
    -- of slaves is reached. It'll never return if there is no maximum. This is useful
    -- if for example we're requesting the slaves with the RequestSlaves and we want
    -- to stop.
    -> m (Maybe b)
    -- ^ 'Nothing' if we ran out of time waiting for slaves.
runNMStatefulMaster ma NMStatefulMasterArgs{..} runServer cont = do
    case nmsmaMinimumSlaves of
        Just n | n < 0 -> fail ("runTCPStatefulMaster: nmsmaMinimumSlaves < 0 (" ++ show n ++ ")")
        _ -> return ()
    case nmsmaMaximumSlaves of
        Just n -> do
            let minMax = fromMaybe 0 nmsmaMinimumSlaves
            when (n < minMax) $
              fail ("runTCPStatefulMaster: nmsmaMaximumSlaves < " ++ show minMax ++ " (" ++ show n ++ ")")
        _ -> return ()
    mh <- initMaster ma
    slavesConnectedVar :: TVar Int <- liftIO (newTVarIO 0)
    slaveAddLock :: MVar () <- newMVar ()
    doneVar :: MVar () <- newEmptyMVar
    let onSlaveConnection :: NMApp (SlaveReq state context input) (SlaveResp state output) m ()
        onSlaveConnection ad = do
            added <- withMVar slaveAddLock $ \() -> do
                slaves <- atomically (readTVar slavesConnectedVar)
                let shouldAdd = case nmsmaMaximumSlaves of
                        Nothing -> True
                        Just n -> slaves <= n
                when shouldAdd $ do
                    addSlaveConnection mh (nmStatefulConn ad)
                    atomically (writeTVar slavesConnectedVar (slaves + 1))
                return shouldAdd
            if added
                then readMVar doneVar
                else $logWarnJ ("Slave tried to connect while past the number of maximum slaves" :: Text)
    let server :: m (Maybe b)
        server = do
            let waitForMaxSlaves n = atomically $ do
                    connected <- readTVar slavesConnectedVar
                    unless (connected == n) STM.retry
            let doneWaitingForSlaves = case nmsmaMaximumSlaves of
                    Nothing -> liftIO (forever (threadDelay maxBound))
                    Just n -> waitForMaxSlaves n
            let wait = liftIO (threadDelay nmsmaSlavesWaitingTime)
            case nmsmaMaximumSlaves of
                Nothing -> wait
                Just n -> void (Async.race wait (waitForMaxSlaves n))
            slaves <- getNumSlaves mh
            let minSlaves = fromMaybe 0 nmsmaMinimumSlaves
            if slaves < minSlaves
                then do
                    $logErrorJ ("Timed out waiting for slaves to connect. Needed " ++ tshow minSlaves ++ ", got " ++ tshow slaves)
                    return Nothing
                else Just <$> cont mh doneWaitingForSlaves
    fmap (either absurd id) $ Async.race (runServer onSlaveConnection) $ finally server $ do
        closeMaster mh
        putMVar doneVar ()

-- | Run a computation, spawning both a master and as many slaves as
-- wanted.
runSimpleNMStateful :: forall m state input output context a.
       ( MonadConnect m
       , NFData state, NFData output, NFData input, NFData context
       , Store state, Store output, Store context, Store input
       , HasTypeHash state, HasTypeHash input, HasTypeHash output, HasTypeHash context
       )
    => ByteString -- ^ Desired host for the master
    -> MasterArgs m state context input output
    -> Int -- ^ Desired slaves
    -> (MasterHandle m state context input output -> m a)
    -> m a
runSimpleNMStateful host ma numSlaves cont = do
    when (numSlaves < 0) $
        fail ("runSimpleNMStateful: numSlaves < 0 (" ++ show numSlaves ++ ")")
    (ss, getPort) <- liftIO (getPortAfterBind (CN.serverSettings 0 "*"))
    nmSettings <- defaultNMSettings
    let acceptConns :: forall void.
            NMApp (SlaveReq state context input) (SlaveResp state output) m () -> m void
        acceptConns f = CN.runGeneralTCPServer ss (runNMApp nmSettings f)
    let nmsma = NMStatefulMasterArgs
            { nmsmaMinimumSlaves = Just numSlaves
            , nmsmaMaximumSlaves = Just numSlaves
            , nmsmaSlavesWaitingTime = 5 * 1000 * 1000
            }
    let runAllSlaves = do
            port <- liftIO getPort
            let cs = CN.clientSettings port host
            go cs nmSettings numSlaves
    mbX <- fmap snd $ Async.concurrently runAllSlaves $
        runNMStatefulMaster ma nmsma acceptConns (\mh wait -> wait >> cont mh)
    case mbX of
        Nothing -> fail ("runSimpleNMStateful: failed waiting for slaves, should not happen.")
        Just x -> return x
  where
    go cs nmSettings numSlaves_ = if numSlaves_ == 0
        then return ()
        else void $ Async.concurrently
            (do () <- control (\invert -> CN.runTCPClient cs (invert . runNMApp nmSettings (runNMStatefulSlave (maUpdate ma))))
                return ())
            (go cs nmSettings (numSlaves_ - 1))

-- | Spawn a slave node that will connect to a master on request.
runRequestedStatefulSlave ::
       (MonadConnect m, NFData state, Sendable state, NFData output, Sendable output, Sendable context, Sendable input, NFData input, NFData context)
    => Redis
    -> Milliseconds
    -> Update m state context input output
    -> m void
runRequestedStatefulSlave r failsafeTimeout update_ =
    connectToMaster r failsafeTimeout (activeOrUnhandledWorkers r) (runNMStatefulSlave update_)

-- | Run a master node that will automatically request existing slave nodes to join it.
runRequestingStatefulMaster :: forall m state output context input b.
       (MonadConnect m, NFData state, Sendable state, NFData output, Sendable output, Sendable context, Sendable input)
    => Redis
    -> Heartbeating
    -> CN.ServerSettings
    -- ^ The settings used to create the server. You can use 0 as port number to have
    -- it automatically assigned
    -> ByteString
    -- ^ The host that will be used by the slaves to connect
    -> Maybe Int
    -- ^ The port that will be used by the slaves to connect.
    -- If Nothing, the port the server is locally bound to will be used
    -> MasterArgs m state context input output
    -> NMStatefulMasterArgs
    -> (MasterHandle m state context input output -> m b)
    -> m (Maybe b)
runRequestingStatefulMaster r (heartbeatingWorkerId -> wid) ss0 host mbPort ma nmsma cont = do
    nmSettings <- defaultNMSettings
    (ss, getPort) <- liftIO (getPortAfterBind ss0)
    whenSlaveConnectsVar :: MVar (m ()) <- newEmptyMVar
    let acceptConns :: forall void.
            NMApp (SlaveReq state context input) (SlaveResp state output) m () -> m void
        acceptConns contSlaveConnect =
            CN.runGeneralTCPServer ss $ \ad -> do
                -- This is just to get the exceptions in the logs rather than on the
                -- terminal: this is run in a separate thread anyway, and so they'd be
                -- lost forever otherwise.
                -- In other words, the semantics of the program are not affected.
                let whenSlaveConnects nm = do
                        join (readMVar whenSlaveConnectsVar)
                        contSlaveConnect nm
                mbExc <- tryAny (runNMApp nmSettings whenSlaveConnects ad)
                case mbExc of
                    Left err ->
                        $logWarnJ ("requestingStatefulMaster: got exception in slave handler " ++ tshow err)
                    Right () -> return ()
    keepRequestingSlaves :: MVar () <- newEmptyMVar
    let runRequestsSlave = do
            -- This is used to get the port if we need it, but also to wait
            -- for the server to be up.
            port <- liftIO getPort
            $logDebugJ ("Master starting on " ++ tshow (CN.serverHost ss, port))
            let wci = WorkerConnectInfo host (fromMaybe port mbPort)
            requestSlaves r wid wci $ \wsc -> do
                putMVar whenSlaveConnectsVar wsc
                takeMVar keepRequestingSlaves
    let stopRequestingSlaves = tryPutMVar keepRequestingSlaves ()
    runNMStatefulMaster ma nmsma
        (\nma -> fmap fst $ Async.concurrently
            (finally (acceptConns nma) stopRequestingSlaves)
            runRequestsSlave)
        (\mh maxSlavesReached ->
            Async.withAsync (cont mh) $ \contAsync ->
            Async.withAsync (maxSlavesReached >> stopRequestingSlaves) $ \maxSlavesAsync -> do
              contOrMaxSlaves <- Async.waitEither contAsync maxSlavesAsync
              case contOrMaxSlaves of
                Left x -> return x
                Right _ -> Async.wait contAsync)

-- | This function creates a job queue worker node to contribute to a
-- distributed stateful computation.
--
-- It uses 'runMasterOrSlave' in order to act as a master or slave for
-- a distributed computation, depending on what is currently needed.
runJobQueueStatefulWorker ::
       (MonadConnect m, NFData state, Sendable state, NFData output, Sendable output, Sendable context, Sendable input, Sendable request, Sendable response, NFData input, NFData context)
    => JobQueueConfig
    -> CN.ServerSettings
    -- ^ The settings used to create the server. You can use 0 as port number to have
    -- it automatically assigned
    -> ByteString
    -- ^ The host that will be used by the slaves to connect
    -> Maybe Int
    -- ^ The port that will be used by the slaves to connect.
    -- If Nothing, the port the server is locally bound to will be used
    -> MasterArgs m state context input output
    -> NMStatefulMasterArgs
    -> (MasterHandle m state context input output -> RequestId -> request -> m (Reenqueue response))
    -> m void
runJobQueueStatefulWorker jqc ss host mbPort ma nmsma cont =
    withRedis (jqcRedisConfig jqc) $ \redis ->
    withHeartbeats (jqcHeartbeatConfig jqc) redis $ \hb -> do
        runMasterOrSlave jqc redis hb
            (\wcis -> connectToAMaster (runNMStatefulSlave (maUpdate ma)) wcis)
            (\reqId req -> do
              mbResp <- runRequestingStatefulMaster redis hb ss host mbPort ma nmsma (\mh -> cont mh reqId req)
              case mbResp of
                Nothing -> liftIO (fail "Timed out waiting for slaves to connect")
                Just resp -> return resp)
