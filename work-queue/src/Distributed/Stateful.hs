{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE RecordWildCards #-}
module Distributed.Stateful
    ( -- * Pure backend
      runPureStatefulSlave
    , runSimplePureStateful
      -- * NetworkMessage backend
    , runNMStatefulSlave
    , runNMStatefulMaster
    , runSimpleNMStateful
    {-
      -- * NetworkMessage backend with automatic slave request
    , runRequestedStatefulSlave
    , runRequestingStatefulMaster
    -}
    ) where

import           ClassyPrelude
import           Control.DeepSeq (NFData)
import           Data.Serialize.Orphans ()
import qualified Control.Concurrent.Async.Lifted.Safe as Async
import           Distributed.Stateful.Slave
import           Distributed.Stateful.Internal
import           Distributed.Stateful.Master
import           FP.Redis (MonadConnect)
import           Data.Serialize (Serialize)
import           Control.Concurrent.STM.TMChan
import           Data.Streaming.NetworkMessage
import           Control.Monad.Logger (logError)
import           Control.Concurrent (threadDelay)
import qualified Control.Concurrent.STM as STM
import qualified Data.Conduit.Network as CN
import           Control.Monad.Trans.Control (control)
import           Data.Void (absurd)
import           Data.TypeFingerprint (HasTypeFingerprint)
-- import           Distributed.RequestSlaves

import Control.Exception (AsyncException)

-- * Pure version, useful for testing, debugging, etc.
-----------------------------------------------------------------------

-- | Waits for the slave to terminate before quitting. In other words,
-- this will crash if you don't quit the slaves correctly in the continuation.
-- This means that you'll have to run a stateful master from inside the
-- continuation (see runSimplePureStateful)
runPureStatefulSlave :: forall m context input state output a.
       (MonadConnect m, NFData state, NFData output, Serialize state)
    => (context -> input -> state -> m (state, output))
    -> (SlaveConn m state context input output -> m a)
    -> m a
runPureStatefulSlave update_ cont = do
    reqChan :: TMChan (SlaveReq state context input) <- liftIO newTMChanIO
    respChan :: TMChan (SlaveResp state output) <- liftIO newTMChanIO
    let slaveConn = chanStatefulConn respChan reqChan
    let masterConn = chanStatefulConn reqChan respChan
    fmap snd $ Async.concurrently
        (runSlave (SlaveArgs update_ slaveConn))
        (finally (cont masterConn) $ atomically $ do
            closeTMChan reqChan
            closeTMChan respChan)
  where
    chanStatefulConn :: forall req resp.
        TMChan req -> TMChan resp -> StatefulConn m req resp
    chanStatefulConn reqChan respChan = StatefulConn
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
        , scRead = do
            mbX <- atomically $ do
                closed <- isClosedTMChan respChan
                if closed
                    then return Nothing
                    else readTMChan respChan
            case mbX of
                Nothing -> fail "runPureStatefulSlave: trying to read on closed chan"
                Just x -> return x
        }

runSimplePureStateful :: forall m context input state output a.
       (MonadConnect m, NFData state, NFData output, Serialize state)
    => MasterArgs
    -> Int -- ^ Desired slaves
    -> (context -> input -> state -> m (state, output))
    -> (MasterHandle m state context input output -> m a)
    -> m a
runSimplePureStateful ma slavesNum0 update_ cont = if slavesNum0 < 1
    then fail "runSimplePureStateful: slavesNum0 < 1"
    else do
        mh <- initMaster ma
        go mh slavesNum0
  where
    go mh slavesNum = if slavesNum == 0
        then finally (cont mh) (closeMaster mh)
        else runPureStatefulSlave update_ $ \conn -> do
          addSlaveConnection mh conn
          go mh (slavesNum - 1)

-- * JobQueue based version
-----------------------------------------------------------------------

nmStatefulConn :: (MonadConnect m, Serialize a, Serialize b) => NMAppData a b -> StatefulConn m a b
nmStatefulConn ad = StatefulConn
    { scWrite = nmWrite ad
    , scRead = nmRead ad
    }

runNMStatefulSlave ::
       (MonadConnect m, NFData state, Serialize state, NFData output, Serialize output, Serialize context, Serialize input)
    => (context -> input -> state -> m (state, output))
    -> NMApp (SlaveResp state output) (SlaveReq state context input) m ()
runNMStatefulSlave update_ ad = runSlave SlaveArgs
    { saUpdate = update_
    , saConn = nmStatefulConn ad
    }

data NMStatefulMasterArgs = NMStatefulMasterArgs
    { nmsmaMinimumSlaves :: !(Maybe Int)
    -- ^ The minimum amount of slaves master needs to proceed. If present, must be
    -- greater or equal to 1. 1 by default.
    , nmsmaSlaveWaitTimeout :: !Int
    -- ^ How much to wait for slaves, in microseconds.
    }

logException :: (MonadConnect m) => Text -> m a -> m a
logException loc m =
    catch m $ \(err :: SomeException) -> do
        case fromException err of
            Just (_ :: AsyncException) -> return ()
            Nothing -> $logError (loc ++ ": " ++ tshow err)
        throwIO err

runNMStatefulMaster :: forall m state output input context b.
       (MonadConnect m, NFData state, Serialize state, NFData output, Serialize output, Serialize context, Serialize input)
    => MasterArgs
    -> NMStatefulMasterArgs
    -> (forall a. NMApp (SlaveReq state context input) (SlaveResp state output) m () -> m a -> m a)
    -- ^ Function providing access to running a NMApp server. The first argument
    -- is the handler that gets called on each connection. The second argument
    -- is a continuation that will run alongside with the server, when the continuation
    -- quits the server will quit too.
    -> (MasterHandle m state context input output -> m b)
    -> m (Maybe b)
    -- ^ 'Nothing' if we ran out of time waiting for slaves.
runNMStatefulMaster ma NMStatefulMasterArgs{..} runServer cont = do
    minSlaves <- case nmsmaMinimumSlaves of
        Nothing -> return 1
        Just minSlaves -> if minSlaves < 1
            then fail ("runTCPStatefulMaster: minSlaves < 1 (" ++ show minSlaves ++ ")")
            else return minSlaves
    mh <- initMaster ma
    slavesConnectedVar :: TVar Int <- liftIO (newTVarIO 0)
    doneVar :: MVar () <- newEmptyMVar
    let onSlaveConnection :: NMApp (SlaveReq state context input) (SlaveResp state output) m ()
        onSlaveConnection ad = do
            addSlaveConnection mh (nmStatefulConn ad)
            atomically (modifyTVar slavesConnectedVar (+1))
            readMVar doneVar -- Hold the connection for as long as the master runs
    let waitForEnoughSlaves :: m ()
        waitForEnoughSlaves = atomically $ do
            numSlaves <- readTVar slavesConnectedVar
            unless (numSlaves >= minSlaves) STM.retry
    let server :: m (Maybe b)
        server = do
            mbContinue <- Async.race (liftIO (threadDelay nmsmaSlaveWaitTimeout)) waitForEnoughSlaves
            case mbContinue of
                Left () -> do
                    numSlaves <- atomically (readTVar slavesConnectedVar)
                    $logError ("Timed out waiting for slaves to connect. Needed " ++ tshow minSlaves ++ ", got " ++ tshow numSlaves)
                    return Nothing
                Right () -> do
                    Just <$> cont mh
    runServer onSlaveConnection $ finally server $ do
        closeMaster mh
        putMVar doneVar ()

runSimpleNMStateful :: forall m state input output context a.
       ( MonadConnect m
       , NFData state, NFData output
       , Serialize state, Serialize output, Serialize context, Serialize input
       , HasTypeFingerprint state, HasTypeFingerprint input, HasTypeFingerprint output, HasTypeFingerprint context
       )
    => ByteString -- ^ Desired host for the master
    -> MasterArgs
    -> Int -- ^ Desired slaves
    -> (context -> input -> state -> m (state, output))
    -> (MasterHandle m state context input output -> m a)
    -> m a
runSimpleNMStateful host ma numSlaves update_ cont = do
    when (numSlaves < 1) $
        fail ("runSimpleNMStateful: numSlaves < 1 (" ++ show numSlaves ++ ")")
    (ss, getPort) <- liftIO (getPortAfterBind (CN.serverSettings 0 "*"))
    nmSettings <- defaultNMSettings
    let acceptConns :: forall b.
            NMApp (SlaveReq state context input) (SlaveResp state output) m () -> m b -> m b
        acceptConns f cont_ = fmap (either absurd id) $
            Async.race (CN.runGeneralTCPServer ss (runNMApp nmSettings f)) cont_
    let nmsma = NMStatefulMasterArgs
            { nmsmaMinimumSlaves = Just numSlaves
            , nmsmaSlaveWaitTimeout = 5 * 1000 * 1000
            }
    let runAllSlaves = logException "slaves" $ do
            port <- liftIO getPort
            let cs = CN.clientSettings port host
            go cs nmSettings numSlaves
    mbX <- fmap snd $ Async.concurrently runAllSlaves (logException "master" (runNMStatefulMaster ma nmsma acceptConns cont))
    case mbX of
        Nothing -> fail ("runSimpleNMStateful: failed waiting for slaves, should not happen.")
        Just x -> return x
  where
    go cs nmSettings numSlaves_ = if numSlaves_ == 0
        then return ()
        else void $ Async.concurrently
            (do () <- control (\invert -> CN.runTCPClient cs (invert . runNMApp nmSettings (runNMStatefulSlave update_)))
                return ())
            (go cs nmSettings (numSlaves_ - 1))

{-
-- * RequestSlave
-----------------------------------------------------------------------

runRequestedStatefulSlave ::
       (MonadConnect m, NFData state, Serialize state, NFData output, Serialize output, Serialize context, Serialize input)
    => Redis
    -> (context -> input -> state -> m (state, output))
    -> m void
runRequestedStatefulSlave r update_ = connectToMaster r (runNMStatefulSlave update_)

runRequestingStatefulMaster ::
       (MonadConnect m, NFData state, Serialize state, NFData output, Serialize output, Serialize context, Serialize input)
    => Redis
    -> CN.ServerSettings
    -- ^ The settings used to create the server. You can use 0 as port number to have
    -- it automatically assigned
    -> ByteString
    -- ^ The host that will be used by the slaves to connect
    -> Maybe Int
    -- ^ The port that will be used by the slaves to connect.
    -- If Nothing, the port the server is locally bound to will be used
    -> MasterArgs
    -> NMStatefulMasterArgs
    -> (MasterHandle m state context input output -> m b)
    -> m (Maybe b)
runRequestingStatefulMaster r ss host mbPort ma nmsma =
    runNMStatefulMaster ma nmsma (acceptSlaveConnections r ss host mbPort)

-- * JobQueue
-----------------------------------------------------------------------

runJobQueueStateful ::
       (MonadConnect m, NFData state, Serialize state, NFData output, Serialize output, Serialize context, Serialize input)
    => JobQueueConfig
    -> (context -> input -> state -> m (state, output))
    -> (MasterHandle m state context input output -> request -> m response)
    -> m void
runJobQueueStateful jqc = runMasterOrSlave jqc
    (\r wcis -> connectToAMaster r (runNMStatefulSlave update_)
-}