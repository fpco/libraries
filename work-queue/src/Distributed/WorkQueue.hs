{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ViewPatterns #-}
module Distributed.WorkQueue
    ( -- * Running the slave
      runSlave
      -- * Running the master
    , MasterConfig(..)
    , defaultMasterConfig
    , withMaster
      -- * Exceptions
    , DistributedWorkQueueException (..)
      -- * Internal utilities
    , handleSlaveException
    ) where

import           ClassyPrelude hiding ((<>))
import           Control.Concurrent.Async (withAsync)
import           Control.Exception (AsyncException)
import           Control.Monad.Logger
import           Control.Monad.Trans.Control
import           Data.Function (fix)
import           Data.Proxy (Proxy(..))
import           Data.Serialize (Serialize)
import           Data.Streaming.Network hiding (getPort)
import           Data.Streaming.NetworkMessage
import           Data.TypeFingerprint
import           Data.WorkQueue
import           Distributed.Types
import           FP.ThreadFileLogger
import           GHC.IO.Exception (IOException(IOError))
import           Options.Applicative
import qualified Data.Text as T

data ToSlave initialData payload
    = TSInit initialData
    | TSPayload payload
    | TSDone
    deriving (Generic, Typeable)
instance (Serialize a, Serialize b) => Serialize (ToSlave a b)

instance (HasTypeFingerprint initialData, HasTypeFingerprint payload) => HasTypeFingerprint (ToSlave initialData payload) where
    typeFingerprint _ = combineTypeFingerprints
        [ typeFingerprint (Proxy :: Proxy initialData)
        , typeFingerprint (Proxy :: Proxy payload)
        ]
    showType _ = "ToSlave (" ++ showType (Proxy :: Proxy initialData) ++ ") (" ++ showType (Proxy :: Proxy payload) ++ ")"

-- Slave running

-- | Run a slave to perform computations for a remote master (started with
-- 'withMaster').
runSlave
    :: (MonadIO m, MonadLogger m, Sendable initialData, Sendable payload, Sendable result)
    => NMSettings
    -> WorkerConnectInfo
    -> (initialData -> payload -> IO result)
    -> m ()
runSlave nms wci@(WorkerConnectInfo host port) calc = do
    let clientSettings = clientSettingsTCP port host
    eres <- liftIO $ runTCPClient clientSettings $ runNMApp nms nmapp
    case eres of
        Right () -> return ()
        Left err -> handleSlaveException wci "Distributed.WorkQueue.Common.runSlave" err
  where
    nmapp nm = try $ do
        let socket = tshow (appSockAddr (nmAppData nm))
        $logIODebugS "runSlave" $ "Starting slave on " ++ socket
        ts0 <- nmRead nm
        case ts0 of
            TSInit initialData -> do
                $logIODebugS "runSlave" $ "Listening for payloads"
                fix $ \loop -> do
                    ts <- nmRead nm
                    case ts of
                        TSInit _ -> throwIO UnexpectedTSInit
                        TSPayload payload -> do
                            $logIODebugS "runSlave" $ "Slave received payload on " ++ socket
                            calc initialData payload >>= nmWrite nm
                            loop
                        TSDone -> return ()
                $logIODebugS "runSlave" $ "Slave finished on " ++ socket
            TSPayload _ -> throwIO UnexpectedTSPayload
            TSDone -> throwIO UnexpectedTSDone

data DistributedWorkQueueException
    = UnexpectedTSInit
    | UnexpectedTSPayload
    | UnexpectedTSDone
    deriving (Show, Typeable)
instance Exception DistributedWorkQueueException

-- Master

data MasterConfig = MasterConfig
    { masterHost :: ByteString
    , masterServerSettings :: ServerSettings
    , masterNMSettings :: NMSettings
    }

defaultMasterConfig :: IO MasterConfig
defaultMasterConfig = do
    nms <- defaultNMSettings
    return MasterConfig
        { masterHost = "localhost"
        , masterServerSettings = serverSettingsTCP 0 "*"
        , masterNMSettings = nms
        }

-- | Start running a server in the background to listen for slaves, create a
-- 'WorkQueue', and assign jobs as necessary.
--
-- The first argument will typically be something like:
--
-- @
-- runTCPServer (serverSettingsTCP 2345 "*")
-- @
--
-- The @initialData@ argument allows you to specify some chunk of data that
-- will be reused for each computation to avoid unnecessary network overhead.
--
-- The inner callback function may then treat the 'WorkQueue' in an abstract
-- way, ignoring whether the operations occur locally or remotely. After the
-- inner function returns, the work queue will be closed, which will cause all
-- slaves to shut down.
--
-- For efficient local computation, see 'withLocalSlave'. For remote slaves,
-- use 'runSlave'.
withMaster
    :: ( MonadBaseControl IO m
       , MonadIO m
       , Sendable initialData
       , Sendable payload
       , Sendable result
       )
    => MasterConfig
    -> initialData
    -> (WorkerConnectInfo -> WorkQueue payload result -> m final)
    -> m final
withMaster MasterConfig {..} initial inner =
    control $ \runInBase -> withWorkQueue $ \queue -> do
        (ss, getPort) <- liftIO $ getPortAfterBind masterServerSettings
        withAsync (server ss queue) $ \_ -> runInBase $ do
            port <- liftIO getPort
            inner (WorkerConnectInfo masterHost port) queue
  where
    -- Runs a 'forkIO' based server to which slaves connect. Each
    -- connection to a slave runs 'provideWorker' to run a local
    -- worker on the master that delegates the payloads it receives to
    -- the slave.
    server ss queue = do
        logExceptions "server" $ runTCPServer ss $ runNMApp masterNMSettings $ \nm -> do
            let socket = tshow (appSockAddr (nmAppData nm))
            $logIODebugS "withMaster" $ "Master initializing slave on " ++ socket
            nmWrite nm $ TSInit initial
            provideWorker queue $ \payload -> do
                nmWrite nm $ TSPayload payload
                nmRead nm
            $logIODebugS "withMaster" $ "Master finalizing slave on " ++ socket
            nmWrite nm TSDone
            $logIODebugS "withMaster" $ "Master done finalizing slave on " ++ socket

handleSlaveException :: (MonadLogger m, MonadIO m) => WorkerConnectInfo -> T.Text -> SomeException -> m ()
handleSlaveException wci fun (fromException -> Just err@(IOError {})) =
    $logInfoS fun $
        "Failed to connect to master " <>
        tshow wci <>
        ".  This probably isn't an issue - the master likely " <>
        "already finished or died.  Here's the exception: " <>
      tshow err
handleSlaveException _ fun (fromException -> Just NMConnectionDropped) =
    $logWarnS fun $ "Ignoring NMConnectionDropped in slave"
handleSlaveException _ fun (fromException -> Just NMConnectionClosed) =
    $logWarnS fun $ "Ignoring NMConnectionClosed in slave"
handleSlaveException _ _ (fromException -> Just err) =
    liftIO $ throwIO (err :: AsyncException)
handleSlaveException _ fun err = do
    $logErrorS fun $ "Unexpected exception in slave: " ++ tshow err
    liftIO $ throwIO err