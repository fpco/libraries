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
-- | Distribute a "Data.WorkQueue" queue over a network via
-- "Data.Streaming.NetworkMessage".
--
-- Run 'withMaster' on the master node, and then 'runSlave' on slaves. Use the
-- operations in "Data.WorkQueue" to assign items that will be performed.
--
-- Note that despite being the eponymous module of this package, the
-- focus has shifted to "Distributed.Stateful".
module Distributed.WorkQueue
    ( MasterConfig(..)
    , defaultMasterConfig
    , withMaster
    , runSlave
      -- * CLI interface
    , RunMode (..)
    , runModeParser
    , runArgs
      -- * Exceptions
    , DistributedWorkQueueException (..)
      -- * Re-exports
    , module Data.WorkQueue
      -- * Internal utilities
    , handleWorkerException
    ) where

import           ClassyPrelude hiding ((<>))
import           Control.Concurrent.Async (withAsync)
import           Control.Monad.Logger
import           Control.Monad.Trans.Control
import qualified Data.ByteString.Char8 as BS8
import           Data.Function (fix)
import           Data.Proxy (Proxy(..))
import           Data.Serialize (Serialize)
import           Data.Streaming.Network
import           Data.Streaming.NetworkMessage
import           Data.TypeFingerprint
import           Data.WorkQueue
import           Distributed.Types
import           FP.ThreadFileLogger
import           FP.ThreadFileLogger (logIODebugS, logExceptions)
import           GHC.IO.Exception (IOException(IOError))
import           Options.Applicative

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

data RunMode
    = DevMode
        { numSlaves :: Int
        }
    | MasterMode
        { numSlaves :: Int
        , masterPort :: Int
        }
    | SlaveMode WorkerConnectInfo
    deriving (Eq, Ord, Show, Generic, Typeable)

runModeParser :: Parser RunMode
runModeParser = subparser
    (
        metavar "MODE"

        <> command "dev" (info
            (DevMode
                <$> (argument auto (metavar "<local slave count>"))
            )
            (progDesc "Run program locally (no distribution)")
        )

        <> command "master" (info
            (MasterMode
                <$> (argument auto (metavar "<local slave count>"))
                <*> (argument auto (metavar "<port>"))
            )
            (progDesc "Start a master listening on given port")
        )

        <> command "slave" (info
            (SlaveMode <$> (WorkerConnectInfo
                <$> (BS8.pack <$> (argument str (metavar "<master host>")))
                <*> (argument auto (metavar "<master port>")))
            )
            (progDesc "Connect to a master on the given host and port")
        )
    )

-- | Decide what to run based on command line arguments.
--
-- This will either run in dev mode (everything on a single machine), master
-- mode, or slave mode.
runArgs
    :: ( MonadIO m
       , Sendable initialData
       , Sendable payload
       , Sendable result
       )
    => IO initialData -- ^ will not be run in slave mode
    -> (initialData -> payload -> IO result) -- ^ perform a single calculation
    -> (initialData -> WorkQueue payload result -> IO ())
    -> m ()
runArgs getInitialData calc inner = liftIO $ do
    runMode <- execParser $ info (helper <*> runModeParser) fullDesc
    case runMode of
        DevMode slaves -> dev slaves
        MasterMode lslaves port -> master lslaves port
        SlaveMode wci -> slave wci
  where
    dev slaves | slaves < 1 = error "Must use at least one slave"
    dev slaves = withWorkQueue $ \queue -> do
        initialData <- getInitialData
        withLocalSlaves queue slaves (calc initialData) (inner initialData queue)

    master lslaves port = do
        initialData <- getInitialData
        config <- defaultMasterConfig
        withMaster config initialData $ \_ queue ->
            withLocalSlaves
                queue
                lslaves
                (calc initialData)
                (inner initialData queue)
      where
        ss = setAfterBind (const $ putStrLn $ "Listening on " ++ tshow port)
                          (serverSettingsTCP port "*")

    slave wci = do
        nms <- defaultNMSettings
        runThreadFileLoggingT $ runSlave nms wci calc

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
        Left err -> handleWorkerException wci err
  where
    nmapp nm = tryAny $ do
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

handleWorkerException :: (MonadLogger m, MonadIO m) => WorkerConnectInfo -> SomeException -> m ()
handleWorkerException wci (fromException -> Just err@(IOError {})) =
    $logInfoS "runSlave" $
        "Failed to connect to master " <>
        tshow wci <>
        ".  This probably isn't an issue - the master likely " <>
        "already finished or died.  Here's the exception: " <>
      tshow err
handleWorkerException _ (fromException -> Just NMConnectionDropped) =
    $logWarnS "runSlave" $ "Ignoring NMConnectionDropped in slave"
handleWorkerException _ (fromException -> Just NMConnectionClosed) =
    $logWarnS "runSlave" $ "Ignoring NMConnectionClosed in slave"
handleWorkerException _ err = do
    $logErrorS "runSlave" $ "Unexpected exception in slave: " ++ tshow err
    liftIO $ throwIO err
