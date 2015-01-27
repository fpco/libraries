{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric      #-}
{-# LANGUAGE FlexibleContexts   #-}
{-# LANGUAGE NoImplicitPrelude  #-}
{-# LANGUAGE OverloadedStrings  #-}
{-# LANGUAGE ConstraintKinds  #-}
-- | Distribute a "Data.WorkQueue" queue over a network via
-- "Data.Streaming.NetworkMessage".
--
-- Run 'withMaster' on the master node, and then 'runSlave' on slaves. Use the
-- operations in "Data.WorkQueue" to assign items that will be performed.
module Distributed.WorkQueue
    ( withMaster
    , runSlave
    , runArgs
    , DistributedWorkQueueException (..)
    , module Data.WorkQueue
    ) where

import ClassyPrelude
import Control.Concurrent.Async      (withAsync)
import Control.Monad.Trans.Control
import Data.Binary                   (Binary)
import Data.Function                 (fix)
import Data.Streaming.Network
import Data.Streaming.NetworkMessage
import Data.Text.Read                (decimal)
import Data.WorkQueue
import System.Environment            (getProgName)

data ToSlave initialData payload
    = TSInit initialData
    | TSPayload payload
    | TSDone
    deriving (Generic, Typeable)
instance (Binary a, Binary b) => Binary (ToSlave a b)

-- | Decide what to run based on command line arguments.
--
-- This will either run in dev mode (everything on a single machine), master
-- mode, or slave mode. Command line interface should be written correctly via
-- optparse-applicative at some point.
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
    args <- getArgs
    case args of
        ["dev", slavesT]
            | Right (slaves, "") <- decimal slavesT -> dev slaves
        ["master", lslavesT, portT]
            | Right (lslaves, "") <- decimal lslavesT
            , Right (port, "") <- decimal portT -> master lslaves port
        ["slave", host, portT]
            | Right (port, "") <- decimal portT -> slave host port
        _ -> do
            pn <- pack <$> getProgName
            mapM_ putStrLn
                [ "Usage:"
                , ""
                , pn ++ " dev <local slave count>"
                , "    Run program locally (no distribution)"
                , ""
                , pn ++ " master <local slave count> <port>"
                , "    Start a master listening on given port"
                , ""
                , pn ++ " slave <remote host> <remote port>"
                , "    Connect to a master on the given host and port"
                ]
  where
    dev slaves | slaves < 1 = error "Must use at least one slave"
    dev slaves = withWorkQueue $ \queue -> do
        initialData <- getInitialData
        withLocalSlaves queue slaves (calc initialData) (inner initialData queue)

    master lslaves port = do
        initialData <- getInitialData
        withMaster (runTCPServer ss) defaultNMSettings initialData $ \queue ->
            withLocalSlaves
                queue
                lslaves
                (calc initialData)
                (inner initialData queue)
      where
        ss = setAfterBind (const $ putStrLn $ "Listening on " ++ tshow port)
                          (serverSettingsTCP port "*")

    slave host port = runSlave
        (clientSettingsTCP port $ fromString $ unpack host)
        defaultNMSettings
        calc

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
       , Sendable initialData
       , Sendable payload
       , Sendable result
       )
    => ((AppData -> IO ()) -> IO ()) -- ^ run the network application
    -> NMSettings
    -> initialData
    -> (WorkQueue payload result -> m final)
    -> m final
withMaster runApp nmSettings initial inner =
    control $ \runInBase -> withWorkQueue
            $ \queue -> withAsync (server queue)
            $ const $ runInBase $ inner queue
  where
    server queue = runApp $ runNMApp nmSettings $ \nm -> do
        nmWrite nm $ TSInit initial
        provideWorker queue $ \payload -> do
            nmWrite nm $ TSPayload payload
            nmRead nm
        nmWrite nm TSDone

-- | Run a slave to perform computations for a remote master (started with
-- 'withMaster').
runSlave
    :: ( MonadIO m
       , Sendable initialData
       , Sendable payload
       , Sendable result
       )
    => ClientSettings
    -> NMSettings
    -> (initialData -> payload -> IO result)
    -> m ()
runSlave cs nmSettings calc =
    liftIO $ runTCPClient cs $ runNMApp nmSettings nmapp
  where
    nmapp nm = do
        ts0 <- nmRead nm
        case ts0 of
            TSInit initialData -> fix $ \loop -> do
                ts <- nmRead nm
                case ts of
                    TSInit _ -> throwIO UnexpectedTSInit
                    TSPayload payload -> do
                        calc initialData payload >>= nmWrite nm
                        loop
                    TSDone -> return ()
            TSPayload _ -> throwIO UnexpectedTSPayload
            TSDone -> throwIO UnexpectedTSDone

data DistributedWorkQueueException
    = UnexpectedTSInit
    | UnexpectedTSPayload
    | UnexpectedTSDone
    deriving (Show, Typeable)
instance Exception DistributedWorkQueueException
