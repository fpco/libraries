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
--
-- REVIEW: Isn't this module obsoleted by Distributed.Integrated now?
module Distributed.WorkQueue.CLI
    ( RunMode (..)
    , runModeParser
    , runArgs
    ) where

import           ClassyPrelude hiding ((<>))
import qualified Data.ByteString.Char8 as BS8
import           Data.Streaming.Network hiding (getPort)
import           Data.Streaming.NetworkMessage
import           Data.WorkQueue
import           Distributed.Types
import           FP.ThreadFileLogger
import           Options.Applicative
import           Distributed.WorkQueue

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
        config0 <- defaultMasterConfig
        let config = config0 { masterServerSettings = serverSettingsTCP port "*" }
        withMaster config initialData $ \_ queue ->
            withLocalSlaves
                queue
                lslaves
                (calc initialData)
                (inner initialData queue)

    slave wci = do
        nms <- defaultNMSettings
        runThreadFileLoggingT $ runSlave nms wci calc

