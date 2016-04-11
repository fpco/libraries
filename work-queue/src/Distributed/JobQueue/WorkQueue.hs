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

-- | 'WorkQueue' operations backed by 'JobQueue'.
module Distributed.JobQueue.WorkQueue
    ( workQueueJQWorker
    ) where

import           ClassyPrelude
import           Control.Monad.Logger
import           Data.Streaming.NetworkMessage
import           Distributed.JobQueue.Worker
import           Distributed.Types
import qualified Distributed.WorkQueue as WQ
import           Data.WorkQueue (WorkQueue)

workQueueJQWorker
    :: (Sendable request, Sendable response, Sendable payload, Sendable result)
    => LogFunc
    -> JobQueueConfig
    -> WQ.MasterConfig
    -> (payload -> IO result)
    -> (Redis -> WorkerConnectInfo -> RequestId -> request -> WorkQueue payload result -> IO response)
    -> IO ()
workQueueJQWorker logFunc config masterConfig slaveFunc masterFunc = do
    let runLogger f = runLoggingT f logFunc
    runJQWorker logFunc config
        (\_redis wci -> runLogger $
            WQ.runSlave (WQ.masterNMSettings masterConfig) wci (\() -> slaveFunc))
        (\redis rid request ->
            WQ.withMaster masterConfig () $ \wci queue ->
                masterFunc redis wci rid request queue)
