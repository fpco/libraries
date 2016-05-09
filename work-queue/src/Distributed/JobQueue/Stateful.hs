{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE OverloadedStrings #-}
module Distributed.JobQueue.Stateful (statefulJQWorker) where

import           ClassyPrelude
import           Control.DeepSeq (NFData)
import           Data.Streaming.NetworkMessage
import           Distributed.JobQueue.Worker
import           Distributed.Stateful.Master as S
import           Distributed.Stateful.Slave as S
import           Distributed.Types
import           Distributed.Stateful

statefulJQWorker
    :: forall state context input output request response m.
     ( MonadConnect m
     , NFData state, NFData output
     , Sendable state, Sendable context, Sendable input, Sendable output, Sendable request, Sendable response
     )
    => JobQueueConfig
    -> StatefulMasterArgs
    -> (context -> input -> state -> m (state, output))
    -> (Redis -> RequestId -> request -> MasterHandle state context input output -> m response)
    -> m ()
statefulJQWorker jqc masterConfig slaveFunc masterFunc = do
    let StatefulMasterArgs {..} = masterConfig
    runJQWorker smaLogFunc jqc
        (\_redis wci -> S.runSlave SlaveArgs
            { saUpdate = slaveFunc
            , saInit = return ()
            , saConnectInfo = wci
            , saNMSettings = smaNMSettings
            , saLogFunc = smaLogFunc
            })
        (\redis rid request -> statefulMaster masterConfig rid (masterFunc redis rid request))
