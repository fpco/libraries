{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE ScopedTypeVariables #-}

--TODO: Not sure if I like the name of this module. Seems to weird to
--divide up specific integrations into spots like
--"Distributed.JobQueue.WorkQueue", though.

module Distributed.Integrated
    ( runMasterOrSlave
    ) where

import Control.Monad.Logger
import Control.Concurrent.Async (race)
import Distributed.ConnectRequest
import Distributed.Redis
import Distributed.WorkQueue
import ClassyPrelude
import Distributed.Types
import Distributed.JobQueue.Worker
import Data.Streaming.NetworkMessage
import Distributed.Stateful.Internal

{-
workQueueWorker
    :: (Sendable request, Sendable response, Sendable payload, Sendable result)
    => LogFunc
    -> JobQueueConfig
    -> MasterConfig
    -> (payload -> IO result)
    -> (Redis -> WorkerConnectInfo -> RequestId -> request -> WorkQueue payload result -> IO response)
    -> IO ()
workQueueWorker logFunc config masterConfig slaveFunc masterFunc =
    runMasterOrSlave

statefulSlave
    :: forall state context input output response.
     ( NFData state, NFData output
     , Sendable state, Sendable context, Sendable input, Sendable output
     )
    => LogFunc
    -> RedisConfig
    -> (context -> input -> state -> IO (state, output))
    -> IO ()
statefulSlave logFunc redis slave =
    runLogger $ withRedis (jqcRedisConfig config) $ \r ->
        withConnectRequests (return True) r $ \(WorkerConnectInfo host port) ->
            liftIO $ runSlave SlaveArgs
                { saUpdate = slave
                , saInit = onInit
                , saClientSettings = CN.clientSettings port host
                , saNMSettings = nms
                , saLogFunc = logFunc
                }
  where
    runLogger f = runLoggingT f logFunc

statefulMaster
    :: forall state context input output response.
     ( NFData state, NFData output
     , Sendable state, Sendable context, Sendable input, Sendable output
     )
    => LogFunc
    -> JobQueueConfig
    -> (MasterHandle state context input output -> IO response)
    ->
-}

-- Implementation

data MasterOrSlave = Idle | Slave | Master
    deriving (Eq, Ord, Show)

runMasterOrSlave
    :: (Sendable request, Sendable response, Sendable payload, Sendable result)
    => LogFunc
    -> JobQueueConfig
    -> MasterConfig
    -> (payload -> IO result)
    -> (Redis -> WorkerConnectInfo -> RequestId -> request -> WorkQueue payload result -> IO response)
    -> IO ()
runMasterOrSlave logFunc config masterConfig slaveFunc masterFunc = do
    stateVar <- newTVarIO Idle
    void $
        runLoggingT (handleWorkerRequests stateVar) logFunc `race`
        runLoggingT (handleRequests stateVar) logFunc
  where
    handleWorkerRequests stateVar =
        withRedis (jqcRedisConfig config) $ \redis ->
            -- Fetch connect requests. The first argument is an STM
            -- action which determines whether we want to receive slave
            -- requests. We only want to become a slave if we're not in
            -- 'Master' mode.
            withConnectRequests ((==Idle) <$> readTVar stateVar) redis $ \wci -> do
                runSlave (masterNMSettings masterConfig) wci (\() -> slaveFunc)
    handleRequests stateVar =
        jobWorker config $ \redis rid request -> liftIO $
            -- Ensure that the node was idle before becoming a master
            -- node. If we got a slave request simultaneously with
            -- getting a work request, the slave request gets
            -- prioritized and the work gets re-enqueud.
            bracket (transitionIdleTo Master stateVar)
                    (\_ -> backToIdle stateVar) $ \wasIdle -> do
                when (not wasIdle) $ reenqueueWork rid
                withMaster masterConfig () $ \wci queue ->
                    masterFunc redis wci rid request queue
    backToIdle stateVar = atomically $ writeTVar stateVar Idle
    transitionIdleTo state' stateVar = atomically $ do
        state <- readTVar stateVar
        if state == Idle
            then do
                writeTVar stateVar state'
                return True
             else return False
