{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE OverloadedStrings #-}

--TODO: Not sure if I like the name of this module. Seems to weird to
--divide up specific integrations into spots like
--"Distributed.JobQueue.WorkQueue", though.

module Distributed.Integrated
    ( StatefulMasterArgs(..)
    , statefulJQWorker
    , workQueueJQWorker
    , runJQWorker
    -- * Running stateful master / slave without job-queue
    , statefulSlave
    , statefulMaster
    ) where

import           ClassyPrelude
import           Control.Concurrent.Async (race, withAsync, concurrently)
import           Control.Concurrent.STM (check)
import           Control.DeepSeq (NFData)
import           Control.Monad.Logger
import qualified Data.Conduit.Network as CN
import           Data.Streaming.NetworkMessage
import           Distributed.ConnectRequest
import           Distributed.JobQueue.Worker
import           Distributed.Redis
import           Distributed.Stateful.Internal as S
import           Distributed.Stateful.Master as S
import           Distributed.Stateful.Slave as S
import           Distributed.Types
import           Distributed.WorkQueue as WQ
import           FP.Redis
import           System.Timeout (timeout)

statefulJQWorker
    :: forall state context input output request response.
     ( NFData state, NFData output
     , Sendable state, Sendable context, Sendable input, Sendable output, Sendable request, Sendable response
     )
    => JobQueueConfig
    -> StatefulMasterArgs
    -> (context -> input -> state -> IO (state, output))
    -> (Redis -> RequestId -> request -> MasterHandle state context input output -> IO response)
    -> IO ()
statefulJQWorker jqc masterConfig slaveFunc masterFunc = do
    let StatefulMasterArgs {..} = masterConfig
    runJQWorker smaLogFunc jqc
        (\_redis wci -> statefulSlave smaLogFunc smaNMSettings smaRedisConfig slaveFunc)
        (\redis rid request -> statefulMaster masterConfig (masterFunc redis rid request))

workQueueJQWorker
    :: (Sendable request, Sendable response, Sendable payload, Sendable result)
    => LogFunc
    -> JobQueueConfig
    -> MasterConfig
    -> (payload -> IO result)
    -> (Redis -> WorkerConnectInfo -> RequestId -> request -> WorkQueue payload result -> IO response)
    -> IO ()
workQueueJQWorker logFunc config masterConfig slaveFunc masterFunc = do
    let runLogger f = runLoggingT f logFunc
    runJQWorker logFunc config
        (\_redis wci -> runLogger $
            WQ.runSlave (masterNMSettings masterConfig) wci (\() -> slaveFunc))
        (\redis rid request ->
            withMaster masterConfig () $ \wci queue ->
                masterFunc redis wci rid request queue)

data MasterOrSlave = Idle | Slave | Master
    deriving (Eq, Ord, Show)

runJQWorker
    :: (Sendable request, Sendable response)
    => LogFunc
    -> JobQueueConfig
    -> (Redis -> WorkerConnectInfo -> IO ())
    -> (Redis -> RequestId -> request -> IO response)
    -> IO ()
runJQWorker logFunc config slaveFunc masterFunc = do
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
            withConnectRequests ((==Idle) <$> readTVar stateVar) redis $ \wci ->
                liftIO $ slaveFunc redis wci
    handleRequests stateVar =
        jobWorker config $ \redis rid request -> liftIO $
            -- Ensure that the node was idle before becoming a master
            -- node. If we got a slave request simultaneously with
            -- getting a work request, the slave request gets
            -- prioritized and the work gets re-enqueud.
            bracket (transitionIdleTo Master stateVar)
                    (\_ -> backToIdle stateVar) $ \wasIdle -> do
                when (not wasIdle) $ reenqueueWork rid
                masterFunc redis rid request
    backToIdle stateVar = atomically $ writeTVar stateVar Idle
    transitionIdleTo state' stateVar = atomically $ do
        state <- readTVar stateVar
        if state == Idle
            then do
                writeTVar stateVar state'
                return True
             else return False

-- * Running stateful master / slave without job-queue

statefulSlave
    :: forall state context input output response.
     ( NFData state, NFData output
     , Sendable state, Sendable context, Sendable input, Sendable output
     )
    => LogFunc
    -> NMSettings
    -> RedisConfig
    -> (context -> input -> state -> IO (state, output))
    -> IO ()
statefulSlave logFunc nms redisConfig slave =
    runLogger $ withRedis redisConfig $ \r ->
        withConnectRequests (return True) r $ \wci ->
            liftIO $ S.runSlave SlaveArgs
                { saUpdate = slave
                -- TODO: do we need saInit anymore?
                , saInit = return ()
                , saConnectInfo = wci
                , saNMSettings = nms
                , saLogFunc = logFunc
                }
  where
    runLogger f = runLoggingT f logFunc

data StatefulMasterArgs = StatefulMasterArgs
  { smaMasterArgs :: MasterArgs
  , smaRequestSlaveCount :: Int
    -- ^ How many slaves to request. Note that this is not a guaranteed
    -- number of slaves, just a suggestion.
  , smaMasterWaitTime :: Seconds
    -- ^ How long to wait for slaves to connect (if no slaves connect in
    -- this time, then the work gets aborted). If all the requested
    -- slaves connect, then waiting is aborted.
  , smaLogFunc :: LogFunc
  , smaRedisConfig :: RedisConfig
  , smaHostName :: ByteString
  , smaNMSettings :: NMSettings
  }

statefulMaster
    :: forall state context input output response.
     ( NFData state, NFData output
     , Sendable state, Sendable context, Sendable input, Sendable output
     )
    => StatefulMasterArgs
    -> (MasterHandle state context input output -> IO response)
    -> IO response
statefulMaster StatefulMasterArgs{..} master = do
    let runLogger f = runLoggingT f smaLogFunc
    (ss, getPort) <- getPortAfterBind (CN.serverSettings 0 "*")
    mh <- mkMasterHandle smaMasterArgs smaLogFunc
    doneVar <- newEmptyMVar
    let acceptConns =
          CN.runGeneralTCPServer ss $ runNMApp smaNMSettings $ \nm -> do
            addSlaveConnection mh nm
            readMVar doneVar
    let runMaster' = do
          res <- timeout (1000 * 1000 * fromIntegral (unSeconds smaMasterWaitTime)) $
            atomically (check . (== smaRequestSlaveCount) =<< getSlaveCount mh)
          slaveCount <- atomically (getSlaveCount mh)
          case (res, slaveCount) of
            (Nothing, 0) -> do
              runLogger $ logErrorN "Timed out waiting for slaves to connect"
              -- FIXME: need a way to cancel being a master.
              fail "Timed out waiting for slaves to connect. (see FIXME comment in code)"
            _ -> liftIO $ master mh
    let requestSlaves = do
            port <- liftIO getPort
            let wci = WorkerConnectInfo smaHostName port
            withRedis smaRedisConfig $ \redis ->
                mapM_ (\_ -> requestWorker redis wci) [1..smaRequestSlaveCount]
    liftIO $ withAsync (runLogger requestSlaves `concurrently` acceptConns) $ \_ ->
        runMaster' `finally` putMVar doneVar ()
