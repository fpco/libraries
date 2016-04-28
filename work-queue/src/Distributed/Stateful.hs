{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TemplateHaskell #-}
module Distributed.Stateful
  ( WorkerArgs(..)
  , runWorker
    -- * Run Master and Slave without job-queue
  , runSlaveRedis
  , runMasterRedis
    -- * Re-exports
  , MasterArgs(..)
  , WorkerConfig(..)
  , defaultWorkerConfig
  , MasterHandle
  , StateId
  , update
  , resetStates
  , getStateIds
  , getStates
  , requestSlave
  ) where

import           ClassyPrelude
import           Control.Concurrent.Async (withAsync, concurrently)
import           Control.Concurrent.STM (check)
import           Control.DeepSeq (NFData)
import           Control.Monad.Logger
import qualified Data.Conduit.Network as CN
import           Data.Streaming.NetworkMessage
import           Distributed.JobQueue.Worker hiding (MasterFunc, SlaveFunc)
import           Distributed.JobQueue.Shared (popSlaveRequest, takeMVarE)
import           Distributed.RedisQueue
import           Distributed.Stateful.Internal
import           Distributed.Stateful.Master
import           Distributed.Stateful.Slave hiding (runSlave)
import           Distributed.Stateful.Slave
import           FP.Redis
import           System.Timeout (timeout)
import qualified Data.Serialize as C
import qualified Data.UUID as UUID
import qualified Data.UUID.V4 as UUID.V4

data WorkerArgs = WorkerArgs
  { waConfig :: WorkerConfig
    -- ^ Configuration for the worker. Note that configuration options
    -- relevant to the work-queue portions of things will be ignored.
    -- In particular, 'workerMasterLocalSlaves'.
  , waMasterArgs :: MasterArgs
    -- ^ Configuration to use when this node is a master.
  , waRequestSlaveCount :: Int
    -- ^ How many slaves to request. Note that this is not a guaranteed
    -- number of slaves, just a suggestion.
  , waMasterWaitTime :: Seconds
    -- ^ How long to wait for slaves to connect (if no slaves connect in
    -- this time, then the work gets aborted). If all the requested
    -- slaves connect, then waiting is aborted.
  -- TODO
  -- , waLogFunc :: LogFunc
  }

-- | Runs a job-queue worker which implements a stateful distributed
-- computation.
runWorker
    :: forall state context input output request response.
     ( NFData state, NFData output
     , Sendable state, Sendable context, Sendable input, Sendable output, Sendable request, Sendable response
     )
    => WorkerArgs
    -- ^ Settings to use to run the worker.
    -> LogFunc
    -- ^ Logger function
    -> (context -> input -> state -> IO (state, output))
    -- ^ This function is run on the slave, to perform a unit of work.
    -- See 'saUpdate'.
    -> (RedisInfo -> RequestId -> request -> MasterHandle state context input output -> IO response)
    -- ^ This function runs on the master after it's received a
    -- reqeuest, and after some slaves have connected. The function is
    -- expected to use functions which take 'MasterHandle' to send work
    -- to its slaves. In particular:
    --
    -- 'resetStates' should be used to initialize the states.
    --
    -- 'update' should be invoked in order to execute the computation.
    --
    -- 'getStates' should be used to fetch result states (if necessary).
    -> IO ()
runWorker wa@WorkerArgs {..} logFunc slave master =
    runLogger (jobQueueNode waConfig slave' master')
  where
    runLogger f = runLoggingT f logFunc
    slave' wp mci onInit = liftIO $ runSlaveImpl logFunc slave (wpNMSettings wp) mci onInit
    master' :: WorkerParams -> CN.ServerSettings -> RequestId -> request -> LoggingT IO MasterConnectInfo -> LoggingT IO response
    master' wp ss rid r getMci = LoggingT $ \_ ->
        runMasterImpl wa rid logFunc (master (wpRedis wp) rid r) ss (runLogger getMci)

-- | Runs a slave worker, which connects to the specified master.  Redis is used
-- to inform the slaves of how to connect to the master.
runSlaveRedis
    :: forall state context input output void.
     ( NFData state, NFData output
     , Sendable state, Sendable context, Sendable input, Sendable output
     )
    => WorkerConfig
    -> LogFunc
    -> (RedisInfo -> context -> input -> state -> IO (state, output))
       -- ^ Computation function
    -> NMSettings
    -> IO ()
       -- ^ Init action - run once connection is established.
    -> IO void
runSlaveRedis wacfg logFunc slave nms onInit = do
    let runLogger f = runLoggingT f logFunc
    runLogger $ withWorkerRedis wacfg $ \redis -> do
        (notifiedMVar, unsub) <- subscribeToRequests redis
        (`finally` liftIO unsub) $ forever $ do
            mmci <- popSlaveRequest redis
            case mmci of
                Nothing -> do
                    $logDebugS "runSlaveRedis" "Waiting for request notification"
                    takeMVarE notifiedMVar NoLongerWaitingForRequest
                    $logDebugS "runSlaveRedis" "Got request notification"
                Just mci -> do
                    $logDebugS "runSlaveRedis" ("Got slave request: " <> tshow mci)
                    void $ slaveHandleNMExceptions mci $ do
                        liftIO $ runSlaveImpl logFunc (slave redis) nms mci onInit
                        $logDebugS "runSlaveRedis" ("Done being a slave of " <> tshow mci)

runSlaveImpl
    :: forall state context input output.
     ( NFData state, NFData output
     , Sendable state, Sendable context, Sendable input, Sendable output
     )
    => LogFunc
    -> (context -> input -> state -> IO (state, output))
       -- ^ Computation function
    -> NMSettings
    -> MasterConnectInfo
    -> IO ()
       -- ^ Init action - run once connection is established.
    -> IO ()
runSlaveImpl logFunc slave nms mci onInit = do
    let runLogger f = runLoggingT f logFunc
    void $ runLogger $ slaveHandleNMExceptions mci $ liftIO $ runSlave SlaveArgs
        { saUpdate = slave
        , saInit = onInit
        , saClientSettings = CN.clientSettings (mciPort mci) (mciHost mci)
        , saNMSettings = nms
        , saLogFunc = logFunc
        }

-- | Runs a master worker, which delegates work to slaves. Redis is used
-- to inform the slaves of how to connect to the master.
--
-- Use 'requestSlave' to request that slaves connect and take work.
runMasterRedis
    :: forall state context input output response.
     ( NFData state, NFData output
     , Sendable state, Sendable context, Sendable input, Sendable output
     )
    => WorkerArgs
    -> LogFunc
    -> (MasterHandle state context input output -> IO response)
    -> CN.ServerSettings
    -> IO response
runMasterRedis wa logFunc master ss = do
    (ss', getPort) <- getPortAfterBind ss
    let getMci = do
            port <- case workerExternalPort (waConfig wa) of
                Nothing -> getPort
                Just port -> return port
            return $ MasterConnectInfo (workerHostName (waConfig wa)) port
    -- Synthetic, unique id for debugging purposes
    rid <- RequestId . C.encode . UUID.toWords <$> UUID.V4.nextRandom
    runMasterImpl wa rid logFunc master ss' getMci

runMasterImpl
    :: forall state context input output response.
     ( NFData state, NFData output
     , Sendable state, Sendable context, Sendable input, Sendable output
     )
    => WorkerArgs
    -> RequestId
    -> LogFunc
    -> (MasterHandle state context input output -> IO response)
    -> CN.ServerSettings
    -> IO MasterConnectInfo
    -> IO response
runMasterImpl WorkerArgs{..} reqId logFunc master ss getMci = do
    let runLogger f = runLoggingT f logFunc
    mh <- mkMasterHandle waMasterArgs reqId logFunc
    nms <- liftIO nmsSettings
    doneVar <- newEmptyMVar
    let acceptConns =
          CN.runGeneralTCPServer ss $ runNMApp nms $ \nm -> do
            addSlaveConnection mh nm
            readMVar doneVar
    let runMaster' = do
          res <- timeout (1000 * 1000 * fromIntegral (unSeconds waMasterWaitTime)) $
            atomically (check . (== waRequestSlaveCount) =<< getSlaveCount mh)
          slaveCount <- atomically (getSlaveCount mh)
          case (res, slaveCount) of
            (Nothing, 0) -> do
              runLogger $ logErrorN "Timed out waiting for slaves to connect"
              -- FIXME: need a way to cancel being a master.
              fail "Timed out waiting for slaves to connect. (see FIXME comment in code)"
            _ -> liftIO $ master mh
    let requestSlaves = withWorkerRedis waConfig $ \redis ->
            mapM_ (\_ -> requestSlave redis =<< liftIO getMci) [1..waRequestSlaveCount]
    liftIO $ withAsync (runLogger requestSlaves `concurrently` acceptConns) $ \_ ->
        runMaster' `finally` putMVar doneVar ()

nmsSettings :: IO NMSettings
nmsSettings = do
    nms <- defaultNMSettings
    return $ setNMHeartbeat 5000000 nms -- 5 seconds
