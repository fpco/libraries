{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
module Distributed.Stateful
  ( WorkerArgs(..)
  , runWorker
    -- * Run Master and Slave without job-queue
  , runSlave
  , runMaster
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
  ) where

import           ClassyPrelude
import           Control.Concurrent.Async (withAsync, concurrently)
import           Control.Concurrent.STM (check)
import           Control.DeepSeq (NFData)
import           Control.Monad.Logger (LoggingT(..), runLoggingT, logErrorN)
import qualified Data.Serialize as B
import qualified Data.Conduit.Network as CN
import           Data.Streaming.NetworkMessage
import           Distributed.JobQueue.Worker hiding (MasterFunc, SlaveFunc)
import           Distributed.RedisQueue
import           Distributed.Stateful.Internal
import           Distributed.Stateful.Master
import           Distributed.Stateful.Slave hiding (runSlave)
import qualified Distributed.Stateful.Slave as Slave
import           FP.Redis
import           System.Timeout (timeout)

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
    slave' wp mci onInit = runSlave wa logFunc slave (wpNMSettings wp) mci onInit
    master' :: WorkerParams -> CN.ServerSettings -> RequestId -> request -> LoggingT IO MasterConnectInfo -> LoggingT IO response
    master' wp ss rid r getMci = LoggingT $ \_ -> do
        let requestSlaves = mapM_ (\_ -> requestSlave (wpRedis wp) =<< getMci) [1..waRequestSlaveCount]
        withAsync (runLogger requestSlaves) $ \_ ->
            runMasterImpl wa logFunc (master (wpRedis wp) rid r) ss

-- | Runs a slave worker, which connects to the specified master.
runSlave
    :: forall state context input output.
     ( NFData state, NFData output
     , Sendable state, Sendable context, Sendable input, Sendable output
     )
    => WorkerArgs
    -> LogFunc
    -> (context -> input -> state -> IO (state, output))
       -- ^ Computation function
    -> NMSettings
    -> MasterConnectInfo
    -> IO ()
       -- ^ Init action - run once connection is established.
    -> LoggingT IO ()
runSlave WorkerArgs {..} logFunc slave nms mci onInit = void $ do
  let runLogger f = runLoggingT f logFunc
  slaveHandleNMExceptions mci $ liftIO $ Slave.runSlave SlaveArgs
    { saUpdate = slave
    , saInit = onInit
    , saClientSettings = CN.clientSettings (mciPort mci) (mciHost mci)
    , saNMSettings = nms
    , saLogFunc = logFunc
    }

-- | Runs a master worker, which delegates work to slaves. It is the
-- responsiblity of the user to tell the slaves to connect with this
-- master.
runMaster
    :: forall state context input output response.
     ( NFData state, NFData output
     , Sendable state, Sendable context, Sendable input, Sendable output, Sendable response
     )
    => WorkerArgs
    -> LogFunc
    -> (IO MasterConnectInfo -> MasterHandle state context input output -> IO response)
    -> CN.ServerSettings
    -> IO response
runMaster wa logFunc master ss = do
    (ss', getPort) <- getPortAfterBind ss
    let getMci = do
            port <- getPort
            return $ MasterConnectInfo (workerHostName (waConfig wa)) port
    runMasterImpl wa logFunc (master getMci) ss'

runMasterImpl
    :: forall state context input output response.
     ( NFData state, NFData output
     , Sendable state, Sendable context, Sendable input, Sendable output
     )
    => WorkerArgs
    -> LogFunc
    -> (MasterHandle state context input output -> IO response)
    -> CN.ServerSettings
    -> IO response
runMasterImpl WorkerArgs{..} logFunc master ss = do
    let runLogger f = runLoggingT f logFunc
    mh <- mkMasterHandle waMasterArgs logFunc
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
    liftIO $ withAsync acceptConns $ \_ ->
        runMaster' `finally` putMVar doneVar ()

nmsSettings :: IO NMSettings
nmsSettings = do
    nms <- defaultNMSettings
    return $ setNMHeartbeat 5000000 nms -- 5 seconds
