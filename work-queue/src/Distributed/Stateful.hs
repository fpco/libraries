{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
module Distributed.Stateful
  ( WorkerArgs(..)
  , runWorker
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
import           Control.Monad.Logger (LoggingT, runLoggingT, logErrorN)
import qualified Data.Binary as B
import           Data.Binary.Orphans ()
import qualified Data.Conduit.Network as CN
import           Data.Streaming.NetworkMessage
import           Distributed.JobQueue.Worker hiding (MasterFunc, SlaveFunc)
import           Distributed.RedisQueue
import           Distributed.Stateful.Internal
import           Distributed.Stateful.Master
import           Distributed.Stateful.Slave
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
  }

-- | Runs a job-queue worker which implements a stateful distributed
-- computation.
runWorker
    :: forall state context input output request response.
     -- FIXME: remove these, by generalizing job-queue to not require
     -- Typeable..
     ( Typeable request, Typeable response
     , NFData state, NFData output
     , B.Binary state, B.Binary context, B.Binary input, B.Binary output, B.Binary request, B.Binary response
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
runWorker WorkerArgs {..} logFunc slave master =
    runLoggingT (jobQueueNode waConfig slave' master') logFunc
  where
    runLogger f = runLoggingT f logFunc
    slave' :: WorkerParams -> MasterConnectInfo -> IO () -> LoggingT IO ()
    slave' wp mci init = liftIO $ runSlave SlaveArgs
      { saUpdate = slave
      , saInit = init
      , saClientSettings = CN.clientSettings (mciPort mci) (mciHost mci)
      , saNMSettings = wpNMSettings wp
      , saLogFunc = logFunc
      }
    master' :: WorkerParams -> CN.ServerSettings -> RequestId -> request -> LoggingT IO MasterConnectInfo -> LoggingT IO response
    master' wp ss rid r getMci = do
      mh <- mkMasterHandle waMasterArgs logFunc
      nms <- liftIO nmsSettings
      doneVar <- newEmptyMVar
      let acceptConns =
            CN.runGeneralTCPServer ss $ generalRunNMApp nms (const "") (const "") $ \nm -> do
              addSlaveConnection mh nm
              readMVar doneVar
      let runMaster = do
            res <- timeout (1000 * 1000 * fromIntegral (unSeconds waMasterWaitTime)) $
              atomically (check . (== waRequestSlaveCount) =<< getSlaveCount mh)
            slaveCount <- atomically (getSlaveCount mh)
            case (res, slaveCount) of
              (Nothing, 0) -> do
                runLogger $ logErrorN "Timed out waiting for slaves to connect"
                -- FIXME: need a way to cancel being a master.
                fail "Timed out waiting for slaves to connect. (see FIXME comment in code)"
              _ -> liftIO $ master (wpRedis wp) rid r mh
      let requestSlaves = runLogger $
            mapM_ (\_ -> requestSlave (wpRedis wp) =<< getMci) [1..waRequestSlaveCount]
      liftIO $ withAsync (requestSlaves `concurrently` acceptConns) $ \_ ->
        runMaster

nmsSettings :: IO NMSettings
nmsSettings = do
    nms <- defaultNMSettings
    return $ setNMHeartbeat 5000000 nms -- 5 seconds
