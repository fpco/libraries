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
  , WorkerConfig
  , MasterHandle
  , StateId
  , update
  , resetStates
  , getStateIds
  , getStates
  ) where

import           ClassyPrelude
import           Control.DeepSeq (NFData)
import           Control.Monad.Logger (MonadLogger)
import           Control.Monad.Trans.Control (MonadBaseControl)
import qualified Data.Binary as B
import           Data.Binary.Orphans ()
import qualified Data.Conduit.Network as CN
import           Distributed.JobQueue.Worker hiding (MasterFunc, SlaveFunc)
import           Distributed.RedisQueue
import           Distributed.Stateful.Master
import           Distributed.Stateful.Slave
import           FP.Redis

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
    :: forall state context input output request response m.
     ( MonadIO m, MonadBaseControl IO m, MonadCatch m, MonadLogger m
     -- FIXME: remove these, by generalizing job-queue to not require
     -- Typeable..
     , Typeable request, Typeable response
     , NFData state, B.Binary state, B.Binary context, B.Binary input, B.Binary output, B.Binary request, B.Binary response
     )
    => WorkerArgs
    -- ^ Settings to use to run the worker.
    -> (context -> input -> state -> IO (state, output))
    -- ^ This function is run on the slave, to perform a unit of work.
    -- See 'saUpdate'.
    -> (RedisInfo -> RequestId -> request -> MasterHandle state context input output -> m response)
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
    -> m ()
runWorker WorkerArgs {..} slave master =
  jobQueueNode waConfig slave' master'
  where
    slave' :: WorkerParams -> MasterConnectInfo -> IO () -> m ()
    slave' wp mci init = runSlave SlaveArgs
      { saUpdate = slave
      , saInit = init
      , saClientSettings = CN.clientSettings (mciPort mci) (mciHost mci)
      , saNMSettings = wpNMSettings wp
      }
    master' :: WorkerParams -> CN.ServerSettings -> RequestId -> request -> m MasterConnectInfo -> m response
    master' wp ss rid r getMci = do
      mh <- mkMasterHandle waMasterArgs
      mci <- getMci
      master (wpRedis wp) rid r mh
