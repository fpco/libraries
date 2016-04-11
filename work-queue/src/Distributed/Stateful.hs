{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE OverloadedStrings #-}
module Distributed.Stateful
    ( -- * 'MasterHandle' operations
      MasterHandle
    , StateId
    , update
    , resetStates
    , getStateIds
    , getStates
    , getSlaveCount
    , SlaveNMAppData
    , addSlaveConnection
    -- * Running the slave
    , SlaveArgs(..)
    , statefulSlave
    , S.runSlave
    -- * Running the master
    , MasterArgs(..)
    , StatefulMasterArgs(..)
    , MasterException(..)
    , mkMasterHandle
    , statefulMaster
    ) where

import           ClassyPrelude
import           Control.Concurrent.Async (withAsync, concurrently)
import           Control.Concurrent.STM (check)
import           Control.DeepSeq (NFData)
import           Control.Monad.Logger
import qualified Data.Conduit.Network as CN
import           Data.Streaming.NetworkMessage
import           Distributed.ConnectRequest
import           Distributed.JobQueue.Worker
import           Distributed.Redis
import           Distributed.Stateful.Master
import           Distributed.Stateful.Slave (SlaveArgs(..))
import qualified Distributed.Stateful.Slave as S
import           Distributed.Types
import           FP.Redis
import           System.Timeout (timeout)

-- * Running stateful master / slave without job-queue

statefulSlave
    :: forall state context input output.
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
    -> RequestId
    -> (MasterHandle state context input output -> IO response)
    -> IO response
statefulMaster StatefulMasterArgs{..} rid master = do
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
              runLogger $ logWarnN "Timed out waiting for slaves to connect"
              cancelWork rid
            _ -> liftIO $ master mh
    let requestSlaves = do
            port <- liftIO getPort
            let wci = WorkerConnectInfo smaHostName port
            withRedis smaRedisConfig $ \redis ->
                mapM_ (\_ -> requestWorker redis wci) [1..smaRequestSlaveCount]
    liftIO $ withAsync (runLogger requestSlaves `concurrently` acceptConns) $ \_ ->
        runMaster' `finally` putMVar doneVar ()
