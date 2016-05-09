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
import qualified Control.Concurrent.Async.Lifted.Safe as Async
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
import           Data.Void (absurd)

-- * Running stateful master / slave without job-queue

statefulSlave
    :: forall state context input output m.
     ( NFData state, NFData output, MonadConnect m
     , Sendable state, Sendable context, Sendable input, Sendable output
     )
    => NMSettings
    -> RedisConfig
    -> (context -> input -> state -> m (state, output))
    -> m ()
statefulSlave nms redisConfig slave =
    withRedis redisConfig $ \r ->
        withConnectRequests r $ \wci ->
            S.runSlave SlaveArgs
                { saUpdate = slave
                , saConnectInfo = wci
                , saNMSettings = nms
                }

data StatefulMasterArgs = StatefulMasterArgs
  { smaMasterArgs :: MasterArgs
  , smaRequestSlaveCount :: Int
    -- ^ How many slaves to request. Note that this is not a guaranteed
    -- number of slaves, just a suggestion.
  , smaMasterWaitTime :: Seconds
    -- ^ How long to wait for slaves to connect (if no slaves connect in
    -- this time, then the work gets aborted). If all the requested
    -- slaves connect, then waiting is aborted.
  , smaRedisConfig :: RedisConfig
  , smaHostName :: ByteString
  , smaNMSettings :: NMSettings
  }

statefulMaster
    :: forall state context input output response m.
     ( NFData state, NFData output, MonadConnect m
     , Sendable state, Sendable context, Sendable input, Sendable output
     )
    => StatefulMasterArgs
    -> (MasterHandle state context input output -> m (Either CancelOrReenqueue response))
    -> m (Either CancelOrReenqueue response)
statefulMaster StatefulMasterArgs{..} master = do
    (ss, getPort) <- liftIO (getPortAfterBind (CN.serverSettings 0 "*"))
    mh <- mkMasterHandle smaMasterArgs
    doneVar <- newEmptyMVar
    let acceptConns =
          CN.runGeneralTCPServer ss $ runNMApp smaNMSettings $ \nm -> do
            addSlaveConnection mh nm
            readMVar doneVar
    let runMaster' = do
          res <- liftIO $ timeout (1000 * 1000 * fromIntegral (unSeconds smaMasterWaitTime)) $
            atomically (check . (== smaRequestSlaveCount) =<< getSlaveCount mh)
          slaveCount <- atomically (getSlaveCount mh)
          case (res, slaveCount) of
            (Nothing, 0) -> do
              logWarnN "Timed out waiting for slaves to connect"
              return (Left Cancel)
            _ -> master mh
    let requestSlaves = do
            port <- liftIO getPort
            let wci = WorkerConnectInfo smaHostName port
            withRedis smaRedisConfig $ \redis ->
                mapM_ (\_ -> requestWorker redis wci) [1..smaRequestSlaveCount]
    fmap (either (absurd . snd) id) $ Async.race
      (Async.concurrently requestSlaves acceptConns)
      (runMaster' `finally` putMVar doneVar ())
