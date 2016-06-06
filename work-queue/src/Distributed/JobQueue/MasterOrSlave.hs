{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE MultiWayIf #-}

module Distributed.JobQueue.MasterOrSlave
    ( runMasterOrSlave
    ) where

import ClassyPrelude
import Control.Monad.Logger
import Data.Streaming.NetworkMessage
import Distributed.JobQueue.Internal
import Distributed.Redis
import Distributed.Types
import FP.Redis
import qualified Control.Concurrent.Async.Lifted.Safe as Async
import Distributed.RequestSlaves
import Distributed.JobQueue.Worker
import Data.List.NonEmpty (NonEmpty)

data MasterOrSlave = Idle | Slave | Master
    deriving (Eq, Ord, Show)

-- REVIEW: To request slaves, there is a separate queue from normal requests, the
-- reason being that we want to prioritize slave requests over normal requests.
runMasterOrSlave :: forall m request response void.
       (MonadConnect m, Sendable request, Sendable response)
    => JobQueueConfig
    -> (Redis -> NonEmpty WorkerConnectInfo -> m ())
    -- ^ Slave function. The slave function should try to connect to the master
    -- with 'connectToMaster', which will do the right thing with the list of
    -- candidate masters.
    -> (Redis -> RequestId -> request -> m (Reenqueue response))
    -- ^ Master function.
    -> m void
runMasterOrSlave config slaveFunc masterFunc = do
    stateVar <- liftIO (newTVarIO Idle)
    fmap (either id id) $ Async.race
        (handleSlaveRequests stateVar) (handleMasterRequests stateVar)
  where
    handleSlaveRequests :: TVar MasterOrSlave -> m void
    handleSlaveRequests stateVar =
        withRedis (jqcRedisConfig config) $ \redis ->
            withSlaveRequests redis (jqcSlaveRequestsNotificationFailsafeTimeout config) $ \wcis -> do
                -- If it can't transition to slave, that's fine: all the
                -- other slave candidates will get the connection request anyway.
                -- In fact, this node itself will get it again, since
                -- 'withSubscribedNotifyChannel' gets every request every
                -- 100 ms.
                mb <- transitionIdleTo stateVar Slave $ do
                    $logInfo "Transitioned to slave"
                    slaveFunc redis wcis
                case mb of
                    Nothing -> $logDebug ("Tried to transition to slave, but couldn't. Will not run slave function with connections " ++ tshow wcis)
                    Just () -> return ()

    handleMasterRequests :: TVar MasterOrSlave -> m void
    handleMasterRequests stateVar =
        jobWorker config $ \redis rid request -> do
            -- If you couldn't transition to master, re-enqueue.
            mbRes <- transitionIdleTo stateVar Master $ do
                $logInfo ("Transitioned to master")
                masterFunc redis rid request
            case mbRes of
                Nothing -> do
                    $logDebug ("Tried to transition to master, but couldn't. Request " ++ tshow rid ++ " will be re-enqueued.")
                    return Reenqueue
                Just res -> do
                    return res

    transitionIdleTo :: TVar MasterOrSlave -> MasterOrSlave -> m a -> m (Maybe a)
    transitionIdleTo stateVar state' cont = bracket
        (atomically $ do
            state <- readTVar stateVar
            if state == Idle
                then do
                    writeTVar stateVar state'
                    return True
                else return False)
        (\changed -> when changed (atomically (writeTVar stateVar Idle)))
        (\changed -> if changed then Just <$> cont else return Nothing)