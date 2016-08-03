{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE MultiWayIf #-}
{-|
Module: Distributed.JobQueue.MasterOrSlave
Description: Spawn nodes that act as masters or slaves, as needed.
-}
module Distributed.JobQueue.MasterOrSlave
    ( runMasterOrSlave
    ) where

import ClassyPrelude
import Control.Monad.Logger.JSON.Extra
import Data.Streaming.NetworkMessage
import Distributed.Heartbeat
import Distributed.JobQueue.Internal
import Distributed.Redis
import Distributed.Types
import FP.Redis
import qualified Control.Concurrent.Mesosync.Lifted.Safe as Async
import Distributed.RequestSlaves
import Distributed.JobQueue.Worker
import Data.List.NonEmpty (NonEmpty)
import qualified Control.Concurrent.STM as STM

data MasterOrSlave = Idle | Slave | Master
    deriving (Eq, Ord, Show)

-- REVIEW: To request slaves, there is a separate queue from normal requests, the
-- reason being that we want to prioritize slave requests over normal requests.

-- | Watch both requests for workers and slaves, and perform a request
-- whenever currently idle.
--
-- This function will concurrently watch for requests for master and
-- slave nodes.  It will optimistically try to perform each request
-- sent.  If it is currently busy, it will abandon and reschedule the
-- new request again.
runMasterOrSlave :: forall m request response void.
       (MonadConnect m, Sendable request, Sendable response)
    => JobQueueConfig
    -> Redis
    -> Heartbeating
    -> (NonEmpty WorkerConnectInfo -> m ())
    -- ^ Slave function. The slave function should try to connect to the master
    -- with 'connectToAMaster', which will do the right thing with the list of
    -- candidate masters.
    -> (RequestId -> request -> m (Reenqueue response))
    -- ^ Master function.
    -> m void
runMasterOrSlave config redis hb slaveFunc masterFunc = do
    stateVar <- liftIO (newTVarIO Idle)
    fmap (either id id) $ Async.race
        (handleSlaveRequests stateVar) (handleMasterRequests stateVar)
  where
    handleSlaveRequests :: TVar MasterOrSlave -> m void
    handleSlaveRequests stateVar =
        withSlaveRequestsWait redis (jqcSlaveRequestsNotificationFailsafeTimeout config) (activeOrUnhandledWorkers redis) (waitToBeIdle stateVar) $ \wcis -> do
            -- If it can't transition to slave, that's fine: all the
            -- other slave candidates will get the connection request anyway.
            -- In fact, this node itself will get it again, since
            -- 'withSubscribedNotifyChannel' gets every request at a
            -- timeout.
            mb <- transitionIdleTo stateVar Slave $ do
                $logInfoJ ("Transitioned to slave" :: String)
                slaveFunc wcis
            case mb of
                Nothing -> $logDebugJ ("Tried to transition to slave, but couldn't. Will not run slave function with connections " ++ tshow wcis)
                Just () -> return ()

    handleMasterRequests :: TVar MasterOrSlave -> m void
    handleMasterRequests stateVar = do
        jobWorkerWait config redis hb (waitToBeIdle stateVar) $ \rid request -> do
            -- If you couldn't transition to master, re-enqueue.
            mbRes <- transitionIdleTo stateVar Master $ do
                $logInfoJ ("Transitioned to master" :: String)
                masterFunc rid request
            case mbRes of
                Nothing -> do
                    $logDebugJ ("Tried to transition to master, but couldn't. Request " ++ tshow rid ++ " will be re-enqueued.")
                    return Reenqueue
                Just res -> do
                    return res

    -- Wait for the node to not be idle first. This is just so that we do not
    -- try too hard in getting requests or slave requests, but does not
    -- change the semantics of this function.. In other words, everything should
    -- still work if we have @waitToBeIdle _ = return ()@.
    waitToBeIdle :: TVar MasterOrSlave -> m ()
    waitToBeIdle stateVar = atomically $ do
        state <- readTVar stateVar
        unless (state == Idle) STM.retry

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
