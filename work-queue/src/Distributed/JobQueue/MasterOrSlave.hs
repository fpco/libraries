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
    , connectToMaster
    , acceptSlaveConnections
    ) where

import ClassyPrelude
import Control.Monad.Logger
import Data.Streaming.NetworkMessage
import Distributed.JobQueue.Internal
import Distributed.Redis
import Distributed.Types
import FP.Redis
import qualified Control.Concurrent.Async.Lifted.Safe as Async
import Distributed.ConnectRequest
import Data.Void (absurd)
import Distributed.JobQueue.Worker
import Data.List.NonEmpty (NonEmpty)
import Control.Monad.Trans.Control (control)
import qualified Data.Conduit.Network as CN

data MasterOrSlave = Idle | Slave | Master
    deriving (Eq, Ord, Show)

connectToMaster :: forall m slaveSends masterSends a.
       (MonadConnect m, Sendable slaveSends, Sendable masterSends)
    => NMSettings -> NonEmpty WorkerConnectInfo -> NMApp slaveSends masterSends m a
    -> m (Maybe a)
connectToMaster nmSettings wcis0 cont = go (toList wcis0) []
  where
    go :: [WorkerConnectInfo] -> [SomeException] -> m (Maybe a)
    go wcis_ excs = case wcis_ of
        [] -> do
            $logWarn ("Could not connect to any of the masters (" ++ tshow wcis0 ++ "), because of exceptions " ++ tshow excs ++ ". This is probably OK, will give up as slave.")
            return Nothing
        wci@(WorkerConnectInfo host port) : wcis -> do
            mbExc :: Either SomeException (Either SomeException a) <-
                try $ control $ \invert -> CN.runTCPClient (CN.clientSettings port host) $
                    runNMApp nmSettings $ \nm -> invert (try (cont nm) :: m (Either SomeException a))
            case mbExc of
                Right (Left err) -> throwIO err
                Right (Right x) -> return (Just x)
                Left err -> if acceptableException err
                    then do
                        $logInfo ("Could not connect to master " ++ tshow wci ++ ", because of acceptable exception " ++ tshow err ++ ", continuing")
                        go wcis (err : excs)
                    else throwIO err

    acceptableException :: SomeException -> Bool
    acceptableException err
        | Just (_ :: IOError) <- fromException err = True
        | Just (_ :: NetworkMessageException) <- fromException err = True
        | True = False

acceptSlaveConnections ::
       (MonadConnect m, Sendable masterSends, Sendable slaveSends)
    => NMSettings
    -> ByteString -- ^ Hostname that will be used in the 'WorkerConnectInfo'
    -> NMApp masterSends slaveSends m () -- ^ What to do when a slave gets added
    -> (WorkerConnectInfo -> m a)
    -- ^ Continuation with the connect info the master is listening on
    -> m a
acceptSlaveConnections nmSettings host contSlaveConnect cont = do
    (ss, getPort) <- liftIO (getPortAfterBind (CN.serverSettings 0 "*"))
    doneVar <- newEmptyMVar
    let acceptConns =
            CN.runGeneralTCPServer ss $ runNMApp nmSettings $ \nm -> do
                contSlaveConnect nm
                readMVar doneVar
    let runMaster = do
            port <- liftIO getPort
            cont (WorkerConnectInfo host port)
    fmap (either absurd id) (Async.race acceptConns (finally runMaster (putMVar doneVar ())))

-- REVIEW: To request slaves, there is a separate queue from normal requests, the
-- reason being that we want to prioritize slave requests over normal requests.
runMasterOrSlave :: forall m request response.
       (MonadConnect m, Sendable request, Sendable response)
    => JobQueueConfig
    -> (Redis -> NonEmpty WorkerConnectInfo -> m ())
    -- ^ Slave function. The slave function should try to connect to the master
    -- with 'connectToMaster', which will do the right thing with the list of
    -- candidate masters.
    -> (Redis -> RequestId -> request -> m (Either CancelOrReenqueue response))
    -- ^ Master function
    -> m ()
runMasterOrSlave config slaveFunc masterFunc = do
    stateVar <- liftIO (newTVarIO Idle)
    fmap (either absurd absurd) $ Async.race
        (handleWorkerRequests stateVar) (handleRequests stateVar)
  where
    handleWorkerRequests :: TVar MasterOrSlave -> m void
    handleWorkerRequests stateVar =
        withRedis (jqcRedisConfig config) $ \redis ->
            withConnectRequests redis $ \wcis -> do
                -- If it can't transition to slave, that's fine: all the
                -- other slave candidates will get the connection request anyway.
                -- In fact, this note itself will get it again, since
                -- 'withSubscribedNotifyChannel' gets every request every
                -- 100 ms.
                mb <- transitionIdleTo stateVar Slave (slaveFunc redis wcis)
                case mb of
                    Nothing -> $logDebug ("Tried to transition to slave, but couldn't. Will not run slave function with connections " ++ tshow wcis)
                    Just () -> return ()

    handleRequests :: TVar MasterOrSlave -> m void
    handleRequests stateVar =
        jobWorker config $ \redis rid request -> do
            -- If you couldn't transition to master, re-enqueue.
            mbRes <- transitionIdleTo stateVar Master $ do
                masterFunc redis rid request
            case mbRes of
                Nothing -> do
                    $logDebug ("Tried to transition to master, but couldn't. Request " ++ tshow rid ++ " will be re-enqueued.")
                    return (Left Reenqueue)
                Just res -> return res

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
