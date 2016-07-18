{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE ViewPatterns #-}
{-|
Module: Distributed.RequestSlaves
Description: Facilities to connect worker nodes as slaves and masters.

The basic mechanism is as follows:

- Prospective masters broadcast that they accept connections via 'requestSlaves'.
- The requests can be acted upon by prospective slaves via 'withSlaveRequests'.

These functions are only converned with bringing masters and slaves
together, not with the actual communication between the nodes.

On top of that, 'acceptSlaveConnections' and 'connectToMaster' are
used to connect masters and slaves, and initiate communication between
them in the form of an 'NMApp'.  Slaves are distributed fairly between
the masters.
-}

module Distributed.RequestSlaves
    ( -- * Basic machinery
      WorkerConnectInfo(..)
    , requestSlaves
    , withSlaveRequests
    , withSlaveRequestsWait
      -- * Utilities to run a master that waits for slaves, and for slaves to connect to it
    , connectToMaster
    , connectToAMaster
    , acceptSlaveConnections
      -- * Exported for Testing/debugging
    , getWorkerRequests
    , workerRequestsKey
    ) where

import ClassyPrelude
import Control.Monad.Logger
import qualified Data.HashSet as HashSet
import Data.List.NonEmpty (NonEmpty(..))
import Data.Store (Store, encode)
import Distributed.Redis
import Distributed.Types (WorkerId(..), DistributedException(..))
import FP.Redis
import qualified Data.List.NonEmpty as NE
import Data.Streaming.NetworkMessage
import qualified Data.Conduit.Network as CN
import qualified Data.Streaming.Network.Internal as CN
import Control.Monad.Trans.Control (control)
import qualified Control.Concurrent.Mesosync.Lifted.Safe as Async
import Data.Void (absurd)
import Control.Exception (BlockedIndefinitelyOnMVar)

-- | Information for connecting to a worker
data WorkerConnectInfo = WorkerConnectInfo
    { wciHost :: !ByteString
    , wciPort :: !Int
    } deriving (Eq, Show, Ord, Generic, Typeable)
instance Store WorkerConnectInfo

data WorkerConnectInfoWithWorkerId = WorkerConnectInfoWithWorkerId
    { wciwwiWorkerId :: !WorkerId
    , wciwwiWci :: !WorkerConnectInfo
    } deriving (Eq, Show, Ord, Generic, Typeable)
instance Store WorkerConnectInfoWithWorkerId

-- | This allows a worker to become a master, and broadcast that it
-- wants slaves to connect to itself.
requestSlaves
    :: (MonadConnect m)
    => Redis
    -> WorkerId
    -> WorkerConnectInfo
    -- ^ This worker's connection information.
    -> (m () -> m a)
    -- ^ When a slave connects, this continuation will be called.  As soon as it terminates, the master will stop accepting slaves.
    -> m a
requestSlaves r wid wci0 cont = do
    let wci = WorkerConnectInfoWithWorkerId wid wci0
    let encoded = encode wci
    stoppedVar :: MVar Bool <- newMVar False
    let add = withMVar stoppedVar $ \stopped ->
            if stopped
                then $logInfo ("Trying to increase the slave count when worker " ++ tshow wid ++ " has already been removed, ignoring")
                else run_ r (zincrby (workerRequestsKey r) 1 encoded)
    let remove = modifyMVar_ stoppedVar $ \stopped ->
            if stopped
                then do
                    $logInfo ("Trying to remove " ++ tshow (wciwwiWorkerId wci) ++ " from worker requests, but it is already removed")
                    return stopped
                else do
                    removed <- run r (zrem (workerRequestsKey r) (encoded :| []))
                    if  | removed == 0 ->
                            $logWarn ("Got no removals when trying to remove " ++ tshow (wciwwiWorkerId wci) ++ " from worker requests. This can happen if the request has already been deleted due to a temporary heartbeat failure of the master.")
                        | removed == 1 ->
                            return ()
                        | True ->
                            throwIO (InternalConnectRequestException ("Got multiple removals when trying to remove " ++ tshow (wciwwiWorkerId wci) ++ " from worker requests."))
                    return True
    bracket
        (run_ r (zadd (workerRequestsKey r) ((0, encoded) :| [])))
        (\() -> remove)
        (\() -> sendNotify r (workerRequestsNotify r) >> cont add)

-- | React to requests for slaves.
withSlaveRequests ::
       (MonadConnect m)
    => Redis
    -> Milliseconds
    -- ^ Timeout.  'withSlaveRequests' will wait for a notification
    -- that slaves are requested.  Since notifications are not
    -- failsafe, it will run after this timeout if no notification was
    -- received.
    -> m [WorkerId]
    -- ^ Function that returns the 'WorkerId's of all workers that are
    -- alive.  Potential masters will be checked against this.  Any
    -- masters not in the list will be removed from
    -- 'workerRequestsKey'.
    -> (NonEmpty WorkerConnectInfo -> m ())
    -- ^ This function will be applied to the list of the masters that
    -- accept slaves (sorted by priority, so that the master with the
    -- fewest connected slaves comes first). Since it is not
    -- guaranteed that all the masters are still running, the list
    -- should be traversed until a running master is found.
    -> m void
withSlaveRequests redis failsafeTimeout getLiveWorkers f = withSlaveRequestsWait redis failsafeTimeout getLiveWorkers (return ()) f

-- | Exactly like 'withSlaveRequests', but allows to delay the loop with the third argument.
withSlaveRequestsWait ::
       (MonadConnect m)
    => Redis
    -> Milliseconds
    -- ^ Timeout.  'withSlaveRequestsWait' will wait for a notification
    -- that slaves are requested.  Since notifications are not
    -- failsafe, it will run after this timeout if no notification was
    -- received.
    -> m [WorkerId]
    -- ^ Function that indicates whether the worker with a given
    -- 'WorkerId' is still alive.  Potential masters will be checked
    -- against this.  Any masters that fail the test will be removed
    -- from 'workerRequestsKey'.
    -> (m ())
    -- ^ This function has a loop that keeps grabbing slave requests. This action
    -- will be called at the beginning of every loop iteration, and can
    -- be useful to "limit" the loop (e.g. have some timer in between,
    -- or wait for the worker to be free of processing requests if we
    -- are doing something else too).
    -> (NonEmpty WorkerConnectInfo -> m ())
    -- ^ This function will be applied to the list of the masters that
    -- accept slaves (sorted by priority, so that the master with the
    -- fewest connected slaves comes first). Since it is not
    -- guaranteed that all the masters are still running, the list
    -- should be traversed until a running master is found.
    -> m void
withSlaveRequestsWait redis failsafeTimeout getLiveWorkers wait f = do
    withSubscribedNotifyChannel (managedConnectInfo (redisConnection redis)) failsafeTimeout (workerRequestsNotify redis) $
        \waitNotification -> forever $ do
            waitNotification
            wait
            reqs <- getWorkerRequestsWithWorkerIds redis
            liveWorkers <- HashSet.fromList <$> getLiveWorkers
            let validReqs = filter (\x -> HashSet.member (wciwwiWorkerId x) liveWorkers) reqs
            case NE.nonEmpty validReqs of
                Nothing -> do
                    $logDebug ("Tried to get masters to connect to but got none")
                    return ()
                Just reqs' -> do
                    $logDebug ("Got " ++ tshow reqs' ++ " masters to try to connect to")
                    mbRes :: Either SomeException () <- tryAny (f (wciwwiWci <$> reqs'))
                    case mbRes of
                        Left err -> do
                            $logWarn ("withSlaveRequests: got error " ++ tshow err ++ ", continuing")
                        Right () -> return ()

-- | Get the list of all 'WorkerConnectInfo' of the masters that accept slave connections.
--
-- This list will be ordered, so that masters with fewer slaves are
-- listed first (to be precise, the number of times a slave connected
-- to a master are counted, and it is assumed that clients stay
-- connected to the master as long as the master runs, see issue #134).
getWorkerRequests :: (MonadConnect m) => Redis -> m [WorkerConnectInfo]
getWorkerRequests r = map wciwwiWci <$> getWorkerRequestsWithWorkerIds r

getWorkerRequestsWithWorkerIds :: (MonadConnect m) => Redis -> m [WorkerConnectInfoWithWorkerId]
getWorkerRequestsWithWorkerIds r = do
    resps <- run r (zrangebyscore (workerRequestsKey r) (-1/0) (1/0) False)
    mapM (decodeOrThrow "getWorkerRequests") resps

-- | Channel used for notifying that there's a new ConnectRequest.
workerRequestsNotify :: Redis -> NotifyChannel
workerRequestsNotify r = NotifyChannel $ Channel $ redisKeyPrefix r <> "connect-requests-notify"

-- | Key used for storing requests for workers.
workerRequestsKey :: Redis -> ZKey
workerRequestsKey r = ZKey $ Key $ redisKeyPrefix r <> "connect-requests"

-- * Utilities to run master/slaves
-----------------------------------------------------------------------

-- | Establish a connection to a master worker.
connectToAMaster :: forall m slaveSends masterSends.
       (MonadConnect m, Sendable slaveSends, Sendable masterSends)
    => NMApp slaveSends masterSends m ()
    -- ^ What to do when we connect. This continuation will be run at
    -- most once, but it could be not run if we couldn't connect to any
    -- master.
    -> NonEmpty WorkerConnectInfo
    -- ^ List of masters.  The first one that currently accepts slave
    -- connections will be used.
    -> m ()
connectToAMaster cont0 wcis0 = do
    nmSettings <- defaultNMSettings
    go nmSettings (toList wcis0) []
  where
    cont wci nm = do
        $logDebug ("Managed to connect to master " ++ tshow wci)
        cont0 nm

    go :: NMSettings -> [WorkerConnectInfo] -> [SomeException] -> m ()
    go nmSettings wcis_ excs = case wcis_ of
        [] -> do
            $logWarn ("Could not connect to any of the masters, because of exceptions " ++ tshow excs ++ ". This is probably OK, will give up as slave.")
            return ()
        wci@(WorkerConnectInfo host port) : wcis -> do
            mbExc :: Either SomeException (Either SomeException ()) <-
                tryAny $ control $ \invert -> CN.runTCPClient (CN.clientSettings port host) $ \ad ->
                    invert $
                        runNMApp nmSettings (\nm -> (try (cont wci nm) :: m (Either SomeException ()))) ad
            case mbExc of
                Right (Left err) -> throwIO err
                Right (Right ()) -> return ()
                Left err -> if acceptableException err
                    then do
                        $logInfo ("Could not connect to master " ++ tshow wci ++ ", because of acceptable exception " ++ tshow err ++ ", continuing")
                        go nmSettings wcis (err : excs)
                    else throwIO err

-- | Runs a slave that runs an action when it connects to master
--
-- Slaves that connect via 'connectToMaster' will be distributed
-- fairly to all masters, so that the master that had the fewest
-- slaves connect to it so far will get the next slave.
--
-- Note that it is assumed that slaves stay connected to a master as
-- long as that master is running; there is not yet a means to
-- explicitly disconnect a client from a master and prioritize that
-- master for new clients (see issue #134).
connectToMaster :: forall m slaveSends masterSends void.
       (MonadConnect m, Sendable slaveSends, Sendable masterSends)
    => Redis
    -> Milliseconds
    -- ^ Timeout as in 'withSlaveRequests'
    -> m [WorkerId]
    -> NMApp slaveSends masterSends m () -- ^ What to do when we connect
    -> m void
connectToMaster r failsafeTimeout getLiveWorkers cont = withSlaveRequests r failsafeTimeout getLiveWorkers (connectToAMaster cont)

-- | Exceptions that we anticipate when trying to connect.
acceptableException :: SomeException -> Bool
acceptableException err
    | Just (_ :: IOError) <- fromException err = True
    | Just (_ :: NetworkMessageException) <- fromException err = True
    | otherwise = False

-- | Run a master that listens to slaves, and request slaves to connect.
acceptSlaveConnections :: forall m masterSends slaveSends a.
       (MonadConnect m, Sendable masterSends, Sendable slaveSends)
    => Redis
    -> WorkerId
    -> CN.ServerSettings
    -- ^ The settings used to create the server. You can use 0 as port number to have
    -- it automatically assigned
    -> ByteString
    -- ^ The host that will be used by the slaves to connect
    -> Maybe Int
    -- ^ The port that will be used by the slaves to connect.
    -- If Nothing, the port the server is locally bound to will be used
    -> NMApp masterSends slaveSends m ()
    -- ^ What to do when a slave gets added
    -> m a
    -- ^ Continuation, the master will quit when requesting slaves when this
    -- continuation exits
    -> m a
acceptSlaveConnections r wid ss0 host mbPort contSlaveConnect cont = do
    nmSettings <- defaultNMSettings
    (ss, getPort) <- liftIO (getPortAfterBind ss0)
    whenSlaveConnectsVar :: MVar (m ()) <- newEmptyMVar
    let acceptConns =
            CN.runGeneralTCPServer ss $ \ad -> do
                let whenSlaveConnects nm = do
                        join (readMVar whenSlaveConnectsVar)
                        contSlaveConnect nm
                -- This is just to get the exceptions in the logs rather than on the
                -- terminal: this is run in a separate thread anyway, and so they'd be
                -- lost forever otherwise.
                -- In other words, the semantics of the program are not affected.
                mbExc :: Either SomeException () <- tryAny (runNMApp nmSettings whenSlaveConnects ad)
                -- We check for this because of the 'readMVar' above -- if the main thread
                -- is killed, we might get this.
                let blockedOnMVar :: SomeException -> Bool
                    blockedOnMVar exc = case fromException exc of
                        Just (_ :: BlockedIndefinitelyOnMVar) -> True
                        Nothing -> False
                case mbExc of
                    Left err -> if acceptableException err || blockedOnMVar err
                        then $logWarn ("acceptSlaveConnections: got IOError or NetworkMessageException, this can happen if the slave dies " ++ tshow err)
                        else $logError ("acceptSlaveConnections: got unexpected exception" ++ tshow err)
                    Right () -> return ()
    let runMaster = do
            -- This is used to get the port if we need it, but also to wait
            -- for the server to be up.
            port <- liftIO getPort
            $logDebug ("Master starting on " ++ tshow (CN.serverHost ss, port))
            let wci = WorkerConnectInfo host (fromMaybe port mbPort)
            requestSlaves r wid wci $ \wsc -> do
                putMVar whenSlaveConnectsVar wsc
                cont
    fmap (either absurd id) (Async.race acceptConns runMaster)
