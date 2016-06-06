{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE MultiWayIf #-}

module Distributed.RequestSlaves
    ( -- * Basic machinery
      WorkerConnectInfo(..)
    , requestSlaves
    , withSlaveRequests
      -- * Utilities to run a master that waits for slaves, and for slaves to connect to it
    , connectToMaster
    , connectToAMaster
    , acceptSlaveConnections
      -- * Testing/debugging
    , getWorkerRequests
    ) where

import ClassyPrelude
import Control.Monad.Logger
import Data.List.NonEmpty (NonEmpty(..))
import Data.Store (encode)
import Distributed.Redis
import Distributed.Types (WorkerId(..), DistributedException(..))
import FP.Redis
import Data.Store (Store)
import qualified Data.List.NonEmpty as NE
import Data.Streaming.NetworkMessage
import qualified Data.Conduit.Network as CN
import qualified Data.Streaming.Network.Internal as CN
import Control.Monad.Trans.Control (control)
import qualified Control.Concurrent.Async.Lifted.Safe as Async
import Data.Void (absurd)
import qualified Data.UUID as UUID
import qualified Data.UUID.V4 as UUID
import Control.Exception (BlockedIndefinitelyOnMVar)

-- * Information for connecting to a worker

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

-- | This command is used by a worker to request that another worker
-- connects to it.
--
-- This currently has the following caveats:
--
--   (1) The worker request is not guaranteed to be fulfilled, for
--   multiple reasons:
--
--       - All workers may be busy doing other work.
--
--       - The queue of worker requests might already be long.
--
--       - A worker might pop the worker request and then shut down
--       before establishing a connection.
--
--   (2) A node may get workers connecting to it that it didn't
--   request.  Here's why:
--
--       - When a worker stops being a master, it does not remove its
--       pending worker requests from the list.  This means they can
--       still be popped by workers.  Usually this means that the
--       worker will attempt to connect, fail, and find something else
--       to do.  However, in the case that the server becomes a master
--       again, it's possible that a worker will pop its request.
--
-- These caveats are not necessitated by any aspect of the overall
-- design, and may be resolved in the future.
requestSlaves
    :: (MonadConnect m)
    => Redis
    -> WorkerConnectInfo
    -> (m () -> m a)
    -> m a
requestSlaves r wci0 cont = do
    wid <- getWorkerId
    let wci = WorkerConnectInfoWithWorkerId wid wci0
    let encoded = encode wci
    stoppedVar :: MVar Bool <- newMVar False
    let add = withMVar stoppedVar $ \stopped ->
            if stopped
                then $logInfo ("Trying to increase the slave count when worker " ++ tshow wid ++ " has already been removed, ignoring")
                else run_ r (zincrby (workerRequestsKey r) 1 encoded)
    -- REVIEW TODO There is a slight chance that this fails, in which case
    -- a stray WorkerConnectInfo remains in the workerRequestsKey forever.
    -- Is this a big problem? Can we mitigate against this?
    let remove = modifyMVar_ stoppedVar $ \stopped -> do
            if stopped
                then do
                    $logInfo ("Trying to remove " ++ tshow (wciwwiWorkerId wci) ++ " from worker requests, but it is already removed")
                    return stopped
                else do
                    removed <- run r (zrem (workerRequestsKey r) (encoded :| []))
                    if  | removed == 0 ->
                            throwIO (InternalConnectRequestException ("Got no removals when trying to remove " ++ tshow (wciwwiWorkerId wci) ++ " from worker requests. This can happen if the removal function is called twice"))
                        | removed == 1 ->
                            return ()
                        | True -> do
                            throwIO (InternalConnectRequestException ("Got multiple removals when trying to remove " ++ tshow (wciwwiWorkerId wci) ++ " from worker requests."))
                    return True
    bracket
        (run_ r (zadd (workerRequestsKey r) ((0, encoded) :| [])))
        (\() -> remove)
        (\() -> sendNotify r (workerRequestsNotify r) >> cont add)

withSlaveRequests ::
       MonadConnect m
    => Redis
    -> Milliseconds
    -> (NonEmpty WorkerConnectInfo -> m ())
    -- A list of masters to connect to, sorted by preference. Note that
    -- it might be the case that masters are already down. The intended
    -- use of this list is that you keep traversing it in order until you
    -- find a master to connect to.
    -> m void
withSlaveRequests redis failsafeTimeout f = do
    withSubscribedNotifyChannel (managedConnectInfo (redisConnection redis)) failsafeTimeout (workerRequestsNotify redis) $
        \waitNotification -> forever $ do
            waitNotification
            reqs <- getWorkerRequests redis
            case NE.nonEmpty reqs of
                Nothing -> do
                    $logDebug ("Tried to get masters to connect to but got none")
                    return ()
                Just reqs' -> do
                    $logDebug ("Got " ++ tshow reqs' ++ " masters to try to connect to")
                    mbRes :: Either SomeException () <- tryAny (f reqs')
                    case mbRes of
                        Left err -> do
                            $logWarn ("withSlaveRequests: got error " ++ tshow err ++ ", continuing")
                        Right () -> return ()

getWorkerRequests :: (MonadConnect m) => Redis -> m [WorkerConnectInfo]
getWorkerRequests r = do
    resps <- run r (zrangebyscore (workerRequestsKey r) (-1/0) (1/0) False)
    map wciwwiWci <$> mapM (decodeOrThrow "getWorkerRequests") resps

-- | Channel used for notifying that there's a new ConnectRequest.
workerRequestsNotify :: Redis -> NotifyChannel
workerRequestsNotify r = NotifyChannel $ Channel $ redisKeyPrefix r <> "connect-requests-notify"

-- | Key used for storing requests for workers.
workerRequestsKey :: Redis -> ZKey
workerRequestsKey r = ZKey $ Key $ redisKeyPrefix r <> "connect-requests"

-- * Utilities to run master/slaves
-----------------------------------------------------------------------

connectToAMaster :: forall m slaveSends masterSends.
       (MonadConnect m, Sendable slaveSends, Sendable masterSends)
    => NMApp slaveSends masterSends m ()
    -- ^ What to do when we connect. This continuation will be run at
    -- most once, but it could be not run if we couldn't connect to any
    -- master.
    -> NonEmpty WorkerConnectInfo
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
connectToMaster :: forall m slaveSends masterSends void.
       (MonadConnect m, Sendable slaveSends, Sendable masterSends)
    => Redis
    -> Milliseconds
    -> NMApp slaveSends masterSends m () -- ^ What to do when we connect
    -> m void
connectToMaster r failsafeTimeout cont = withSlaveRequests r failsafeTimeout (connectToAMaster cont)

acceptableException :: SomeException -> Bool
acceptableException err
    | Just (_ :: IOError) <- fromException err = True
    | Just (_ :: NetworkMessageException) <- fromException err = True
    | True = False

getWorkerId :: (MonadIO m) => m WorkerId
getWorkerId = do
    liftIO (WorkerId . UUID.toASCIIBytes <$> UUID.nextRandom)

acceptSlaveConnections :: forall m masterSends slaveSends a.
       (MonadConnect m, Sendable masterSends, Sendable slaveSends)
    => Redis
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
acceptSlaveConnections r ss0 host mbPort contSlaveConnect cont = do
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
            requestSlaves r wci $ \wsc -> do
                putMVar whenSlaveConnectsVar wsc
                cont
    fmap (either absurd id) (Async.race acceptConns runMaster)
