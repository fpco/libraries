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
    , acceptSlaveConnections
    ) where

import ClassyPrelude
import Control.Monad.Logger
import Data.List.NonEmpty (NonEmpty(..))
import Data.Serialize (encode)
import Distributed.Redis
import Distributed.Types (WorkerId(..), DistributedException(..))
import FP.Redis
import Data.Serialize (Serialize)
import qualified Data.List.NonEmpty as NE
import Data.Streaming.NetworkMessage
import qualified Data.Conduit.Network as CN
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
instance Serialize WorkerConnectInfo

data WorkerConnectInfoWithWorkerId = WorkerConnectInfoWithWorkerId
    { wciwwiWorkerId :: !WorkerId
    , wciwwiWci :: !WorkerConnectInfo
    } deriving (Eq, Show, Ord, Generic, Typeable)
instance Serialize WorkerConnectInfoWithWorkerId

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
    -> WorkerId
    -> WorkerConnectInfo
    -> (m () -> m a)
    -- ^ The action must be called when a slave successfully connects.
    -> m a
requestSlaves r wid wci0 cont = do
    let wci = WorkerConnectInfoWithWorkerId wid wci0
    let encoded = encode wci
    let add = run_ r (zincrby (workerRequestsKey r) 1 encoded)
    -- REVIEW TODO There is a slight chance that this fails, in which case
    -- a stray WorkerConnectInfo remains in the workerRequestsKey forever.
    -- Is this a big problem? Can we mitigate against this?
    let remove = do
            removed <- run r (zrem (workerRequestsKey r) (encoded :| []))
            if  | removed == 0 ->
                    throwIO (InternalConnectRequestException ("Got no removals when trying to remove " ++ tshow (wciwwiWorkerId wci) ++ " from worker requests."))
                | removed == 1 ->
                    return ()
                | True ->
                    throwIO (InternalConnectRequestException ("Got multiple removals when trying to remove " ++ tshow (wciwwiWorkerId wci) ++ " from worker requests."))
    bracket
        (run_ r (zadd (workerRequestsKey r) ((0, encoded) :| [])))
        (\() -> remove)
        (\() -> sendNotify r (workerRequestsNotify r) >> cont add)

withSlaveRequests ::
       MonadConnect m
    => Redis
    -> (NonEmpty WorkerConnectInfo -> m ())
    -- A list of masters to connect to, sorted by preference. Note that
    -- it might be the case that masters are already down. The intended
    -- use of this list is that you keep traversing it in order until you
    -- find a master to connect to.
    -> m void
withSlaveRequests redis f = do
    withSubscribedNotifyChannel (managedConnectInfo (redisConnection redis)) (Milliseconds 100) (workerRequestsNotify redis) $
        \waitNotification -> forever $ do
            waitNotification
            reqs <- getWorkerRequests redis
            case NE.nonEmpty reqs of
                Nothing -> do
                    $logDebug ("Tried to got masters to connect to but got none")
                    return ()
                Just reqs' -> do
                    $logDebug ("Got " ++ tshow reqs' ++ " masters to try to connect to")
                    mbRes :: Either SomeException () <- try (f reqs')
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

-- | Runs a slave that runs an action when it connects to master
connectToMaster :: forall m slaveSends masterSends void.
       (MonadConnect m, Sendable slaveSends, Sendable masterSends)
    => Redis
    -> NMSettings
    -> NMApp slaveSends masterSends m () -- ^ What to do when we connect
    -> m void
connectToMaster r nmSettings cont0 = withSlaveRequests r (\wcis -> go (toList wcis) [])
  where
    cont wci nm = do
        $logDebug ("Managed to connect to master " ++ tshow wci)
        cont0 nm

    go :: [WorkerConnectInfo] -> [SomeException] -> m ()
    go wcis_ excs = case wcis_ of
        [] -> do
            $logWarn ("Could not connect to any of the masters, because of exceptions " ++ tshow excs ++ ". This is probably OK, will give up as slave.")
            return ()
        wci@(WorkerConnectInfo host port) : wcis -> do
            mbExc :: Either SomeException (Either SomeException ()) <-
                try $ control $ \invert -> CN.runTCPClient (CN.clientSettings port host) $ \ad ->
                    invert $
                        runNMApp nmSettings (\nm -> (try (cont wci nm) :: m (Either SomeException ()))) ad
            case mbExc of
                Right (Left err) -> throwIO err
                Right (Right ()) -> return ()
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

getWorkerId :: (MonadIO m) => m WorkerId
getWorkerId = do
    liftIO (WorkerId . UUID.toASCIIBytes <$> UUID.nextRandom)

acceptSlaveConnections :: forall m masterSends slaveSends a.
       (MonadConnect m, Sendable masterSends, Sendable slaveSends)
    => Redis
    -> NMSettings
    -> ByteString -- ^ Hostname that will be used in the 'WorkerConnectInfo'
    -> NMApp masterSends slaveSends m () -- ^ What to do when a slave gets added
    -> (WorkerConnectInfo -> m a)
    -- ^ Continuation with the connect info the master is listening on
    -> m a
acceptSlaveConnections r nmSettings host contSlaveConnect cont = do
    wid <- getWorkerId
    (ss, getPort) <- liftIO (getPortAfterBind (CN.serverSettings 0 "*"))
    whenSlaveConnectsVar :: MVar (m ()) <- newEmptyMVar
    let acceptConns =
            CN.runGeneralTCPServer ss $ \ad -> do
                -- This is just to get the exceptions in the logs rather than on the
                -- terminal: this is run in a separate thread anyway, and so they'd be
                -- lost forever otherwise.
                -- In other words, the semantics of the program are not affected.
                let whenSlaveConnects nm = do
                        join (readMVar whenSlaveConnectsVar)
                        contSlaveConnect nm
                mbExc :: Either SomeException () <- try (runNMApp nmSettings whenSlaveConnects ad)
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
            port <- liftIO getPort
            $logDebug ("Master starting on " ++ tshow (host, port))
            let wci = WorkerConnectInfo host port
            requestSlaves r wid wci $ \wsc -> do
                putMVar whenSlaveConnectsVar wsc
                cont wci
    fmap (either absurd id) (Async.race acceptConns runMaster)
