{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE NoImplicitPrelude #-}

module Distributed.ConnectRequest
    ( WorkerConnectInfo(..)
    , requestWorkers
    , withConnectRequests
    ) where

import ClassyPrelude
import Data.List.NonEmpty (NonEmpty(..))
import Data.Serialize (encode)
import Distributed.Redis
import Distributed.Types (WorkerId, DistributedException(..))
import FP.Redis
import Data.Serialize (Serialize)
import qualified Data.List.NonEmpty as NE

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
requestWorkers
    :: (MonadConnect m)
    => Redis
    -> WorkerId
    -> WorkerConnectInfo
    -> (m () -> m a)
    -- ^ The action must be called when a slave successfully connects.
    -> m a
requestWorkers r wid wci0 cont = do
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

withConnectRequests ::
       MonadConnect m
    => Redis
    -> (NonEmpty WorkerConnectInfo -> m ())
    -- A list of workers to connect to, sorted by preference. Note that
    -- it might be the case that workers are already down. The intended
    -- use of this list is that you keep traversing it in order until you
    -- find a worker to connect to.
    -> m void
withConnectRequests redis f = do
    withSubscribedNotifyChannel (managedConnectInfo (redisConnection redis)) (Milliseconds 100) (workerRequestsNotify redis) $
        \waitNotification -> forever $ do
            waitNotification
            reqs <- getWorkerRequests redis
            case NE.nonEmpty reqs of
                Nothing -> return ()
                Just reqs' -> f reqs'

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
