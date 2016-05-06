{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveGeneric #-}

module Distributed.ConnectRequest
    ( WorkerConnectInfo(..)
    , requestWorker
    , withConnectRequests
    ) where

import Control.Monad (forever)
import Data.List.NonEmpty
import Data.Monoid ((<>))
import Data.Serialize (encode)
import Distributed.Redis
import FP.Redis
import Data.Serialize (Serialize)
import Data.ByteString (ByteString)
import Data.Typeable (Typeable)
import GHC.Generics (Generic)

-- * Information for connecting to a worker

data WorkerConnectInfo = WorkerConnectInfo
    { wciHost :: !ByteString
    , wciPort :: !Int
    }
    deriving (Eq, Show, Ord, Generic, Typeable)

instance Serialize WorkerConnectInfo

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
requestWorker
    :: MonadConnect m
    => Redis
    -> WorkerConnectInfo
    -> m ()
requestWorker r wci = do
    let encoded = encode wci
    run_ r (lpush (workerRequestsKey r) (encoded :| []))
    sendNotify r (workerRequestsNotify r)

withConnectRequests :: MonadConnect m => Redis -> (WorkerConnectInfo -> m ()) -> m void
withConnectRequests redis f = do
    withSubscribedNotifyChannel (managedConnectInfo (redisConnection redis)) (Milliseconds 100) (workerRequestsNotify redis) $
        \waitNotification -> forever $ do
            waitNotification
            mapM f =<< popWorkerRequest redis

-- REVIEW TODO: Do not "pop" this, instead just peek it, so that we can easily requests
-- slaves "forever".
popWorkerRequest :: MonadConnect m => Redis -> m (Maybe WorkerConnectInfo)
popWorkerRequest r =
    run r (rpop (workerRequestsKey r)) >>=
    mapM (decodeOrThrow "popWorkerRequest")

-- | Channel used for notifying that there's a new ConnectRequest.
workerRequestsNotify :: Redis -> NotifyChannel
workerRequestsNotify r = NotifyChannel $ Channel $ redisKeyPrefix r <> "connect-requests-notify"

-- | Key used for storing requests for workers.
workerRequestsKey :: Redis -> LKey
workerRequestsKey r = LKey $ Key $ redisKeyPrefix r <> "connect-requests"
