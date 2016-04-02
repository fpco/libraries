{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}

module Distributed.ConnectRequest
    ( requestWorker
    , withConnectRequests
    ) where

import Control.Concurrent.STM (atomically, check, STM)
import Control.Exception.Lifted (finally)
import Control.Monad (forever)
import Control.Monad.IO.Class (liftIO)
import Data.Foldable (forM_)
import Data.List.NonEmpty
import Data.Monoid ((<>))
import Data.Serialize (encode)
import Distributed.Redis
import Distributed.Types
import FP.Redis

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
    :: MonadCommand m
    => Redis
    -> WorkerConnectInfo
    -> m ()
requestWorker r wci = do
    let encoded = encode wci
    run_ r $ lpush (workerRequestsKey r) (encoded :| [])
    sendNotify r (workerRequestsNotify r)

withConnectRequests :: MonadConnect m => STM Bool -> Redis -> (WorkerConnectInfo -> m ()) -> m void
withConnectRequests enabled r f = do
    (notifyVar, unsub) <- subscribeToNotify r (workerRequestsNotify r)
    (`finally` liftIO unsub) $ forever $ do
        mwci <- popWorkerRequest r
        forM_ mwci $ \wci -> do
            isEnabled <- liftIO $ atomically enabled
            if isEnabled
                then f wci
                else unpopWorkerRequest r wci
        -- Wait for a notification. Then, wait for receiving connect
        -- requests to be enabled.
        takeMVarE notifyVar NoLongerWaitingForWorkerRequest
        liftIO $ atomically $ enabled >>= check

popWorkerRequest :: MonadCommand m => Redis -> m (Maybe WorkerConnectInfo)
popWorkerRequest r =
    run r (rpop (workerRequestsKey r)) >>=
    mapM (decodeOrThrow "popWorkerRequest")

unpopWorkerRequest :: MonadCommand m => Redis -> WorkerConnectInfo -> m ()
unpopWorkerRequest r =
    run_ r . lpush (workerRequestsKey r) . (:| []) . encode


-- | Channel used for notifying that there's a new ConnectRequest.
workerRequestsNotify :: Redis -> NotifyChannel
workerRequestsNotify r = NotifyChannel $ Channel $ redisKeyPrefix r <> "connect-requests-notify"

-- | Key used for storing requests for workers.
workerRequestsKey :: Redis -> LKey
workerRequestsKey r = LKey $ Key $ redisKeyPrefix r <> "connect-requests"
