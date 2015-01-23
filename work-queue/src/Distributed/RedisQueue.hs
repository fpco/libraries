{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TemplateHaskell #-}

-- | This module provides a work-queue based on Redis, with the
-- following features:
--
-- (1) Many clients can enqueue work and block on responses, without
-- knowing anything about the workers.
--
-- (2) Many workers can ask for incoming work requests, and block on
-- these.
--
-- (3) A guarantee that enqueued work will not be lost until it's
-- completed, even in the presence of server failure.  A failure in
-- Redis persistence can invalidate this guarantee.
--
--     - SIDENOTE: We may need a high reliability redis configuration
--     for this guarantee as well. The redis wikipedia article
--     mentions that the default config can lose changes received
--     during the 2 seconds before failure.
--
-- One caveat is that it does not current make the guarantee that
-- results are only delivered once.  It's up to the client to deal
-- with this. The higher level API "Distributed.JobQueue" does handle
-- this gracefully.
module Distributed.RedisQueue
    ( ClientInfo(..), WorkerInfo(..), RedisInfo(..)
    , RequestId(..), BackchannelId(..), WorkerId(..)
    , DistributedRedisQueueException(..)
    , withRedisInfo
    , pushRequest
    , popRequest
    , sendResponse
    , readResponse
    , subscribeToResponses
    , sendHeartbeat
    , checkHeartbeats
    , getUnusedWorkerId
    ) where

import           ClassyPrelude
import           Control.Monad.Logger (MonadLogger, logError)
import           Data.Binary (Binary, encode, decode)
import qualified Data.ByteString.Char8 as BS8
import           Data.List.NonEmpty (NonEmpty((:|)))
import           Data.Ratio ((%))
import           Data.Time.Clock.POSIX (getPOSIXTime)
import           FP.Redis

-- | Info required to submit requests to the queue ('pushRequest'),
-- wait for responses ('subscribeToResponses'), and retrieve them
-- ('readResponse').
data ClientInfo = ClientInfo
    { clientRedis :: RedisInfo
    , clientBackchannelId :: BackchannelId
    }

-- | Info required to wait for incoming requests ('popRequest'), and
-- yield corresponding responses ('sendResponse').
data WorkerInfo = WorkerInfo
    { workerRedis :: RedisInfo
    , workerId :: WorkerId
    }

-- | Common information about redis, used by both client and worker.
data RedisInfo = RedisInfo
    { redisConnection :: Connection
    , redisConnectInfo :: ConnectInfo
    , redisKeyPrefix :: ByteString
    }

-- | ID of a redis channel used for notifications about a particular
-- request.  One way to use this is to give each client server its own
-- 'BackchannelId', so that it is informed of responses to the
-- requests it makes.
newtype BackchannelId = BackchannelId { unBackchannelId :: ByteString }
    deriving (Eq, Show, Binary, IsString)

-- | Every worker server has a 'WorkerId' to uniquely identify it.
-- It's needed for the fault tolerance portion - in the event that a
-- worker goes down we need to be able to reenqueue its work.
newtype WorkerId = WorkerId { unWorkerId :: ByteString }
    deriving (Eq, Show, Binary, IsString)

-- | This is the key used for enqueued requests, and, later, the
-- response associated with it.  It actually isn't an arbitrary
-- identifying ByteString - see 'encodeRequestId'.  Along with an ID,
-- It encodes a 'BackchannelId', so that the worker knows which
-- channel to push its response to.
newtype RequestId = RequestId { unRequestId :: ByteString }
    deriving (Eq, Show, Binary, Hashable)

encodeRequestId :: (BackchannelId, Int64) -> RequestId
encodeRequestId = RequestId . toStrict . encode

decodeRequestId :: RequestId -> (BackchannelId, Int64)
decodeRequestId = decode . fromStrict . unRequestId

withRedisInfo
    :: MonadConnect m => ByteString -> ConnectInfo -> (RedisInfo -> m ()) -> m ()
withRedisInfo redisKeyPrefix redisConnectInfo f =
    withConnection redisConnectInfo $ \redisConnection -> f RedisInfo {..}

pushRequest
    :: MonadCommand m => ClientInfo -> ByteString -> m RequestId
pushRequest (ClientInfo r bid) request = do
    -- Atomically increment a redis counter in order to get a new,
    -- unique request ID.
    requestNumber <- run r $ incr (idCounterKey r)
    -- Store the request data as a normal redis value.
    let k = encodeRequestId (bid, requestNumber)
    runSetNX r (requestDataKey r k) request
    -- Enqueue its ID on the requests list.
    run_ r $ lpush (requestsKey r) (unRequestId k)
    return k

popRequest
    :: MonadCommand m => WorkerInfo -> m (RequestId, ByteString)
popRequest (WorkerInfo r wid) = do
    mk <- run r $ brpoplpush (requestsKey r) (inProgressKey r wid) (Seconds 0)
    case mk of
        Nothing -> fail "impossible: brpoplpush with 0 timeout reported timeout"
        Just (RequestId -> k) -> do
            x <- getExisting r (requestDataKey r k)
            return (k, x)

sendResponse
    :: (MonadCommand m, MonadLogger m) => WorkerInfo -> RequestId -> ByteString -> m ()
sendResponse (WorkerInfo r wid) k x = do
    -- Store the response data, and notify the client that it's ready.
    run_ r $ set (responseDataKey r k) x []
    run_ r $ publish (responseChannelFor r k) (unRequestId k)
    -- Remove the RequestId associated with this response, from the
    -- list of in-progress requests.
    let ipk = inProgressKey r wid
    removed <- run r $ lrem ipk 1 (unRequestId k)
    when (removed == 0) $ do
        $logError $
            tshow ipk <>
            " isn't a member of the in-progress queue, likely indicating that\
            \ a heartbeat failure happened, causing it to be erroneously\
            \ re-enqueued.  This doesn't affect correctness, but could mean that\
            \ redundant work is performed."
    -- Remove the request data, as it's no longer needed.  We don't
    -- check if the removal succeeds, as this may not be the first
    -- time a response is sent for the request.  See the error message above.
    run_ r $ del [requestDataKey r k]

-- | Retrieves and deletes the response for the specified 'RequestId'.
readResponse
    :: MonadCommand m => RedisInfo -> RequestId -> m ByteString
readResponse r k = do
    response <- getExisting r k'
    delExisting r k'
    return response
  where
    k' = responseDataKey r k

-- | Subscribes to responses on the channel specified by this client's
-- 'BackchannelId'.  It changes the @subscribed@ 'TVar' to 'True' when
-- the subscription is established, and 'False' when unsubscribed.  In
-- order to be sure to receive the response, clients should wait for
-- the subscription to be established before enqueueing requests.
subscribeToResponses
    :: MonadConnect m => ClientInfo -> TVar Bool -> (RequestId -> m ()) -> m void
subscribeToResponses (ClientInfo r bid) subscribed f = do
    let sub = subscribe [responseChannel r bid]
    withSubscriptionsWrapped (redisConnectInfo r) (sub :| []) $
        trackSubscriptionStatus subscribed $ \_ k ->
            f (RequestId k)

sendHeartbeat
    :: MonadCommand m => WorkerInfo -> m ()
sendHeartbeat (WorkerInfo r wid) = do
    now <- liftIO getPOSIXTime
    run_ r $ zadd (heartbeatKey r) [(realToFrac now, unWorkerId wid)]

checkHeartbeats
    :: MonadCommand m => RedisInfo -> Int -> m ()
checkHeartbeats r micros = do
    now <- liftIO getPOSIXTime
    let ivl = fromRational (fromIntegral micros % (1000 * 1000))
        threshold = realToFrac now - ivl
    expired <- run r $ zrangebyscore (heartbeatKey r) 0 threshold False
    -- Need this check because zrem can fail otherwise.
    unless (null expired) $ do
        mapM_ (handleWorkerFailure r . WorkerId) expired
        run_ r $ zrem (heartbeatKey r) expired

-- NOTE: this should only be run when it's known for certain that
-- the worker is down and no longer manipulating the in-progress list,
-- since this is not an atomic update.
handleWorkerFailure
    :: MonadCommand m => RedisInfo -> WorkerId -> m ()
handleWorkerFailure r wid = do
    xs <- run r $ lrange (inProgressKey r wid) 0 (-1)
    unless (null xs) $ do
        run_ r $ rpush' (requestsKey r) xs
        delExisting r (inProgressKey r wid)

getUnusedWorkerId
    :: MonadCommand m => RedisInfo -> ByteString -> m WorkerId
getUnusedWorkerId r initial = go (0 :: Int)
  where
    go n = do
        let k | n == 0 = initial
              | otherwise = initial <> "-" <> BS8.pack (show n)
        exists <- liftM isJust $ run r $ zscore (heartbeatKey r) k
        if exists
            then go (n+1)
            else return (WorkerId k)

-- * Functions to compute Redis keys

idCounterKey, requestsKey, heartbeatKey :: RedisInfo -> Key
idCounterKey r = Key $ redisKeyPrefix r <> "id-counter"
requestsKey  r = Key $ redisKeyPrefix r <> "requests"
heartbeatKey r = Key $ redisKeyPrefix r <> "heartbeat"

requestDataKey, responseDataKey :: RedisInfo -> RequestId -> Key
requestDataKey  r k = Key $ redisKeyPrefix r <> "request:" <> unRequestId k
responseDataKey r k = Key $ redisKeyPrefix r <> "response:" <> unRequestId k

responseChannelFor :: RedisInfo -> RequestId -> Channel
responseChannelFor r k = responseChannel r (fst (decodeRequestId k))

responseChannel :: RedisInfo -> BackchannelId -> Channel
responseChannel r k = Channel $ redisKeyPrefix r <> "responses:" <> unBackchannelId k

inProgressKey :: RedisInfo -> WorkerId -> Key
inProgressKey r k = Key $ redisKeyPrefix r <> "in-progress:" <> unWorkerId k

-- * Redis utilities

run :: MonadCommand m => RedisInfo -> CommandRequest a -> m a
run = runCommand . redisConnection

run_ :: MonadCommand m => RedisInfo -> CommandRequest a -> m ()
run_ = runCommand_ . redisConnection

runSetNX :: MonadCommand m => RedisInfo -> Key -> ByteString -> m ()
runSetNX r k x = do
    worked <- run r $ set k x [NX]
    when (not worked) $ liftIO $ throwIO (KeyAlreadySet k)

getExisting :: MonadCommand m => RedisInfo -> Key -> m ByteString
getExisting r k = do
    mresult <- run r $ get k
    case mresult of
        Nothing -> liftIO $ throwIO (KeyMissing k)
        Just result -> return result

delExisting :: MonadCommand m => RedisInfo -> Key -> m ()
delExisting r k = do
    removed <- run r $ del [k]
    when (removed == 0) $ liftIO $ throwIO (KeyMissing k)

rpush' :: Key -> [ByteString] -> CommandRequest Int64
rpush' key vals = makeCommand "RPUSH" (encodeArg key : map encodeArg vals)

-- * Exceptions

data DistributedRedisQueueException
    = KeyAlreadySet Key
    | KeyMissing Key
    deriving (Eq, Show, Typeable)

instance Exception DistributedRedisQueueException
