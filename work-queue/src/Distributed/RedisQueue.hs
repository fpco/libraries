{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE RecordWildCards #-}

module Distributed.RedisQueue
    ( ClientInfo(..), WorkerInfo(..), RedisInfo(..)
    , RequestId(..), BackchannelId(..), WorkerId(..)
    , DistributedRedisQueueException(..)
    , withRedisInfo
    , pushRequest
    , popRequest
    , sendResponse
    , readResponse
    , deleteResponse
    , withResponse
    , subscribeToResponses
    , dispatchResponse
    , sendHeartbeats
    , checkHeartbeats
    ) where

import ClassyPrelude
import Control.Concurrent (threadDelay)
import Data.Binary (Binary, encode, decode)
import Data.Ratio ((%))
import Data.Time.Clock.POSIX (getPOSIXTime)
import FP.Redis.Command (runCommand, runCommand_, makeCommand)
import FP.Redis.Command.Generic (del)
import FP.Redis.Command.List (lpush, lrem, lrange, brpoplpush)
import FP.Redis.Command.PubSub (publish)
import FP.Redis.Command.SortedSet (zadd, zrem, zrangebyscore)
import FP.Redis.Command.String (set, get, incr)
import FP.Redis.Connection (withConnection)
import FP.Redis.PubSub (withSubscriptionsEx, trackSubscriptionStatus, subscribe)
import FP.Redis.Types (Connection, ConnectInfo, MonadCommand, MonadConnect, CommandRequest, SetOption(NX))
import FP.Redis.Types.Internal (encodeArg)

-- Things to consider:
--
-- * Using a hash instead of atomically incrementing a request id
--
-- * Batching some of the commands by using send instead of run.

-- | Info required to submit requests to the queue ('pushRequest'),
-- wait for responses ('subscribeToResponses'), and retrieve them
-- ('readResponse' / 'withResponse').
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

newtype BackchannelId = BackchannelId { unBackchannelId :: ByteString }
    deriving (Eq, Show, Binary, IsString)

newtype WorkerId = WorkerId { unWorkerId :: ByteString }
    deriving (Eq, Show, Binary, IsString)

newtype RequestId = RequestId { unRequestId :: ByteString }
    deriving (Eq, Show, Binary)

encodeRequestId :: (BackchannelId, Int64) -> RequestId
encodeRequestId = RequestId . toStrict . encode

decodeRequestId :: RequestId -> (BackchannelId, Int64)
decodeRequestId = decode . fromStrict . unRequestId

withRedisInfo
    :: MonadConnect m
    => ByteString
    -> ConnectInfo
    -> (RedisInfo -> m ())
    -> m ()
withRedisInfo redisKeyPrefix redisConnectInfo f =
    withConnection redisConnectInfo $ \redisConnection -> f RedisInfo {..}

pushRequest
    :: MonadCommand m
    => ClientInfo
    -> ByteString
    -> m RequestId
pushRequest (ClientInfo r bid) request = do
    requestNumber <- run r $ incr (idCounterKey r)
    let k = encodeRequestId (bid, requestNumber)
    runSetNX r (requestDataKey r k) request
    run_ r $ lpush (requestsKey r) (unRequestId k)
    return k

popRequest
    :: MonadCommand m
    => WorkerInfo
    -> Int64
    -> m (RequestId, ByteString)
popRequest (WorkerInfo r wid) ms = do
    mk <- run r $ brpoplpush (requestsKey r) (inProgressKey r wid) ms
    case mk of
        Nothing -> fail ""
        Just (RequestId -> k) -> do
            x <- getExisting r (requestDataKey r k)
            return (k, x)

sendResponse
    :: MonadCommand m
    => WorkerInfo
    -> RequestId
    -> ByteString
    -> m ()
sendResponse (WorkerInfo r wid) k x = do
    run_ r $ set (responseDataKey r k) x []
    run_ r $ publish (responseChannelFor r k) (unRequestId k)
    delExisting r (requestDataKey r k)
    lremExisting r (inProgressKey r wid) 1 (unRequestId k)

readResponse
    :: MonadCommand m
    => ClientInfo
    -> RequestId
    -> m ByteString
readResponse (ClientInfo r _) = getExisting r . responseDataKey r

deleteResponse
    :: MonadCommand m
    => ClientInfo
    -> RequestId
    -> m ()
deleteResponse (ClientInfo r _) = delExisting r . responseDataKey r

withResponse
    :: MonadCommand m
    => ClientInfo
    -> RequestId
    -> (ByteString -> m a)
    -> m a
withResponse ci k =
    bracket (readResponse ci k) (\_ -> deleteResponse ci k)

subscribeToResponses
    :: MonadConnect m
    => ClientInfo
    -> TVar Bool
    -> (RequestId -> m ())
    -> m ()
subscribeToResponses (ClientInfo r bid) subscribed f = do
    let sub = subscribe [responseChannel r bid]
    withSubscriptionsEx (redisConnectInfo r) [sub] $
        trackSubscriptionStatus subscribed $ \_ k ->
            f (RequestId k)

dispatchResponse
    :: ( Functor m, MonadIO m
       , IsMap map, MapValue map ~ m (), ContainerKey map ~ RequestId)
    => m map
    -> RequestId
    -> m ()
dispatchResponse getMap k = do
    mcallback <- lookup k <$> getMap
    case mcallback of
        Nothing -> liftIO $ throwIO (NoCallbackFor k)
        Just callback -> callback

-- | This sends a heartbeat to redis. This should be done
-- periodically, so that we can know when to call
-- 'handleWorkerFailure'.
sendHeartbeats :: MonadCommand m => WorkerInfo -> Int -> m ()
sendHeartbeats (WorkerInfo r wid) micros = forever $ do
    now <- liftIO getPOSIXTime
    run_ r $ zadd (heartbeatKey r) [(realToFrac now, unWorkerId wid)]
    liftIO $ threadDelay micros

checkHeartbeats :: MonadCommand m => RedisInfo -> Int -> m ()
checkHeartbeats r micros = do
    now <- liftIO getPOSIXTime
    let ivl = fromRational (fromIntegral micros % (1000 * 1000))
        threshold = realToFrac now - ivl
    expired <- run r $ zrangebyscore (heartbeatKey r) 0 threshold False
    -- Need this check because zrem can fail otherwise.
    unless (null expired) $ do
        mapM_ (handleWorkerFailure r . WorkerId) expired
        run_ r $ zrem (heartbeatKey r) expired

-- | NOTE: this should only be run when it's known for certain that
-- the worker is down and no longer manipulating the in-progress list,
-- since this is not an atomic update.
handleWorkerFailure :: MonadCommand m => RedisInfo -> WorkerId -> m ()
handleWorkerFailure r wid = do
    xs <- run r $ lrange (inProgressKey r wid) 0 (-1)
    unless (null xs) $
        run_ r $ rpush' (requestsKey r) xs

data DistributedRedisQueueException
    = KeyAlreadySet ByteString
    | KeyMissing ByteString
    | ListItemMissing ByteString ByteString
    | NoCallbackFor RequestId
    | PopRequestTimeout
    deriving (Eq, Show, Typeable)

instance Exception DistributedRedisQueueException

idCounterKey, requestsKey, heartbeatKey :: RedisInfo -> ByteString
idCounterKey r = redisKeyPrefix r <> "id-counter"
requestsKey  r = redisKeyPrefix r <> "requests"
heartbeatKey r = redisKeyPrefix r <> "heartbeat"

requestDataKey, responseDataKey, responseChannelFor
    :: RedisInfo -> RequestId -> ByteString
requestDataKey     r k = redisKeyPrefix r <> "request." <> unRequestId k
responseDataKey    r k = redisKeyPrefix r <> "response." <> unRequestId k
responseChannelFor r k = responseChannel r (fst (decodeRequestId k))

responseChannel :: RedisInfo -> BackchannelId -> ByteString
responseChannel r k = redisKeyPrefix r <> "responses." <> unBackchannelId k

inProgressKey :: RedisInfo -> WorkerId -> ByteString
inProgressKey r k = redisKeyPrefix r <> "in-progress." <> unWorkerId k

run :: MonadCommand m => RedisInfo -> CommandRequest a -> m a
run = runCommand . redisConnection

run_ :: MonadCommand m => RedisInfo -> CommandRequest a -> m ()
run_ = runCommand_ . redisConnection

runSetNX :: MonadCommand m => RedisInfo -> ByteString -> ByteString -> m ()
runSetNX r k x = do
    worked <- run r $ set k x [NX]
    when (not worked) $ liftIO $ throwIO (KeyAlreadySet k)

getExisting :: MonadCommand m => RedisInfo -> ByteString -> m ByteString
getExisting r k = do
    mresult <- run r $ get k
    case mresult of
        Nothing -> liftIO $ throwIO (KeyMissing k)
        Just result -> return result

delExisting :: MonadCommand m => RedisInfo -> ByteString -> m ()
delExisting r k = do
    removed <- run r $ del [k]
    when (removed == 0) $ liftIO $ throwIO (KeyMissing k)

lremExisting :: MonadCommand m => RedisInfo -> ByteString -> Int64 -> ByteString -> m ()
lremExisting r k n v = do
    removed <- run r $ lrem k n v
    when (removed == 0) $ liftIO $ throwIO (ListItemMissing k v)

rpush' :: ByteString -> [ByteString] -> CommandRequest Int64
rpush' key vals = makeCommand "RPUSH" (encodeArg key : map encodeArg vals)
