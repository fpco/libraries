{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

module Distributed.RedisQueue.Internal where

import           ClassyPrelude
import           Data.Binary (Binary)
import           FP.Redis

-- * Types used in the API

-- | Common information about redis, used by both client and worker.
data RedisInfo = RedisInfo
    { redisConnection :: Connection
    , redisConnectInfo :: ConnectInfo
    , redisKeyPrefix :: ByteString
    }

-- | ID of a redis channel used for notifications about a particular
-- request.  One way to use this is to give each client server its own
-- 'BackchannelId', so that it is only informed of responses
-- associated with the requests it makes.
newtype BackchannelId = BackchannelId { unBackchannelId :: ByteString }
    deriving (Eq, Show, Binary, IsString)

-- | Every worker has a 'WorkerId' to uniquely identify it.  It's
-- needed for the fault tolerance portion - in the event that a worker
-- goes down we need to be able to re-enqueue its work.
newtype WorkerId = WorkerId { unWorkerId :: ByteString }
    deriving (Eq, Show, Binary, IsString)

-- | This is the key used for enqueued requests, and, later, the
-- response associated with it.  It's the hash of the request, which
-- allows responses to be cached.
newtype RequestId = RequestId { unRequestId :: ByteString }
    deriving (Eq, Show, Binary, Hashable)

-- * Datatypes used for serialization / deserialization

data RequestInfo = RequestInfo RequestId BackchannelId
    deriving (Generic)

instance Binary RequestInfo

-- * Functions to compute Redis keys

-- List of "Data.Binary" encoded @RequestInfo@.
requestsKey :: RedisInfo -> LKey
requestsKey r = LKey $ Key $ redisKeyPrefix r <> "requests"

-- Given a 'RequestId', computes the key for the request or response
-- data.
requestDataKey, responseDataKey :: RedisInfo -> RequestId -> VKey
requestDataKey  r k = VKey $ Key $ redisKeyPrefix r <> "request:" <> unRequestId k
responseDataKey r k = VKey $ Key $ redisKeyPrefix r <> "response:" <> unRequestId k

-- Given a 'BackchannelId', computes the name of a 'Channel'.  This
-- 'Channel' is used to notify clients when responses are available.
responseChannel :: RedisInfo -> BackchannelId -> Channel
responseChannel r k =
    Channel $ redisKeyPrefix r <> "responses:" <> unBackchannelId k

-- Given a 'WorkerId', computes the name of a list which holds the
-- items it's currently working on.
activeKey :: RedisInfo -> WorkerId -> LKey
activeKey r k = LKey $ Key $ redisKeyPrefix r <> "active:" <> unWorkerId k

-- * Redis utilities

-- | This acquires a Redis 'Connection' and wraps it up along with a
-- key prefix into a 'RedisInfo' value.  This 'RedisInfo' value can
-- then be used to run the various functions in this module.
withRedis
    :: MonadConnect m
    => ByteString -> ConnectInfo -> (RedisInfo -> m a) -> m a
withRedis redisKeyPrefix redisConnectInfo f =
    withConnection redisConnectInfo $ \redisConnection -> f RedisInfo {..}

run :: MonadCommand m => RedisInfo -> CommandRequest a -> m a
run = runCommand . redisConnection

run_ :: MonadCommand m => RedisInfo -> CommandRequest a -> m ()
run_ = runCommand_ . redisConnection
