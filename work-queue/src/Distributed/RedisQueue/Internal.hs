{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE DeriveDataTypeable #-}

-- | Functions and types shared by "Distributed.RedisQueue" and
-- "Distributed.JobQueue".
module Distributed.RedisQueue.Internal where

import ClassyPrelude
import Data.Binary (Binary, decodeOrFail)
import Data.Typeable (typeRep)
import FP.Redis

-- * Types used in the API

-- | Common information about redis, used by both client and worker.
data RedisInfo = RedisInfo
    { redisConnection :: Connection
    , redisConnectInfo :: ConnectInfo
    , redisKeyPrefix :: ByteString
    } deriving (Typeable)

-- | ID of a redis channel used for notifications about a particular
-- request.  One way to use this is to give each client server its own
-- 'BackchannelId', so that it is only informed of responses
-- associated with the requests it makes.
newtype BackchannelId = BackchannelId { unBackchannelId :: ByteString }
    deriving (Eq, Show, Binary, IsString, Typeable)

-- | Every worker has a 'WorkerId' to uniquely identify it.  It's
-- needed for the fault tolerance portion - in the event that a worker
-- goes down we need to be able to re-enqueue its work.
newtype WorkerId = WorkerId { unWorkerId :: ByteString }
    deriving (Eq, Show, Binary, IsString, Typeable)

-- | This is the key used for enqueued requests, and, later, the
-- response associated with it.  It's the hash of the request, which
-- allows responses to be cached.
newtype RequestId = RequestId { unRequestId :: ByteString }
    deriving (Eq, Show, Binary, Hashable, Typeable)

-- * Datatypes used for serialization / deserialization

data RequestInfo = RequestInfo RequestId BackchannelId
    deriving (Generic, Typeable)

instance Binary RequestInfo

-- * Functions to compute Redis keys

-- | List of "Data.Binary" encoded @RequestInfo@.
requestsKey :: RedisInfo -> LKey
requestsKey r = LKey $ Key $ redisKeyPrefix r <> "requests"

-- | Given a 'RequestId', computes the key for the request or response
-- data.
requestDataKey, responseDataKey :: RedisInfo -> RequestId -> VKey
requestDataKey  r k = VKey $ Key $ redisKeyPrefix r <> "request:" <> unRequestId k
responseDataKey r k = VKey $ Key $ redisKeyPrefix r <> "response:" <> unRequestId k

-- | Given a 'BackchannelId', computes the name of a 'Channel'.  This
-- 'Channel' is used to notify clients when responses are available.
responseChannel :: RedisInfo -> BackchannelId -> Channel
responseChannel r k =
    Channel $ redisKeyPrefix r <> "responses:" <> unBackchannelId k

-- | Given a 'WorkerId', computes the name of a list which holds the
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

-- | Convenience function to run a redis command.
run :: MonadCommand m => RedisInfo -> CommandRequest a -> m a
run = runCommand . redisConnection

-- | Convenience function to run a redis command, ignoring the result.
run_ :: MonadCommand m => RedisInfo -> CommandRequest a -> m ()
run_ = runCommand_ . redisConnection

-- * Binary utilities

-- | Attempt to decode the given 'ByteString'.  If this fails, then
-- throw a 'DecodeError' tagged with a 'String' indicating the source
-- of the decode error.
decodeOrThrow :: forall m a. (MonadIO m, Binary a, Typeable a)
              => String -> ByteString -> m a
decodeOrThrow src lbs =
    case decodeOrFail (fromStrict lbs) of
        Left (_, _, err) -> throwErr err
        Right (remaining, _, x) ->
            if null remaining
                then return x
                else throwErr $ unwords
                    [ "Expected end of binary data."
                    , show (length remaining)
                    , "bytes remain."
                    ]
  where
    throwErr = liftIO . throwIO . DecodeError src typ
    typ = show (typeRep (Nothing :: Maybe a))

-- | Since the users of 'decodeOrThrow' attempt to ensure that types
-- and executable hashes match up, the occurance of this exception
-- indicates a bug in the library.
data DecodeError = DecodeError
    { deLoc :: String
    , deTyp :: String
    , deErr :: String
    }
    deriving (Show, Typeable)

instance Exception DecodeError
