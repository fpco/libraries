{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE ViewPatterns #-}

-- | Functions and types shared by "Distributed.RedisQueue" and
-- "Distributed.JobQueue".
module Distributed.RedisQueue.Internal where

import ClassyPrelude
import Control.Concurrent (forkIO)
import Control.Monad.Catch (Handler(Handler))
import Control.Monad.Logger (logDebugS, logErrorS)
import Control.Retry (RetryPolicy, limitRetries, constantDelay)
import Data.Binary (Binary, decodeOrFail)
import Data.List.NonEmpty (NonEmpty((:|)))
import Data.Typeable (Proxy(..), typeRep)
import FP.Redis
import FP.Redis.Internal (recoveringWithReset)
import FP.ThreadFileLogger

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
    deriving (Eq, Ord, Show, Binary, IsString, Typeable)

-- | Every worker has a 'WorkerId' to uniquely identify it.  It's
-- needed for the fault tolerance portion - in the event that a worker
-- goes down we need to be able to re-enqueue its work.
newtype WorkerId = WorkerId { unWorkerId :: ByteString }
    deriving (Eq, Ord, Show, Binary, IsString, Typeable)

-- | This is the key used for enqueued requests, and, later, the
-- response associated with it.  It's the hash of the request, which
-- allows responses to be cached.
newtype RequestId = RequestId { unRequestId :: ByteString }
    deriving (Eq, Ord, Show, Binary, Hashable, Typeable)

-- * Datatypes used for serialization / deserialization

-- | Encoding used when enqueuing to the 'requestsKey' list.
data RequestInfo = RequestInfo
    { riBackchannel :: BackchannelId
    , riRequest :: RequestId
    }
    deriving (Eq, Ord, Show, Generic, Typeable)

instance Binary RequestInfo

-- * Functions to compute Redis keys

-- | List of "Data.Binary" encoded @RequestInfo@.  The client enqueues
-- requests to this list, and the workers pop them off.
requestsKey :: RedisInfo -> LKey
requestsKey r = LKey $ Key $ redisKeyPrefix r <> "requests"

-- | Given a 'RequestId', computes the key for the request data.
requestDataKey :: RedisInfo -> RequestId -> VKey
requestDataKey  r k = VKey $ Key $ redisKeyPrefix r <> "request:" <> unRequestId k

-- | Given a 'RequestId', computes the key for the response data.
responseDataKey :: RedisInfo -> RequestId -> VKey
responseDataKey r k = VKey $ Key $ redisKeyPrefix r <> "response:" <> unRequestId k

-- | Given a 'BackchannelId', computes the name of a 'Channel'.  This
-- 'Channel' is used to notify clients when responses are available.
responseChannel :: RedisInfo -> BackchannelId -> Channel
responseChannel r k =
    Channel $ redisKeyPrefix r <> "responses:" <> unBackchannelId k

-- | Given a 'WorkerId', computes the name of a redis key which usually
-- contains a list.  This list holds the items the worker is currently
-- working on.
--
-- See the documentation for 'Distributed.RedisQueue.HeartbeatFailure'
-- for more information on why this is not an 'LKey'.
activeKey :: RedisInfo -> WorkerId -> Key
activeKey r k = Key $ redisKeyPrefix r <> "active:" <> unWorkerId k

-- * Redis utilities

-- | This acquires a Redis 'Connection' and wraps it up along with a
-- key prefix into a 'RedisInfo' value.  This 'RedisInfo' value can
-- then be used to run the various functions in this module.
withRedis
    :: MonadConnect m
    => ByteString -> ConnectInfo -> (RedisInfo -> m a) -> m a
withRedis redisKeyPrefix ci f =
    withConnection redisConnectInfo $ \redisConnection -> f RedisInfo {..}
  where
    redisConnectInfo = ci
        { connectRetryPolicy = Just retryPolicy
        }
    retryPolicy = fromMaybe defaultRetryPolicy (connectRetryPolicy ci)

-- | It invokes the @IO () -> m ()@ action every time
-- the subscription is re-established, passing it an action which
-- disconnects the subscription.
--
-- See https://github.com/fpco/libraries/issues/54 for why this needs
-- to exist.
withSubscription
    :: MonadConnect m
    => RedisInfo
    -> Channel
    -> (IO () -> m ())
    -> (ByteString -> m ())
    -> m void
withSubscription r chan connected f = do
    isDisconnecting <- newIORef False
    let subs = subscribe (chan :| []) :| []
        ci = (redisConnectInfo r) { connectRetryPolicy = Nothing }
        handler = Handler $ \(fromException -> ex) -> case ex of
            Just DisconnectedException -> not <$> readIORef isDisconnecting
            _ -> return False
    forever $ recoveringWithReset defaultRetryPolicy [\_ -> handler] $ \resetRetries -> do
        $logDebugS "withSubscription" ("Subscribing to " ++ tshow chan)
        withSubscriptionsWrappedConn ci subs $ \conn -> return $ \msg ->
            case msg of
                Unsubscribe {} -> $logErrorS "withSubscription" "Unexpected Unsubscribe"
                Subscribe {} -> do
                    $logDebugS "withSubscription" ("Subscribed to " ++ tshow chan)
                    resetRetries
                    connected $ do
                        writeIORef isDisconnecting True
                        -- Disconnecting can block, so fork a thread.
                        void $ forkIO $ disconnectSub conn
                Message _ x -> f x

defaultRetryPolicy :: RetryPolicy
defaultRetryPolicy = limitRetries 10 ++ constantDelay (1000 * 1000)

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
    typ = show (typeRep (Proxy :: Proxy a))

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
