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

import           ClassyPrelude
import           Control.Concurrent (forkIO)
import           Control.Monad.Catch (Handler(Handler))
import           Control.Monad.Logger (logDebugS, logErrorS)
import           Control.Retry (RetryPolicy, limitRetries, constantDelay)
import qualified Data.Aeson as Aeson
import           Data.Store (Store)
import qualified Data.Store as S
import qualified Data.ByteString.Char8 as BS8
import           Data.List.NonEmpty (NonEmpty((:|)))
import qualified Data.Text as T
import           Data.Time.Clock.POSIX
import           FP.Redis
import           FP.Redis.Internal (recoveringWithReset)
import           Control.DeepSeq (NFData)

-- * Types used in the API

-- | Common information about redis, used by both client and worker.
data RedisInfo = RedisInfo
    { redisConnection :: Connection
    , redisConnectInfo :: ConnectInfo
    , redisKeyPrefix :: ByteString
    } deriving (Typeable)

-- | Every worker has a 'WorkerId' to uniquely identify it.  It's
-- needed for the fault tolerance portion - in the event that a worker
-- goes down we need to be able to re-enqueue its work.
newtype WorkerId = WorkerId { unWorkerId :: ByteString }
    deriving (Eq, Ord, Show, Store, IsString, Typeable)

instance Aeson.ToJSON WorkerId where toJSON = Aeson.String . T.pack . BS8.unpack . unWorkerId

-- | This is the key used for enqueued requests, and, later, the
-- response associated with it.  It's the hash of the request, which
-- allows responses to be cached.
newtype RequestId = RequestId { unRequestId :: ByteString }
    deriving (Eq, Ord, Show, Store, Hashable, Typeable, NFData)

instance Aeson.ToJSON RequestId where toJSON = Aeson.String . T.pack . BS8.unpack . unRequestId

-- * Functions to compute Redis keys

-- | List of 'RequestId'. The client enqueues requests to this list,
-- and the workers pop them off.
requestsKey :: RedisInfo -> LKey
requestsKey r = LKey $ Key $ redisKeyPrefix r <> "requests"

-- | Given a 'RequestId', computes the key for the request data.
requestDataKey :: RedisInfo -> RequestId -> VKey
requestDataKey  r k = VKey $ Key $ redisKeyPrefix r <> "request:" <> unRequestId k

-- | Given a 'RequestId', stores when the request was enqueued.
requestTimeKey :: RedisInfo -> RequestId -> VKey
requestTimeKey r k = VKey $ Key $ redisKeyPrefix r <> "request:" <> unRequestId k <> ":time"

-- | Given a 'RequestId', computes the key for the response data.
responseDataKey :: RedisInfo -> RequestId -> VKey
responseDataKey r k = VKey $ Key $ redisKeyPrefix r <> "response:" <> unRequestId k

-- | Given a 'BackchannelId', computes the name of a 'Channel'.  This
-- 'Channel' is used to notify clients when responses are available.
responseChannel :: RedisInfo -> Channel
responseChannel r = Channel $ redisKeyPrefix r <> "response-channel"

-- | Given a 'RequestId', stores when the request was enqueued.
responseTimeKey :: RedisInfo -> RequestId -> VKey
responseTimeKey r k = VKey $ Key $ redisKeyPrefix r <> "response:" <> unRequestId k <> ":time"

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

-- | This creates a subscription to a redis channel. An @IO () -> m ()@
-- action is invoked every time the subscription is established
-- (initially, and on re-connect). The provided @IO ()@ allows you to
-- deactivate the subscription.
--
-- The @ByteString -> m ()@ action is run for every message received on
-- the 'Channel'.
--
-- Note that this never returns (its return type is @void@). When the
-- disconnect action @IO ()@ is invoked, or we run out of reconnect
-- retries, this will throw DisconnectedException.
--
-- See https://github.com/fpco/libraries/issues/54 for why this needs to
-- exist.
withSubscription
    :: MonadConnect m
    => RedisInfo
    -> Channel
    -> (IO () -> m ())
    -> (ByteString -> m ())
    -> m void
withSubscription r chan connected f = do
    expectingDisconnect <- newIORef False
    let subs = subscribe (chan :| []) :| []
        ci = (redisConnectInfo r) { connectRetryPolicy = Nothing }
        handler = Handler $ \ex -> case ex of
            DisconnectedException -> not <$> readIORef expectingDisconnect
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
                        writeIORef expectingDisconnect True
                        -- Disconnecting can block, so fork a thread.
                        void $ forkIO $ disconnectSub conn
                Message _ x -> f x

-- | This is the retry policy used for 'withRedis' and 'withSubscription'
-- reconnects. If it fails 10 reconnects, with 1 second between each,
-- then it gives up.
defaultRetryPolicy :: RetryPolicy
defaultRetryPolicy = limitRetries 10 ++ constantDelay (1000 * 1000)

-- | Convenience function to run a redis command.
run :: MonadCommand m => RedisInfo -> CommandRequest a -> m a
run = runCommand . redisConnection

-- | Convenience function to run a redis command, ignoring the result.
run_ :: MonadCommand m => RedisInfo -> CommandRequest a -> m ()
run_ = runCommand_ . redisConnection

-- * Serialize utilities

-- | Attempt to decode the given 'ByteString'.  If this fails, then
-- throw a 'DecodeError' tagged with a 'String' indicating the source
-- of the decode error.
decodeOrThrow :: forall m a. (MonadIO m, Store a)
              => String -> ByteString -> m a
decodeOrThrow src lbs =
    case S.decode lbs of
        Left err -> throwErr (show err)
        Right x -> return x
  where
    throwErr = liftIO . throwIO . DecodeError src

-- | Since the users of 'decodeOrThrow' attempt to ensure that types
-- and executable hashes match up, the occurance of this exception
-- indicates a bug in the library.
data DecodeError = DecodeError
    { deLoc :: String
    , deErr :: String
    }
    deriving (Show, Typeable)

instance Exception DecodeError

-- * Utilities for reading and writing timestamps

-- NOTE: Rounds time to the nearest millisecond.

timeToBS :: POSIXTime -> ByteString
timeToBS = BS8.pack . show . timeToInt

timeFromBS :: ByteString -> POSIXTime
timeFromBS (BS8.unpack -> input) =
    case readMay input of
        Nothing -> error $ "Failed to decode timestamp " ++ input
        Just result -> timeFromInt result

timeToInt :: POSIXTime -> Int
timeToInt x = floor (x * 1000)

timeFromInt :: Int -> POSIXTime
timeFromInt x = fromIntegral x / 1000

getRedisTime :: MonadCommand m => RedisInfo -> VKey -> m (Maybe POSIXTime)
getRedisTime r k = fmap timeFromBS <$> run r (get k)

setRedisTime :: MonadCommand m => RedisInfo -> VKey -> POSIXTime -> [SetOption] -> m ()
setRedisTime r k x opts = run_ r $ set k (timeToBS x) opts
