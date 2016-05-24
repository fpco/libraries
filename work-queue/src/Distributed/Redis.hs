{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Distributed.Redis
    ( -- * Types
      RedisConfig(..)
    , rcConnectInfo
    , defaultRedisConfig
    , defaultRetryPolicy
    , Redis(..)
    , redisConnectInfo

    -- * Notifications
    , NotifyChannel(..)
    , withSubscribedNotifyChannel
    , sendNotify

    -- * Initialization/running
    , withRedis
    , run
    , run_

    -- * Operations
    , getRedisTime
    , setRedisTime

    -- * Spurious utilities
    , decodeOrErr
    , decodeOrThrow
    ) where

import qualified Control.Concurrent.Async.Lifted.Safe as Async
import           Control.Concurrent.Lifted (threadDelay)
import           Control.Concurrent.MVar.Lifted
import           Control.Exception.Lifted (throwIO)
import           Control.Monad (forever, void, guard)
import           Control.Monad.IO.Class
import           Control.Retry
import           Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as BS8
import           Data.List.NonEmpty (NonEmpty((:|)))
import           Data.Serialize (Serialize)
import qualified Data.Serialize as C
import qualified Data.Text as T
import           Data.Time.Clock.POSIX
import           FP.Redis
import           Safe (readMay)
import           Distributed.Types
import           Data.Void (absurd)

-- Types
-----------------------------------------------------------------------

-- | Configuration of redis connection, along with a prefix for keys.
data RedisConfig = RedisConfig
    { rcHost :: !ByteString
    , rcPort :: !Int
    -- ^ Redis host and port.
    , rcKeyPrefix :: !ByteString
    -- ^ Prefix to prepend to redis keys.
    , rcMaxConnections :: !Int
    } deriving (Eq, Show)

-- | Default settingfs for connecting to redis:
--
-- * Use port 6379 (redis default port)
--
-- * Use 'defaultRetryPolicy' to determine redis reconnect behavior.
defaultRedisConfig :: ByteString -> RedisConfig
defaultRedisConfig prefix = RedisConfig
    { rcHost = "127.0.0.1"
    , rcPort = 6379
    , rcKeyPrefix = prefix
    , rcMaxConnections = 10
    }

-- | This is the retry policy used for 'withRedis' and 'withSubscription'
-- reconnects. If it fails 10 reconnects, with 1 second between each,
-- then it gives up.
defaultRetryPolicy :: RetryPolicy
defaultRetryPolicy = limitRetries 10 <> constantDelay (1000 * 1000)

-- | A connection to redis, along with a prefix for keys.
data Redis = Redis
    { redisConnection :: !ManagedConnection
    -- ^ Connection to redis.
    , redisKeyPrefix :: !ByteString
    -- ^ Prefix to prepend to redis keys.
    }

-- Operations
-----------------------------------------------------------------------

rcConnectInfo :: RedisConfig -> ConnectInfo
rcConnectInfo RedisConfig{..} = (connectInfo rcHost){connectPort = rcPort}

-- | Connect to redis, and provide the connection to the inner action.
-- When the inner action exits (either via return or exception), the
-- connection is released.
withRedis :: MonadConnect m => RedisConfig -> (Redis -> m a) -> m a
withRedis rc@RedisConfig{..} f = do
    withManagedConnection (rcConnectInfo rc) rcMaxConnections $ \conn -> f Redis
        { redisConnection = conn
        , redisKeyPrefix = rcKeyPrefix
        }

{-
-- | Convenient utility for creating a 'RedisConfig'.
mkRedisConfig :: ByteString -> Int -> Maybe RetryPolicy -> ByteString -> Int -> RedisConfig
mkRedisConfig host port mpolicy prefix maxConns = RedisConfig
    { rcConnectInfo = (connectInfo host)
        { connectPort = port
        , connectRetryPolicy = mpolicy
        }
    , rcKeyPrefix = prefix
    , rcMaxConnections = maxConns
    }
-}

redisConnectInfo :: Redis -> ConnectInfo
redisConnectInfo = managedConnectInfo . redisConnection

-- | Convenience function to run a redis command.
run :: MonadConnect m => Redis -> CommandRequest a -> m a
run redis cmd = useConnection (redisConnection redis) (\conn -> runCommand conn cmd)

-- | Convenience function to run a redis command, ignoring the result.
run_ :: MonadConnect m => Redis -> CommandRequest a -> m ()
run_ redis cmd = useConnection (redisConnection redis) (\conn -> runCommand_ conn cmd)

-- * Notification

newtype NotifyChannel = NotifyChannel Channel

withSubscribedNotifyChannel :: forall m a.
       (MonadConnect m)
    => ConnectInfo
    -> Milliseconds
    -- ^ Since subscriptions are not reliable, we also call the notification
    -- action every X milliseconds.
    -> NotifyChannel
    -> (m () -> m a)
    -> m a
withSubscribedNotifyChannel cinfo (Milliseconds millis) (NotifyChannel chan) cont = do
    notifyVar :: MVar () <- newEmptyMVar
    let subscriptionLoop :: forall b void. m b -> m void
        subscriptionLoop getMsg = forever $ do
            void getMsg
            void (tryPutMVar notifyVar ())
    let delayLoop :: forall void. m void
        delayLoop = forever $ do
            void (tryPutMVar notifyVar ())
            threadDelay (fromIntegral millis * 1000)
    fmap (either id (either absurd absurd)) $
        Async.race
            (cont (takeMVar notifyVar))
            (Async.race
                (withSubscriptionsManaged cinfo (chan :| []) subscriptionLoop)
                delayLoop)

sendNotify
    :: MonadConnect m
    => Redis
    -> NotifyChannel
    -> m ()
sendNotify redis (NotifyChannel chan) = run_ redis (publish chan "")

-- TODO: move the following utils elsewhere?

-- * Serialize utilities

decodeOrErr :: forall a. (Serialize a)
            => String -> ByteString -> Either DistributedException a
decodeOrErr src lbs =
    case C.runGet (C.get <* (guard =<< C.isEmpty)) lbs of
        Left err -> Left (DecodeError (T.pack src) (T.pack err))
        Right x -> return x

-- | Attempt to decode the given 'ByteString'.  If this fails, then
-- throw a 'DecodeError' tagged with a 'String' indicating the source
-- of the decode error.
decodeOrThrow :: forall m a. (MonadIO m, Serialize a)
              => String -> ByteString -> m a
decodeOrThrow src lbs = either (liftIO . throwIO) return (decodeOrErr src lbs)

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

getRedisTime :: MonadConnect m => Redis -> VKey -> m (Maybe POSIXTime)
getRedisTime r k = fmap timeFromBS <$> run r (get k)

setRedisTime :: MonadConnect m => Redis -> VKey -> POSIXTime -> [SetOption] -> m ()
setRedisTime r k x opts = run_ r $ set k (timeToBS x) opts
