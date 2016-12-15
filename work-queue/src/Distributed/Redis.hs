{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-|
Module: Distributed.Redis
-}
module Distributed.Redis
    ( -- * Types
      RedisConfig(..)
    , rcConnectInfo
    , defaultRedisConfig
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

    -- * Utilities for reading and writing timestamps
    , getRedisTime
    , setRedisTime

    -- * Serialization utilities
    , decodeOrErr
    , decodeOrThrow
    ) where

import qualified Control.Concurrent.Async.Lifted.Safe as Async
import           Control.Concurrent.Lifted (threadDelay)
import           Control.Concurrent.MVar.Lifted
import           Control.Exception.Lifted (throwIO)
import           Control.Monad (forever, void)
import           Control.Monad.IO.Class
import           Control.Retry
import           Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as BS8
import           Data.List.NonEmpty (NonEmpty((:|)))
import           Data.Store (Store)
import qualified Data.Store as S
import qualified Data.Text as T
import           Data.Time.Clock.POSIX
import           FP.Redis
import           Safe (readMay)
import           Distributed.Types
import           Data.Void (absurd)
import           Data.Monoid ((<>))

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
    , rcConnectionRetries :: !Int
    , rcConnectionRetriesDelay :: !Int
    } deriving (Eq, Show)

-- | Default settingfs for connecting to redis:
--
-- * Use port 6379 (redis default port)
--
-- * Use 'defaultRetryPolicy' to determine redis reconnect behavior.
defaultRedisConfig ::
    ByteString -- ^ Key Prefix
    -> RedisConfig
defaultRedisConfig prefix = RedisConfig
    { rcHost = "127.0.0.1"
    , rcPort = 6379
    , rcKeyPrefix = prefix
    , rcMaxConnections = 10
    , rcConnectionRetries = 0 -- By default we do not retry the redis connection.
    , rcConnectionRetriesDelay = 1000 * 1000 -- 1 second
    }

-- | A connection to redis, along with a prefix for keys.
data Redis = Redis
    { redisConnection :: !ManagedConnection
    -- ^ Connection to redis.
    , redisKeyPrefix :: !ByteString
    -- ^ Prefix to prepend to redis keys.
    }

-- Operations
-----------------------------------------------------------------------

-- | Get a 'ConnectInfo' from a 'RedisConfig'.
rcConnectInfo :: RedisConfig -> ConnectInfo
rcConnectInfo RedisConfig{..} = (connectInfo rcHost)
    { connectPort = rcPort
    , connectRetryPolicy = Just (limitRetries rcConnectionRetries <> constantDelay rcConnectionRetriesDelay)
    }

-- | Connect to redis, and provide the connection to the inner action.
-- When the inner action exits (either via return or exception), the
-- connection is released.
withRedis :: MonadConnect m => RedisConfig -> (Redis -> m a) -> m a
withRedis rc@RedisConfig{..} f =
    withManagedConnection (rcConnectInfo rc) rcMaxConnections $ \conn -> f Redis
        { redisConnection = conn
        , redisKeyPrefix = rcKeyPrefix
        }

-- | Get a 'ConnectInfo' from a 'Redis' connection.
redisConnectInfo :: Redis -> ConnectInfo
redisConnectInfo = managedConnectInfo . redisConnection

-- | Convenience function to run a redis command.
run :: MonadConnect m => Redis -> CommandRequest a -> m a
run redis cmd = useConnection (redisConnection redis) (\conn -> runCommand conn cmd)

-- | Convenience function to run a redis command, ignoring the result.
run_ :: MonadConnect m => Redis -> CommandRequest a -> m ()
run_ redis cmd = useConnection (redisConnection redis) (\conn -> runCommand_ conn cmd)

-- * Notification

-- | A channel to send notifications that trigger some action.  The
-- subscriber will listen to the 'NotifyChannel' using
-- 'withSubscribedNotifyChannel', and perform some action as soon as
-- 'sendNotify' is called on the 'NotifyChannel'.
newtype NotifyChannel = NotifyChannel Channel

-- | Perform some action upon notification in a 'NotifyChannel'.
--
-- Since it is possible that the notification does not arrive because
-- of a dying connection, there is also a timeout after which the
-- action will be performed, regardless of whether a notification
-- arrived or not.
--
--
withSubscribedNotifyChannel :: forall m a.
       (MonadConnect m)
    => ConnectInfo
    -- ^ Connection to use for the notification
    -> Milliseconds
    -- ^ Timeout after which the action will be called in case no notification arrived
    -> NotifyChannel
    -- ^ A notification in this 'NotifyChannel' triggers the action
    -> (m () -> m a)
    -- ^ Action to perform upon notification (or after the timeout, whichever happens first)
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

-- | Send a notification to a 'NotifyChannel'
sendNotify
    :: MonadConnect m
    => Redis
    -> NotifyChannel
    -> m ()
sendNotify redis (NotifyChannel chan) = run_ redis (publish chan "")

-- TODO: move the following utils elsewhere?

-- * Serialize utilities

-- | Attempt to decode a 'ByteString'.  Any errors will result in a
-- 'Left' 'DecodeError'.
decodeOrErr :: forall a. (Store a)
            => String -> ByteString -> Either DistributedException a
decodeOrErr src lbs =
    case S.decode lbs of
        Left exc -> Left (DecodeError (T.pack src) (T.pack (show exc)))
        Right x -> return x

-- | Attempt to decode the given 'ByteString'.  If this fails, then
-- throw a 'DecodeError' tagged with a 'String' indicating the source
-- of the decode error.
decodeOrThrow :: forall m a. (MonadIO m, Store a)
              => String -> ByteString -> m a
decodeOrThrow src lbs = either (liftIO . throwIO) return (decodeOrErr src lbs)

-- * Utilities for reading and writing timestamps

-- | Encode a 'POSIXTime' in a 'ByteString'.
--
-- The time is rounded to the nearest millisecond.
timeToBS :: POSIXTime -> ByteString
timeToBS = BS8.pack . show . timeToInt

-- | Inverse of 'timeToBS'.
timeFromBS :: ByteString -> POSIXTime
timeFromBS (BS8.unpack -> input) =
    case readMay input of
        Nothing -> error $ "Failed to decode timestamp " ++ input
        Just result -> timeFromInt result

timeToInt :: POSIXTime -> Int
timeToInt x = floor (x * 1000)

timeFromInt :: Int -> POSIXTime
timeFromInt x = fromIntegral x / 1000

-- | Retrieve a Timestamp from the database that was set via 'setRedisTime'.
getRedisTime :: MonadConnect m => Redis -> VKey -> m (Maybe POSIXTime)
getRedisTime r k = fmap timeFromBS <$> run r (get k)

-- | Store (or update) a Timestamp in the database.
setRedisTime :: MonadConnect m => Redis -> VKey -> POSIXTime -> [SetOption] -> m ()
setRedisTime r k x opts = run_ r $ set k (timeToBS x) opts
