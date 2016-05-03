{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE ViewPatterns #-}

module Distributed.Redis
    ( -- * Types
      RedisConfig(..)
    , defaultRedisConfig
    , defaultRetryPolicy
    , Redis(..)
    , NotifyChannel(..)

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

import           Control.Concurrent.Async (Async, async, withAsync, race)
import           Control.Concurrent.Lifted (fork)
import           Control.Concurrent.MVar.Lifted
import           Control.Exception (BlockedIndefinitelyOnMVar(..))
import           Control.Exception.Lifted (Exception, throwIO, catch)
import           Control.Monad (forever, void, guard)
import           Control.Monad.Catch (Handler(Handler))
import           Control.Monad.IO.Class
import           Control.Monad.Logger
import           Control.Monad.Trans.Control (MonadBaseControl, liftBaseWith)
import           Control.Retry
import           Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as BS8
import           Data.IORef.Lifted
import           Data.List.NonEmpty (NonEmpty((:|)))
import           Data.Serialize (Serialize)
import qualified Data.Serialize as C
import qualified Data.Text as T
import           Data.Time.Clock.POSIX
import           Data.Typeable (Typeable)
import           FP.Redis
import           FP.Redis.Internal (recoveringWithReset)
import           FP.Redis.Types.Internal (connectionInfo_)
import           FP.ThreadFileLogger
import           Safe (readMay)
import           Distributed.Types

-- Types
-----------------------------------------------------------------------

-- | Configuration of redis connection, along with a prefix for keys.
data RedisConfig = RedisConfig
    { rcConnectInfo :: !ConnectInfo
    -- ^ Redis host and port.
    , rcKeyPrefix :: !ByteString
    -- ^ Prefix to prepend to redis keys.
    }

-- | Default settingfs for connecting to redis:
--
-- * Use port 6379 (redis default port)
--
-- * Use 'defaultRetryPolicy' to determine redis reconnect behavior.
--
-- * It will use \"job-queue:\" as a key prefix in redis. This should
-- almost always get set to something else.
defaultRedisConfig :: RedisConfig
defaultRedisConfig = RedisConfig
    { rcConnectInfo = (connectInfo "localhost")
          { connectRetryPolicy = Just defaultRetryPolicy }
    , rcKeyPrefix = "job-queue:"
    }

-- | This is the retry policy used for 'withRedis' and 'withSubscription'
-- reconnects. If it fails 10 reconnects, with 1 second between each,
-- then it gives up.
defaultRetryPolicy :: RetryPolicy
defaultRetryPolicy = limitRetries 10 <> constantDelay (1000 * 1000)

-- | A connection to redis, along with a prefix for keys.
data Redis = Redis
    { redisConnection :: !Connection
    -- ^ Connection to redis.
    , redisKeyPrefix :: !ByteString
    -- ^ Prefix to prepend to redis keys.
    }

-- Operations
-----------------------------------------------------------------------

-- | Connect to redis, and provide the connection to the inner action.
-- When the inner action exits (either via return or exception), the
-- connection is released.
withRedis :: MonadConnect m => RedisConfig -> (Redis -> m a) -> m a
withRedis RedisConfig{..} f =
    withConnection rcConnectInfo $ \conn -> f Redis
        { redisConnection = conn
        , redisKeyPrefix = rcKeyPrefix
        }

-- | Convenient utility for creating a 'RedisConfig'.
mkRedisConfig :: ByteString -> Int -> Maybe RetryPolicy -> ByteString -> RedisConfig
mkRedisConfig host port mpolicy prefix = RedisConfig
    { rcConnectInfo = (connectInfo host)
        { connectPort = port
        , connectRetryPolicy = mpolicy
        }
    , rcKeyPrefix = prefix
    }

redisConnectInfo :: Redis -> ConnectInfo
redisConnectInfo = connectionInfo_ . redisConnection

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
    => Redis
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
        $logDebugS "withSubscription" ("Subscribing to " <> T.pack (show chan))
        withSubscriptionsWrappedConn ci subs $ \conn -> return $ \msg ->
            case msg of
                Unsubscribe {} -> $logErrorS "withSubscription" "Unexpected Unsubscribe"
                Subscribe {} -> do
                    $logDebugS "withSubscription" ("Subscribed to " <> T.pack (show chan))
                    resetRetries
                    connected $ do
                        writeIORef expectingDisconnect True
                        -- Disconnecting can block, so fork a thread.
                        void $ fork $ disconnectSub conn
                Message _ x -> f x

-- | Convenience function to run a redis command.
run :: MonadCommand m => Redis -> CommandRequest a -> m a
run = runCommand . redisConnection

-- | Convenience function to run a redis command, ignoring the result.
run_ :: MonadCommand m => Redis -> CommandRequest a -> m ()
run_ = runCommand_ . redisConnection

-- * Notification

newtype NotifyChannel = NotifyChannel Channel

-- | This subscribes to a notification channel. The yielded @MVar ()@ is
-- filled when we receive a notification. The yielded @IO ()@ action
-- unsubscribes from the channel.
--
-- When the connection is lost, at reconnect, the notification MVar is
-- also filled. This way, things will still work even if we missed a
-- notification.
subscribeToNotify
    :: MonadConnect m
    => Redis
    -> NotifyChannel
    -> m (MVar (), IO ())
subscribeToNotify redis (NotifyChannel chan) = do
    -- When filled, 'ready' indicates that the subscription has been established.
    ready <- newEmptyMVar
    -- This is the MVar yielded by the function. It gets filled when a
    -- message is received on the channel.
    notified <- newEmptyMVar
    -- This stores the disconnection action provided by
    -- 'withSubscription'.
    disconnectVar <- newIORef (error "impossible: disconnectVar not initialized.")
    let handleConnect dc = do
            writeIORef disconnectVar dc
            void $ tryPutMVar notified ()
            void $ tryPutMVar ready ()
    void $ asyncLifted $ logNest "subscribeToNotify" $
        withSubscription redis chan handleConnect $ \_ ->
            void $ tryPutMVar notified ()
    -- Wait for the subscription to connect before returning.
    takeMVarE ready NoLongerWaitingForRequest
    -- 'notified' also gets filled by 'handleConnect', since this is
    -- needed when a reconnection occurs. We don't want it to be filled
    -- for the initial connection, though, so we take it.
    takeMVarE notified NoLongerWaitingForRequest
    -- Since we waited for ready to be filled, disconnectVar must no
    -- longer contains its error value.
    unsub <- readIORef disconnectVar
    return (notified, unsub)

sendNotify
    :: MonadCommand m
    => Redis
    -> NotifyChannel
    -> m ()
sendNotify redis (NotifyChannel chan) = run_ redis $ publish chan ""

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

getRedisTime :: MonadCommand m => Redis -> VKey -> m (Maybe POSIXTime)
getRedisTime r k = fmap timeFromBS <$> run r (get k)

setRedisTime :: MonadCommand m => Redis -> VKey -> POSIXTime -> [SetOption] -> m ()
setRedisTime r k x opts = run_ r $ set k (timeToBS x) opts

-- * Concurrency utilities

-- | Like 'takeMVar', but convert wrap 'BlockedIndefinitelyOnMVar' exception in other exception.
takeMVarE :: (MonadBaseControl IO m, Exception ex) => MVar a -> ex -> m a
takeMVarE mvar exception = takeMVar mvar `catch` \BlockedIndefinitelyOnMVar -> throwIO exception

asyncLifted :: MonadBaseControl IO m => m () -> m (Async ())
asyncLifted f = liftBaseWith $ \restore -> async (void (restore f))

withAsyncLifted :: MonadBaseControl IO m => m () -> (Async () -> m ()) -> m ()
withAsyncLifted f g = liftBaseWith $ \restore -> withAsync (void (restore f)) (void . restore . g)

raceLifted :: MonadBaseControl IO m => m () -> m () -> m ()
raceLifted f g = liftBaseWith $ \restore -> void (race (void (restore f)) (void (restore g)))
