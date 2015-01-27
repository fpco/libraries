{-# LANGUAGE NoImplicitPrelude, OverloadedStrings, ScopedTypeVariables, TypeFamilies,
             DeriveDataTypeable, FlexibleContexts, FlexibleInstances, RankNTypes, GADTs,
             ConstraintKinds, NamedFieldPuns #-}

-- | Mutexes built on Redis operations

module FP.Redis.Mutex
    ( module FP.Redis.Mutex.Types
    , periodicActionWrapped
    , periodicActionEx
    , withMutex
    , acquireMutex
    , tryAcquireMutex
    , releaseMutex
    , refreshMutex )
    where

import ClassyPrelude.Conduit
import qualified Control.Concurrent.Async as Async
import Control.Concurrent (threadDelay)
import Control.Monad.Logger
import Control.Monad.Trans.Control (control)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import qualified Data.Time as Time
import qualified Data.UUID as UUID
import qualified Data.UUID.V4 as UUID

import FP.Redis
import FP.Redis.Mutex.Types

-- | Like 'periodicActionEx', but wrapps everything in non-async exception handlers so that
-- the periodic action keeps being called even if there are temporary problems causing exceptions.
periodicActionWrapped :: (MonadConnect m)
                      => Connection
                         -- ^ Redis connection
                      -> ByteString
                         -- ^ Key prefix for keys used to coordinate clients
                      -> Seconds
                         -- ^ Period (how often to run the action), in seconds
                      -> m ()
                         -- ^ I/O action to to perform.
                      -> m void
periodicActionWrapped conn keyPrefix period@(Seconds seconds) inner =
    forever (catchAny (periodicActionEx conn keyPrefix period wrappedInner) outerHandler)
  where
    wrappedInner = catchAny inner innerHandler
    outerHandler ex = do
        logErrorNS (connectLogSource (connectionInfo conn))
                   ("periodicActionWrapper outerHandler: " ++ tshow ex)
        liftIO (threadDelay (fromIntegral seconds * 1000000))
    innerHandler ex =
        logErrorNS (connectLogSource (connectionInfo conn))
                   ("periodicActionWrapped innerHandler: " ++ tshow ex)

-- | Coordinate between any number of clients to run a periodic action, where
-- each run of the action will be handled by one of the clients.
periodicActionEx :: (MonadConnect m)
                 => Connection
                    -- ^ Redis connection
                 -> ByteString
                    -- ^ Key prefix for keys used to coordinate clients
                 -> Seconds
                    -- ^ Period (how often to run the action), in seconds
                 -> m ()
                    -- ^ I/O action to to perform.
                 -> m void
periodicActionEx conn keyPrefix (Seconds period) inner = forever $ do
    sleepSeconds <- withMutex conn (Key (keyPrefix ++ ".mutex")) $ do
        maybeLastStartedAt <- runCommand conn
            (get (Key (keyPrefix ++ ".last-started-at")))
        curTime <- liftIO $ Time.getCurrentTime
        let lastStartedAt = fromMaybe (Time.UTCTime (Time.ModifiedJulianDay 0) 0)
                  (((unpack . decodeUtf8) <$> maybeLastStartedAt)
                    >>= readMay)
            interval = curTime `Time.diffUTCTime` lastStartedAt
        if interval >= fromIntegral period
            then do
                _ <- runCommand conn
                    (set (Key (keyPrefix ++ ".last-started-at")) (encodeUtf8 (tshow curTime)) [])
                inner
                curTime' <- liftIO $ Time.getCurrentTime
                return $ fromIntegral period - (curTime' `Time.diffUTCTime` curTime)
            else return $ fromIntegral period - interval
    when (sleepSeconds > 0.0) $
        liftIO $ threadDelay $ round $ sleepSeconds * 1000000
    return ()

-- | Acquire a mutex, run an action, and then release the mutex.  See
-- 'acquireMutex' for more information about the arguments.  This takes care
-- of refreshing the mutex for you in an async thread, without the action
-- needing to call `refreshMutex`.
withMutex :: (MonadCommand m, MonadThrow m)
          => forall a. Connection
             -- ^ Redis connection
          -> Key
             -- ^ Mutex key
          -> m a
             -- ^ I/O action to run while mutex is claimed.
          -> m a
             -- ^ Value returned by the I/O action
withMutex conn key inner =
    bracket (acquireMutex conn mutexTtl key)
            (releaseMutex conn key) $ \mutexToken ->
        control $ \runInIO ->
            -- This is the same pattern as `race`, but using waitEitherCatch for more control
            -- over exception handling (otherwise ThreadKilled exceptions end up being rethrown)
            Async.withAsync (runInIO $ refreshThread mutexToken) $ \a ->
                Async.withAsync (runInIO inner) $ \b -> do
                    w <- Async.waitEitherCatch a b
                    case w of
                        Left (Left e) -> throwM e
                        Left (Right _) -> error "FP.Redis.Mutex.waitMutex: refreshThread should never return, but it did!"
                        Right (Left e) -> throwM e
                        Right (Right v) -> return v
  where
    refreshThread mutexToken = forever $ do
        liftIO (threadDelay (refreshInterval * 1000000))
        refreshMutex conn mutexTtl key mutexToken
    -- This TTL should be at least as long as a cycle of retries in case the Redis server is
    -- temporarily unreachable.
    mutexTtl = Seconds 90
    refreshInterval = 5 :: Int

-- | Acquire a mutex.  This will block until the mutex is available.  It will
-- retry for a limited time if the Redis server is unreachable.
--
-- This uses the pattern outlined in <http://redis.io/commands/set>.
--
-- The mutex will be automatically released after the time-to-live, but you
-- will not be informed if this happens unless you call 'refreshMutex'.
--
-- If you must hold on to a mutex for a long period, it is best to set
-- a shorter time-to-live, but call 'refreshMutex' periodically to extend the
-- time to live.
acquireMutex :: (MonadCommand m)
             => Connection
                -- ^ Redis connection
             -> Seconds
                -- ^ Time-to-live of the mutex, in seconds (see description).
             -> Key
                -- ^ Mutex key
             -> m MutexToken
                -- ^ A token identifying who holds the mutex (see description).
acquireMutex conn mutexTtl key =
    go initDelay
  where
    go delay = do
        tokenMaybe <- tryAcquireMutex conn mutexTtl key
        case tokenMaybe of
            Just token -> return token
            Nothing -> do
                liftIO (threadDelay delay)
                go (if delay * 2 > maxDelay then maxDelay else delay)
    initDelay = 1000000
    maxDelay = 30000000

-- | Attempt to acquire a mutex.  This will return immediately if the mutex is
-- not available.  It will retry for a limited time if the Redis server is not
-- reachable.  See `acquireMutex` for more information about the arguments.
tryAcquireMutex :: (MonadCommand m)
                => Connection
                    -- ^ Redis connection
                -> Seconds
                    -- ^ Time-to-live of the mutex, in seconds (see description).
                -> Key
                    -- ^ Mutex key
                -> m (Maybe MutexToken)
                    -- ^ Nothing if the mutex is unavailable, or Just a token
                    -- identifying who holds the mutex (see description).
tryAcquireMutex conn mutexTtl key = do
    token <- liftIO $ (BS.concat . BSL.toChunks . UUID.toByteString) <$> UUID.nextRandom
    re <- runCommand conn (set key token [NX,EX mutexTtl])
    return (if re then Just (MutexToken token)
                  else Nothing)

-- | Release a mutex.  This will retry for a limited period of time if the
-- Redis server is not reachable.
--
-- This will /not/ report an error if you do not own the mutex, but will not
-- release a mutex that someone else owns.
releaseMutex :: (MonadCommand m)
             => Connection
                -- ^ Redis connection
             -> Key
                -- ^ Mutex key
             -> MutexToken
                -- ^ Token returned by 'acquireMutex'
             -> m ()
releaseMutex conn key (MutexToken token) =
    runCommand_ conn
                (eval "if redis.call('get',KEYS[1]) == ARGV[1]\n\
                      \then\n\
                      \    return redis.call(\"del\",KEYS[1])\n\
                      \else\n\
                      \    return 0\n\
                      \end"
                      [key]
                      [token] :: CommandRequest ())

-- | Refresh the time-to-live of a mutex.  This will retry for a limited period
-- of time if the Redis server is not reachable.
--
-- If you do not own the mutex (the mutex's token is different than the one you
-- passed in), this will throw 'IncorrectRedisMutexException'.
refreshMutex :: (MonadCommand m, MonadThrow m)
             => Connection
                -- ^ Redis connection
             -> Seconds
                -- ^ New time-to-live (in seconds)
             -> Key
                -- ^ Mutex key
             -> MutexToken
                -- ^ The token that was returned by 'acquireMutex'
             -> m ()
refreshMutex conn mutexTtl key (MutexToken token) = do
    re <- runCommand conn (eval "if redis.call(\"get\",KEYS[1]) == ARGV[2]\n\
                                \then\n\
                                \    redis.call(\"setex\",KEYS[1],ARGV[1],ARGV[2])\n\
                                \    return 1\n\
                                \else\n\
                                \    return 0\n\
                                \end"
                                [key]
                                [encodeArg mutexTtl, token])
    if re == (0::Int64)
        then throwM IncorrectRedisMutexException
        else return ()
