{-# LANGUAGE NoImplicitPrelude, OverloadedStrings, ScopedTypeVariables, TypeFamilies,
             DeriveDataTypeable, FlexibleContexts, FlexibleInstances, RankNTypes, GADTs,
             ConstraintKinds, NamedFieldPuns, ViewPatterns #-}

-- | Mutexes built on Redis operations

module FP.Redis.Mutex
    ( module FP.Redis.Mutex.Types
    , periodicActionWrapped
    , periodicActionEx
    , withMutex
    , holdMutexDuring
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
                      -> PeriodicPrefix
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
                 -> PeriodicPrefix
                    -- ^ Key prefix for keys used to coordinate clients
                 -> Seconds
                    -- ^ Period (how often to run the action), in seconds
                 -> m ()
                    -- ^ I/O action to to perform.
                 -> m void
periodicActionEx conn (PeriodicPrefix pre) (Seconds period) inner = forever $ do
    let mutexKey = MutexKey $ Key $ pre ++ ".mutex"
    sleepSeconds <- withMutex conn mutexKey (Seconds 5) (Seconds 90) $ do
        let lastStartedKey = VKey (Key (pre ++ ".last-started-at"))
        maybeLastStartedAt <- runCommand conn (get lastStartedKey)
        curTime <- liftIO $ Time.getCurrentTime
        let lastStartedAt = fromMaybe (Time.UTCTime (Time.ModifiedJulianDay 0) 0)
                  (((unpack . decodeUtf8) <$> maybeLastStartedAt)
                    >>= readMay)
            interval = curTime `Time.diffUTCTime` lastStartedAt
        if interval >= fromIntegral period
            then do
                _ <- runCommand conn
                    (set lastStartedKey (encodeUtf8 (tshow curTime)) [])
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
withMutex :: MonadCommand m
          => Connection
             -- ^ Redis connection
          -> MutexKey
             -- ^ Mutex key
          -> Seconds
             -- ^ Refresh interval - how often the mutex is set.
          -> Seconds
             -- ^ Mutex time-to-live.  If the process exits without
             -- cleaning up, or gets disconnected from redis, this is
             -- how long other processes will need to wait until the
             -- mutex is available.
             --
             -- In order for this function to guarantee that it will
             -- either hold the mutex or throw an error, this needs to
             -- be larger than the maximum time spent retrying.
          -> m a
             -- ^ I/O action to run while mutex is claimed.
          -> m a
             -- ^ Value returned by the I/O action
withMutex conn key refresh mutexTtl inner =
    bracket (acquireMutex conn mutexTtl key)
            (releaseMutex conn key)
            (\token -> holdMutexDuring conn key token refresh mutexTtl inner)

-- | While the inner action is running, this periodically refreshes a
-- mutex that has already been acquired, likely by 'tryAcquireMutex'.
-- This will throw an exception if the mutex isn't already acquired.
--
-- Note that this doesn't release the mutex when the inner action
-- completes.
holdMutexDuring :: MonadCommand m
                => Connection
                -> MutexKey
                -> MutexToken
                -> Seconds
                -> Seconds
                -> m a
                -> m a
holdMutexDuring conn key token (Seconds refresh) mutexTtl inner =
    control $ \runInIO ->
        -- This is the same pattern as `race`, but using waitEitherCatch for more control
        -- over exception handling (otherwise ThreadKilled exceptions end up being rethrown)
        Async.withAsync (runInIO refreshThread) $ \a ->
            Async.withAsync (runInIO inner) $ \b -> do
                w <- Async.waitEitherCatch a b
                case w of
                    Left (Left e) -> throwM e
                    Left (Right _) -> error "FP.Redis.Mutex.holdMutexDuring: refreshThread should never return, but it did!"
                    Right (Left e) -> throwM e
                    Right (Right v) -> return v
  where
    refreshThread = forever $ do
        liftIO (threadDelay (fromIntegral refresh * 1000000))
        refreshMutex conn mutexTtl key token

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
             -> MutexKey
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
                -> MutexKey
                    -- ^ Mutex key
                -> m (Maybe MutexToken)
                    -- ^ Nothing if the mutex is unavailable, or Just a token
                    -- identifying who holds the mutex (see description).
tryAcquireMutex conn mutexTtl (VKey . unMutexKey -> key) = do
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
             -> MutexKey
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
                      [unMutexKey key]
                      [token] :: CommandRequest ())

-- | Refresh the time-to-live of a mutex.  This will retry for a limited period
-- of time if the Redis server is not reachable.
--
-- If you do not own the mutex (the mutex's token is different than the one you
-- passed in), this will throw 'IncorrectRedisMutexException'.
refreshMutex :: MonadCommand m
             => Connection
                -- ^ Redis connection
             -> Seconds
                -- ^ New time-to-live (in seconds)
             -> MutexKey
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
                                [unMutexKey key]
                                [encodeArg mutexTtl, token])
    if re == (0::Int64)
        then liftIO $ throwIO IncorrectRedisMutexException
        else return ()
