{-# OPTIONS_HADDOCK hide #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE ViewPatterns #-}

-- REVIEW TODO: Explicit exports
module Distributed.JobQueue.Client.Internal where

import           ClassyPrelude
import           Control.Concurrent (threadDelay)
import           Control.Concurrent.Async (race, async, cancel)
import           Control.Concurrent.STM (check, retry)
import           Control.Exception (AsyncException)
import           Control.Monad.Logger (MonadLogger, logErrorS, logInfoS, logDebugS, logWarnS, runLoggingT)
import           Control.Monad.Trans.Control (MonadBaseControl, liftBaseWith, restoreM)
import qualified Data.ByteString.Char8 as BS8
import           Data.List.NonEmpty (NonEmpty((:|)))
import           Data.Serialize (encode)
import           Data.Streaming.NetworkMessage (Sendable)
import           Data.TypeFingerprint
import           Data.Typeable (Proxy(..))
import           Data.Void (absurd, Void)
import           Distributed.Heartbeat (checkHeartbeats)
import           Distributed.Heartbeat.Internal (heartbeatActiveKey)
import           Distributed.JobQueue.Internal
import           Distributed.Redis
import           Distributed.Types
import           FP.Redis
import           FP.ThreadFileLogger
import           Focus (Decision(Remove))
import qualified ListT
import qualified STMContainers.Map as SM

data JobClient response = JobClient
    { jcConfig :: !JobQueueConfig
    , jcDispatch :: !(DispatchMap response)
    , jcRedis :: !Redis
    , jcLogFunc :: !LogFunc
    , jcKeepRunning :: !(IORef ())
    -- ^ This IORef is not used for anything except its identity. We
    -- want to detect when our @JobClient@ is no longer referenced by
    -- the application, since at that point we can stop running the
    -- background job client thread. However, weak pointers are only
    -- reliable when attached to objects with identity (like an @IORef@
    -- or @MVar@). So we keep such an @IORef@ in this data type and,
    -- when initializing, create a weak reference to it.
    }

type DispatchMap response =
    SM.Map RequestId (TVar (Maybe (Either DistributedException response)))

-- | Start a new job-queue client, which will run forever. For most
-- usecases, this should only be invoked once for the process, usually
-- on initialization.
--
-- The client does two things:
--
-- 1. Listening for responses from workers.
--
-- 2. Checking for worker heartbeat responses, and handling heartbeat
--    failures.
--
-- REVIEW TODO: The client is not run forever anymore. It's now garbage
-- collected.
-- REVIEW: We need a thread here for the client to check the heartbeats and
-- to subscribe to the channel receiving incoming response notifications.
newJobClient
    :: (MonadIO m, Sendable response)
    => LogFunc
    -> JobQueueConfig
    -> m (JobClient response)
newJobClient logFunc config@JobQueueConfig{..} = liftIO $ do
    dispatch <- SM.newIO
    inFlight <- newTVarIO 0
    jobQueueClientAsync <- async (jobClientThread logFunc config dispatch inFlight)
    -- See description of jcKeepRunning for the purpose of this strange
    -- IORef and weak pointer business
    keepRunning <- newIORef ()
    _ <- mkWeakIORef keepRunning $ do
        let logi = flip runLoggingT logFunc . $logInfoS "JobClient"
        logi "Detected JobClient no longer used, checking / waiting for there to be no callbacks"
        atomically $ do
            check =<< SM.null dispatch
            check . (0 ==) =<< readTVar inFlight
        logi "Detected JobClient no longer used, and no longer has any callbacks, killing jobQueueClient thread"
        cancel jobQueueClientAsync
    let ci = rcConnectInfo jqcRedisConfig
    conn <- runLoggingT (connect ci) logFunc
    return JobClient
        { jcConfig = config
        , jcDispatch = dispatch
        , jcRedis = Redis
            { redisConnection = conn
            , redisKeyPrefix = rcKeyPrefix jqcRedisConfig
            }
        , jcLogFunc = logFunc
        , jcKeepRunning = keepRunning
        }

jobClientThread
    :: forall response void.
       Sendable response
    => LogFunc -> JobQueueConfig -> DispatchMap response -> TVar Int -> IO void
jobClientThread logFunc config dispatch inFlight =
    flip runLoggingT logFunc $ forever $ do
        -- TODO: use redis conn here too. My reluctance to make the
        -- change now is that perhaps there is a subtle reason it is not
        -- already that way.
        eres <- try $ withRedis (jqcRedisConfig config) $
            jobClientThread' config dispatch inFlight
        case eres of
            Right x -> absurd x
            Left (fromException -> Just err) -> liftIO $ throwIO (err :: AsyncException)
            Left err -> do
                $logErrorS "JobClient" (pack (show err))
                $logInfoS "JobClient" "Waiting a second and restarting job client after exception."
                liftIO $ threadDelay (1000 * 1000)

jobClientThread'
    :: forall response m void.
       (MonadConnect m, Sendable response)
    => JobQueueConfig -> DispatchMap response -> TVar Int -> Redis -> m void
jobClientThread' config dispatch inFlight redis = do
    setRedisSchemaVersion redis
    result <- liftBaseWith $ \restore ->
        race (restore (checker :: m Void)) (restore (subscriber :: m Void))
    case result of
        Left v -> absurd =<< restoreM v
        Right v -> absurd =<< restoreM v
  where
    checker = checkHeartbeats (jqcHeartbeatConfig config) redis $ \inactive -> do
        reenqueuedSome <- fmap or $ forM inactive $
            handleWorkerFailure redis (jqcHeartbeatFailureExpiry config)
        when reenqueuedSome $ do
            $logDebugS "JobClient" "Notifying that some requests were re-enqueued"
            sendNotify redis (requestChannel redis)
    subscriber = withLogTag (LogTag "jobClient") $
        withSubscription redis (responseChannel redis) handleConnect $ \rid ->
            handleResponse (RequestId rid)
    -- When the subscription reconnects, check if any responses came
    -- back in the meantime.
    handleConnect _ = do
        $logDebugS "jobClient" "Checking for responses after (re)connect"
        contents <- atomically $ ListT.toList $ SM.stream dispatch
        forM_ contents $ \(rid, resultVar) -> do
            mresponse <- run redis $ get (responseDataKey redis rid)
            case mresponse of
                Nothing -> return ()
                Just response -> do
                    $logDebugS "jobClient" $
                        "Found a missed result after reconnect:" ++ tshow rid
                    atomically $ SM.delete rid dispatch
                    decodeOrThrow "jobClient" response >>=
                        atomically . writeTVar resultVar . Just
    handleResponse rid = do
        -- Lookup the handler before fetching / deleting the response,
        -- as the message may get delivered to multiple clients.
        let lookupAndRemove handler = return (handler, Remove)
        mresultVar <- atomically $ do
            res <- SM.focus lookupAndRemove rid dispatch
            when (isJust res) (modifyTVar' inFlight (subtract 1))
            return res
        let decrement = atomically (modifyTVar' inFlight (+ 1))
        forM_ mresultVar $ \resultVar -> (`finally` decrement) $ do
            mresponse <- run redis $ get (responseDataKey redis rid)
            case mresponse of
                Nothing -> atomically $ writeTVar resultVar $
                    Just (Left (ResponseMissingException rid))
                Just response ->
                    decodeOrThrow "jobClient" response >>=
                    atomically . writeTVar resultVar . Just

-- | Submits a new request. Returns a 'Just' if the response to the
-- request is already ready (e.g. if a request with the given
-- 'RequestId' was already submitted and processed).
--
-- REVIEW TODO: Consider dropping the automatic check for existing response,
-- since we can achieve that with the other functions in this module and
-- it makes 'submitRequest' a bit slower.
submitRequest
    :: forall request response m.
       (MonadCommand m, Sendable request, Sendable response)
    => JobClient response
    -> RequestId
    -> request
    -> m (Maybe response)
submitRequest jc@JobClient{..} rid request = flip runLoggingT jcLogFunc $ do
    let encoded = encode JobRequest
            { jrRequestTypeFingerprint = typeFingerprint (Proxy :: Proxy request)
            , jrResponseTypeFingerprint = typeFingerprint (Proxy :: Proxy response)
            , jrSchema = redisSchemaVersion
            , jrBody = encode request
            }
    $logDebugS "sendRequest" $ "Checking for response for request " <> tshow rid
    mresponse <- checkForResponse jc rid
    case mresponse of
        Nothing -> do
            $logDebugS "sendRequest" $ "Sending request " <> tshow rid
            submitRequestInternal jc rid encoded
            return Nothing
        Just (Left err) -> do
            $logDebugS "sendRequest" $ "Cached response to " <> tshow rid <> " is an error: " <> tshow err
            $logDebugS "sendRequest" $ "Clearing response cache for " <> tshow rid
            run_ jcRedis $ del (unVKey (responseDataKey jcRedis rid) :| [])
            submitRequestInternal jc rid encoded
            return Nothing
        Just (Right x) -> do
            $logDebugS "sendRequest" $ "Using cached response for " <> tshow rid
            return (Just x)

-- | Internal function to send a request without checking redis for an
-- existing response. Does not submit the request if the request data
-- already exists.
submitRequestInternal
    :: (MonadCommand m, MonadLogger m)
    => JobClient response -> RequestId -> ByteString -> m ()
submitRequestInternal JobClient{..} rid request = do
    $logDebugS "sendRequest" $ "Pushing request " <> tshow rid
    added <- run jcRedis $ set (requestDataKey jcRedis rid) request [EX (jqcRequestExpiry jcConfig), NX]
    if not added
        then $logWarnS "submitRequest" "Didn't submit request because it already exists in redis."
        else do
            run_ jcRedis $ lpush (requestsKey jcRedis) (unRequestId rid :| [])
            $logDebugS "submitRequest" $ "Notifying about request " <> tshow rid
            sendNotify jcRedis (requestChannel jcRedis)
            $logDebugS "submitRequest" $ "Done notifying about request " <> tshow rid
            addRequestEnqueuedEvent jcConfig jcRedis rid

-- | Checks if the specified 'RequestId' exists, in the form of request
-- or response data.
requestExists
    :: MonadCommand m
    => Redis -> RequestId -> m Bool
requestExists r k = do
    dataExists <- run r $ exists (unVKey (requestDataKey r k))
    if dataExists then return True else do
        run r $ exists (unVKey (responseDataKey r k))

-- | Provides an 'STM' action to be able to wait on the response.
waitForResponse ::
       (MonadIO m, MonadBaseControl IO m, Sendable response)
    => JobClient response
    -> RequestId
    -> m (STM (Maybe (Either DistributedException response)))
waitForResponse jc rid = do
    -- First, ensure that the request actually exists.
    reqExists <- requestExists (jcRedis jc) rid
    when (not reqExists) $
        throwIO (NoRequestForCallbackRegistration rid)
    (doCheck, resultVar) <- atomically $ do
        mresultVar <- SM.lookup rid (jcDispatch jc)
        case mresultVar of
            Just resultVar -> return (False, resultVar)
            Nothing -> do
                resultVar <- newTVar Nothing
                SM.insert resultVar rid (jcDispatch jc)
                return (True, resultVar)
    -- If the response already came back and the TVar isn't filled yet,
    -- fill with the response. This must happen after the above callback
    -- registration, because otherwise we can end up with circumstances
    -- where the response isn't observed.
    when doCheck $ do
        mresponse <- checkForResponse jc rid
        forM_ mresponse $ \response -> do
            atomically $ do
                writeTVar resultVar (Just response)
                SM.delete rid (jcDispatch jc)
    return (readTVar resultVar)

-- | Returns immediately with the request, if present.
checkForResponse ::
       (MonadIO m, MonadBaseControl IO m, Sendable response)
    => JobClient response
    -> RequestId
    -> m (Maybe (Either DistributedException response))
checkForResponse JobClient{..} rid = flip runLoggingT jcLogFunc $ do
    mresponse <- run jcRedis $ get (responseDataKey jcRedis rid)
    forM mresponse $ \response -> do
        result <- decodeOrThrow "checkForResponse" response
        addRequestEvent jcRedis rid RequestResponseRead
        return result

-- | This is a straightforward combination of 'submitRequest' and
-- 'waitForResponse'. The convenience of this function is that you don't
-- need to handle the case where 'submitRequest' finds a cached value.
submitRequestAndWaitForResponse ::
       (MonadCommand m, Sendable response, Sendable request)
    => JobClient response
    -> RequestId
    -> request
    -> m (STM (Maybe (Either DistributedException response)))
submitRequestAndWaitForResponse jc rid request = do
    mres <- submitRequest jc rid request
    case mres of
        Just res -> return (return (Just (Right res)))
        Nothing -> waitForResponse jc rid

-- | Cancel a request. Note that if the request is currently being worked
-- on, then you must also pass the 'WorkerId'. Returns 'True' if it
-- successfully removed the request from redis. (Note that this does
-- *not* guarantee that the worker actually manages to cancel its work).
--
-- NOTE: this feature is a bit experimental and may not work correctly
-- if the async exception gets caught. We may need to consider a more
-- forceful "cancel work" that involves killing processes.
--
-- FIXME: remove 'Maybe WorkerId' parameter.
--
-- FIXME: I think there's a race here where deleting the data could
-- cause a 'RequestMissingException'
--
-- REVIEW: Note that the only way to get the 'WorkerId' for a submitted request
-- is using the status.
cancelRequest :: MonadCommand m => Seconds -> Redis -> RequestId -> Maybe WorkerId -> m Bool
cancelRequest expiry redis k mwid = do
    run_ redis $ del (unVKey (requestDataKey redis k) :| [])
    ndel <- run redis (lrem (requestsKey redis) 1 (unRequestId k))
    case (ndel, mwid) of
        (1, Nothing) -> return True
        (0, Just wid) -> do
            eres <- try $ run redis $
                lrem (LKey (activeKey redis wid)) 1 (unRequestId k)
            case eres of
                Right 0 -> return False
                Right _ -> do
                    run_ redis $ set (cancelKey redis k) cancelValue [EX expiry]
                    return True
                -- Indicates a heartbeat failure.
                Left (_ :: RedisException) -> return False
        _ -> return False

handleWorkerFailure
    :: (MonadCommand m, MonadLogger m) => Redis -> Seconds -> WorkerId -> m Bool
handleWorkerFailure r (Seconds expiry) wid = do
    moved <- run r $ (eval script ks as :: CommandRequest Int64)
    case moved of
        (-1) -> return ()
        0 -> $logWarnS "JobQueue" $ tshow wid <>
            " failed its heartbeat, but didn't have items to re-enqueue."
        1 -> $logWarnS "JobQueue" $ tshow wid <>
            " failed its heartbeat.  Re-enqueuing its items."
        _ -> $logErrorS "JobQueue" $ unwords
            [ tshow wid
            , "failed its heartbeat.  Re-enqueing its items."
            , "It had more than one item on its work-queue.  This is"
            , "unexpected, and may indicate a bug."
            ]
    return (moved > 0)
  where
    as = [ unWorkerId wid
         , BS8.pack (show expiry)
         ]
    ks = [ activeKey r wid
         , unLKey (requestsKey r)
         , unZKey (heartbeatActiveKey r)
         ]
    -- NOTE: In order to handle moving many requests, this script
    -- should probably work around the limits of lua 'unpack'. This is
    -- fine for now, because we should only have at most one item in
    -- the active work queue.
    --
    -- See this older implementation, which I believe handles this,
    -- but is more complicated as a result:
    --
    -- https://github.com/fpco/libraries/blob/9b078aff00aab0a0ee30d33a3ffd9e3f5c869531/work-queue/src/Distributed/RedisQueue.hs#L349
    script = BS8.unlines
        [ "local xs = redis.pcall('lrange', KEYS[1], 0, -1)"
        -- This indicates that failure was already handled.
        , "if xs['err'] then"
        , "    redis.call('zrem', KEYS[3], ARGV[1])"
        , "    return -1"
        , "else"
        , "    local len = table.getn(xs)"
        , "    if len > 0 then"
        , "        redis.call('rpush', KEYS[2], unpack(xs))"
        , "    end"
        , "    redis.call('del', KEYS[1])"
        , "    redis.call('setex', KEYS[1], ARGV[2], 'HeartbeatFailure')"
        , "    redis.call('zrem', KEYS[3], ARGV[1])"
        , "    return len"
        , "end"
        ]

-- Utilities for blocking on responses.

atomicallyFromJust
    :: MonadIO m
     => STM (Maybe a)
     -> m a
atomicallyFromJust f = liftIO $ atomically $ do
    meres <- f
    case meres of
        Nothing -> retry
        Just x -> return x

atomicallyReturnOrThrow
    :: (Exception ex, MonadIO m)
    => STM (Maybe (Either ex a))
    -> m a
atomicallyReturnOrThrow f = liftIO $ atomically $ do
    meres <- f
    case meres of
        Nothing -> retry
        Just (Left err) -> throwSTM err
        Just (Right x) -> return x
