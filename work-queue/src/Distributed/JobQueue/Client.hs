{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TupleSections #-}

-- | This module provides the API used by job-queue clients.  See
-- "Distributed.JobQueue" for more info.
module Distributed.JobQueue.Client
    ( -- * Main client API
      ClientConfig(..)
    , defaultClientConfig
    , ClientVars(..)
    , withJobQueueClient
    , sendRequest
    , sendRequestWithId
    , registerResponseCallback
      -- * Extra client APIs
    , newClientVars
    , jobQueueClient
    , jobQueueRequest
    , jobQueueRequestWithId
    , checkForResponse
    , cancelRequest
    , DistributedJobQueueException(..)
    -- * Exposed for the test-suite
    , encodeRequest
    , sendRequestInternal'
    )
    where

import           ClassyPrelude
import           Control.Concurrent.Async (race)
import           Control.Monad.Logger (MonadLogger, logErrorS, logDebugS)
import           Control.Monad.Trans.Control (liftBaseWith, restoreM)
import           Data.Binary (encode)
import           Data.ConcreteTypeRep (fromTypeRep)
import           Data.List.NonEmpty (NonEmpty((:|)))
import           Data.Streaming.NetworkMessage (Sendable)
import           Data.Typeable (typeRep, Proxy(..))
import           Data.Void (absurd, Void)
import           Distributed.JobQueue.Heartbeat (checkHeartbeats)
import           Distributed.JobQueue.Shared
import           Distributed.RedisQueue
import           Distributed.RedisQueue.Internal
import           FP.Redis
import           FP.ThreadFileLogger
import           Focus (Decision(Remove, Replace))
import qualified ListT
import qualified STMContainers.Map as SM

-- | Variables used by 'jobQueueClient' / 'jobQueueRequest'.
data ClientVars m response = ClientVars
    { clientDispatch :: SM.Map RequestId (Either DistributedJobQueueException response -> m ())
      -- ^ A map between 'RequestId's and their associated handlers.
      -- 'jobQueueClient' uses this to invoke the handlers inserted by
      -- 'jobQueueRequest'.
    } deriving (Typeable)

-- | Create a new 'ClientVars' value.
newClientVars :: IO (ClientVars m response)
newClientVars = ClientVars <$> SM.newIO

-- | Configuration used for running the client functions of job-queue.
data ClientConfig = ClientConfig
    { clientHeartbeatCheckIvl :: Seconds
      -- ^ How often to send heartbeat requests to the workers, and
      -- check for responses.  This value should be the same for all
      -- clients.
    , clientRequestExpiry :: Seconds
      -- ^ The expiry time of the request data stored in redis.
    } deriving (Typeable)

-- | A default client configuration:
--
-- * Heartbeats are checked every 30 seconds
--
-- * Request bodies expire in redis after 1 hour
defaultClientConfig :: ClientConfig
defaultClientConfig = ClientConfig
    { clientHeartbeatCheckIvl = Seconds 30
    , clientRequestExpiry = Seconds 3600
    }

-- | This is the preferred way to run 'jobQueueClient'.  It ensures
-- that the thread gets cleaned up when the inner action completes.
withJobQueueClient
    :: forall m response a. (MonadConnect m, Sendable response)
    => ClientConfig -> RedisInfo -> (ClientVars m response -> m a) -> m a
withJobQueueClient config redis f = do
    result <- liftBaseWith $ \restore -> do
        cvs <- newClientVars
        race (restore (jobQueueClient config cvs redis :: m Void))
             (restore (f cvs :: m a))
    case result of
        Left v -> absurd =<< restoreM v
        Right x -> restoreM x

-- | Runs a listener for responses from workers, which dispatches to
-- callbacks registered with 'jobQueueRequest'.  It also runs
-- 'checkHeartbeats', to ensure that some server periodically checks
-- the worker heartbeats.
--
-- This function should be run in its own thread, as it never returns
-- (the return type is @void@).
jobQueueClient
    :: forall m response void. (MonadConnect m, Sendable response)
    => ClientConfig
    -> ClientVars m response
    -> RedisInfo
    -> m void
jobQueueClient config cvs redis = do
    result <- liftBaseWith $ \restore ->
        race (restore (checker :: m Void)) (restore (subscriber :: m Void))
    case result of
        Left v -> absurd =<< restoreM v
        Right v -> absurd =<< restoreM v
  where
    checker = checkHeartbeats redis (clientHeartbeatCheckIvl config)
    subscriber = withLogTag (LogTag "jobQueueClient") $
        subscribeToResponses redis handleConnect handleResponse
    -- When the subscription reconnects, check if any responses came
    -- back in the meantime.
    handleConnect _ = do
        $logDebugS "jobQueueClient" "Checking for responses after (re)connect"
        contents <- atomically $ ListT.toList $ SM.stream (clientDispatch cvs)
        forM_ contents $ \(rid, handler) -> do
            mresponse <- readResponse redis rid
            case mresponse of
                Nothing -> return ()
                Just response -> do
                    $logDebugS "jobQueueClient" $
                        "Found a missed result after reconnect:" ++ tshow rid
                    atomically $ SM.delete rid (clientDispatch cvs)
                    decodeOrThrow "jobQueueClient" response >>= handler
    handleResponse rid = do
        -- Lookup the handler before fetching / deleting the response,
        -- as the message may get delivered to multiple clients.
        let lookupAndRemove handler = return (handler, Remove)
        mhandler <- atomically $
            SM.focus lookupAndRemove rid (clientDispatch cvs)
        forM_ mhandler $ \handler -> do
            mresponse <- readResponse redis rid
            case mresponse of
                Nothing -> handler (Left (ResponseMissingException rid))
                Just response ->
                    decodeOrThrow "jobQueueClient" response >>=
                    handler

-- | Once a 'jobQueueClient' has been run with the 'ClientVars' value,
-- this function can be used to make requests and block on their
-- response.  It's up to the user of this and 'jobQueueWorker' to
-- ensure that the types of @request@ and @response@ match up.
-- Getting this wrong will cause a 'TypeMismatch' exception to be
-- thrown.
--
-- If the worker yields a 'DistributedJobQueueException', then this
-- function rethrows it.
jobQueueRequest
    :: forall m request response.
       (MonadCommand m, MonadLogger m, Sendable request, Sendable response)
    => ClientConfig
    -> ClientVars m response
    -> RedisInfo
    -> request
    -> m response
jobQueueRequest config cvs redis request = do
    let encoded = encodeRequest request (Proxy :: Proxy response)
        k = getRequestId encoded
    jobQueueRequestRaw config cvs redis k encoded

-- | Like 'jobQueueRequest', but allows the client to specify a custom
-- 'RequestId'.
--
-- Note that this 'RequestId' should have the property that it isn't
-- ever re-used for different request data. If it is re-used, then
-- cached values may be yielded.
jobQueueRequestWithId
    :: forall m request response.
       (MonadCommand m, MonadLogger m, Sendable request, Sendable response)
    => ClientConfig
    -> ClientVars m response
    -> RedisInfo
    -> RequestId
    -> request
    -> m response
jobQueueRequestWithId config cvs redis k request = do
    let encoded = encodeRequest request (Proxy :: Proxy response)
    jobQueueRequestRaw config cvs redis k encoded

jobQueueRequestRaw
    :: forall m response.
       (MonadCommand m, MonadLogger m, Sendable response)
    => ClientConfig
    -> ClientVars m response
    -> RedisInfo
    -> RequestId
    -> ByteString
    -> m response
jobQueueRequestRaw config cvs redis k encoded = do
    mresponse <- checkForResponse redis k
    case mresponse of
        Just eres ->
            either (liftIO . throwIO) return eres
        Nothing -> do
            -- We register the callback before sending the request so
            -- that we can avoid race conditions where the response
            -- comes back before registering the callback (even if
            -- this highly unlikely)
            resultVar <- newEmptyMVar
            registerResponseCallbackInternal cvs k $ putMVar resultVar
            sendRequestInternal config redis k encoded
            eres <- takeMVar resultVar
            either (liftIO . throwIO) return eres

-- | Sends a request to the workers.  This yields a 'RequestId' for
-- use with 'registerResponseCallback'.  If it yields a 'Just' value
-- for the response, then this indicates that there's already a cached
-- result for this request.
--
-- One thing to note is that if there's a cached
-- 'DistributedJobQueueException' result, it gets cleared.  The
-- assumption is that this exception was a temporary issue.
sendRequest
    :: forall m request response.
       (MonadCommand m, MonadLogger m, Sendable request, Sendable response)
    => ClientConfig
    -> RedisInfo
    -> request
    -> m (RequestId, Maybe response)
sendRequest config redis request = do
    let encoded = encodeRequest request (Proxy :: Proxy response)
        k = getRequestId encoded
    (k, ) <$> sendRequestRaw config redis k encoded

-- | Like 'sendRequest', but allows the client to specify a custom
-- 'RequestId'.
--
-- Note that this 'RequestId' should have the property that it isn't
-- ever re-used for different request data. If it is re-used, then
-- cached values may be yielded.
sendRequestWithId
    :: forall m request response.
       (MonadCommand m, MonadLogger m, Sendable request, Sendable response)
    => ClientConfig
    -> RedisInfo
    -> RequestId
    -> request
    -> m (Maybe response)
sendRequestWithId config redis k request = do
    let encoded = encodeRequest request (Proxy :: Proxy response)
    sendRequestRaw config redis k encoded

sendRequestRaw
    :: forall m response.
       (MonadCommand m, MonadLogger m, Sendable response)
    => ClientConfig
    -> RedisInfo
    -> RequestId
    -> ByteString
    -> m (Maybe response)
sendRequestRaw config redis k encoded = do
    $logDebugS "sendRequest" $ "Checking for response for request " <> tshow k
    mresponse <- checkForResponse redis k
    case mresponse of
        Nothing -> do
            $logDebugS "sendRequest" $ "Sending request " <> tshow k
            sendRequestInternal config redis k encoded
            return Nothing
        Just (Left err) -> do
            $logDebugS "sendRequest" $ "Cached response to " <> tshow k <> " is an error: " <> tshow err
            $logDebugS "sendRequest" $ "Clearing response cache for " <> tshow k
            clearResponse redis k
            sendRequestInternal config redis k encoded
            return Nothing
        Just (Right x) -> do
            $logDebugS "sendRequest" $ "Using cached response for " <> tshow k
            return (Just x)

-- | Computes the encoded ByteString representation of a 'JobRequest'.
encodeRequest
    :: forall request response. (Sendable response, Sendable request)
    => request
    -> Proxy response
    -> ByteString
encodeRequest request _ =
    toStrict $ encode JobRequest
        { jrRequestType = fromTypeRep (typeRep (Proxy :: Proxy request))
        , jrResponseType = fromTypeRep (typeRep (Proxy :: Proxy response))
        , jrBody = toStrict (encode request)
        }

-- Internal function to send a request without checking redis for an
-- existing response. It does check for an existing request, though, and
-- does nothing if found.
sendRequestInternal
    :: (MonadCommand m, MonadLogger m)
    => ClientConfig -> RedisInfo -> RequestId -> ByteString -> m ()
sendRequestInternal config redis k encoded = do
    exists <- requestExists redis k
    unless exists $ sendRequestInternal' config redis k encoded

-- Internal function to send a request without checking redis for an
-- existing response.
sendRequestInternal'
    :: (MonadCommand m, MonadLogger m)
    => ClientConfig -> RedisInfo -> RequestId -> ByteString -> m ()
sendRequestInternal' config redis k encoded = do
    let expiry = clientRequestExpiry config
    $logDebugS "sendRequest" $ "Pushing request " <> tshow k
    pushed <- pushRequest redis expiry k encoded
    $logDebugS "sendRequest" $ "Notifying about request " <> tshow k
    when pushed $ notifyRequestAvailable redis
    $logDebugS "sendRequest" $ "Done notifying about request " <> tshow k

-- | This registers a callback to handle the response to the specified
-- 'RequestId'.  Note that synchronous exceptions thrown by the
-- callback get swallowed and logged as errors.  This is because the
-- callbacks are usually run by the 'jobQueueClient' thread, and it
-- shouldn't halt due to an exception in the response callback.
--
-- This also checks if there's already a response available.  If it
-- is, then the callback is invoked.
registerResponseCallback
    :: forall m response.
       (MonadCommand m, MonadThrow m, MonadLogger m, Sendable response)
    => ClientVars m response
    -> RedisInfo
    -> RequestId
    -> (Either DistributedJobQueueException response -> m ())
    -> m ()
registerResponseCallback cvs redis k handler = do
    -- First, ensure that the request actually exists.
    reqExists <- requestExists redis k
    when (not reqExists) $
        throwM (NoRequestForCallbackRegistration k)
    -- We register a callback before checking for responses, because
    -- checking for responses takes time and could potentially block.
    registerResponseCallbackInternal cvs k handler
    -- If the response already came back, and the callback hasn't been
    -- called yet, then invoke it.
    mresponse <- checkForResponse redis k
    forM_ mresponse $ \response -> do
        dispatchExisted <- atomically $
            SM.focus (\mo -> return (isJust mo, Remove)) k (clientDispatch cvs)
        when dispatchExisted $ logCallbackExceptions k $ handler response

-- Like 'registerResponseCallback, but without checking if the result
-- already exists.
registerResponseCallbackInternal
    :: forall m response.
       (MonadCommand m, MonadLogger m, Sendable response)
    => ClientVars m response
    -> RequestId
    -> (Either DistributedJobQueueException response -> m ())
    -> m ()
registerResponseCallbackInternal cvs k handler = do
    atomically $ SM.focus addOrExtend k (clientDispatch cvs)
  where
    addOrExtend Nothing = return ((), Replace runHandler)
    addOrExtend (Just old) = return ((), Replace (\x -> old x >> runHandler x))
    runHandler = logCallbackExceptions k . handler

-- Internal function used to catch and log exceptions which occur in
-- the callback handlers.
logCallbackExceptions :: (MonadCommand m, MonadLogger m) => RequestId -> m () -> m ()
logCallbackExceptions k f =
     catchAny f $ \ex ->
         $logErrorS "JobQueue" ("Exception thrown in job-queue client callback for request " ++ tshow k ++ ": " ++ tshow ex)

-- | Check for a response, give a 'RequestId'.
checkForResponse
    :: (MonadCommand m, Sendable response)
    => RedisInfo
    -> RequestId
    -> m (Maybe (Either DistributedJobQueueException response))
checkForResponse redis k = do
     mresponse <- readResponse redis k
     forM mresponse $ decodeOrThrow "checkForResponse"

-- | Cancel a request. Note that if the request is currently being worked
-- on, then you must also pass the 'WorkerId'. Returns 'True' if it
-- successfully removed the request from redis. (Note that this does
-- *not* guarantee that the worker actually manages to cancel its work).
cancelRequest :: MonadCommand m => Seconds -> RedisInfo -> RequestId -> Maybe WorkerId -> m Bool
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
