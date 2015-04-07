{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}

-- | This module provides the API used by job-queue clients.  See
-- "Distributed.JobQueue" for more info.
module Distributed.JobQueue.Client
    ( -- * Main client API
      ClientConfig(..)
    , defaultClientConfig
    , ClientVars(..)
    , withJobQueueClient
    , sendRequest
    , registerResponseCallback
      -- * Extra client APIs
    , newClientVars
    , jobQueueClient
    , jobQueueRequest
    , checkForResponse
    , DistributedJobQueueException(..)
    )
    where

import           ClassyPrelude
import           Control.Concurrent.Async (withAsync)
import           Control.Monad.Logger (MonadLogger, logErrorS, logInfoS)
import           Control.Monad.Trans.Control (control)
import           Data.Binary (encode)
import           Data.ConcreteTypeRep (fromTypeRep)
import           Data.Streaming.NetworkMessage (Sendable)
import           Data.Typeable (typeRep, Proxy(..))
import           Distributed.JobQueue.Heartbeat (checkHeartbeats)
import           Distributed.JobQueue.Shared
import           Distributed.RedisQueue
import           Distributed.RedisQueue.Internal
import           FP.Redis
import           FP.ThreadFileLogger
import           Focus (Decision(Remove, Replace))
import qualified STMContainers.Map as SM
import qualified ListT

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
    , clientBackchannelId :: BackchannelId
      -- ^ Identifies the channel used to notify about a response.
    } deriving (Typeable)

-- | A default client configuration:
--
-- * Heartbeats are checked every 30 seconds
--
-- * Request bodies expire in redis after 1 hour
--
-- * Notifications responses use 'defaultBackchannel'
defaultClientConfig :: ClientConfig
defaultClientConfig = ClientConfig
    { clientHeartbeatCheckIvl = Seconds 30
    , clientRequestExpiry = Seconds 3600
    , clientBackchannelId = defaultBackchannel
    }

-- | A default backchannel.  If all clients use this, then they'll all
-- be notified about responses.
defaultBackchannel :: BackchannelId
defaultBackchannel = "all-servers"

-- | This is the preferred way to run 'jobQueueClient'.  It ensures
-- that the thread gets cleaned up when the inner action completes.
withJobQueueClient
    :: (MonadConnect m, Sendable response)
    => ClientConfig -> RedisInfo -> (ClientVars m response -> m a) -> m a
withJobQueueClient config redis f = do
    control $ \restore -> do
        cvs <- newClientVars
        withAsync (restore (jobQueueClient config cvs redis)) $ \_ ->
            restore (f cvs)

-- | Runs a listener for responses from workers, which dispatches to
-- callbacks registered with 'jobQueueRequest'.  It also runs
-- 'checkHeartbeats', to ensure that some server periodically checks
-- the worker heartbeats.
--
-- This function should be run in its own thread, as it never returns
-- (the return type is @void@).
jobQueueClient
    :: (MonadConnect m, Sendable response)
    => ClientConfig
    -> ClientVars m response
    -> RedisInfo
    -> m void
jobQueueClient config cvs redis = do
    control $ \restore ->
        withAsync (restore checker) $ \_ -> restore $
            withLogTag (LogTag "jobQueueClient") $
            subscribeToResponses redis
                                 (clientBackchannelId config)
                                 handleConnect
                                 handleResponse
  where
    checker = checkHeartbeats redis (clientHeartbeatCheckIvl config)
    -- When the subscription reconnects, check if any responses came
    -- back in the meantime.
    handleConnect _ = do
        $logInfoS "jobQueueClient" "Checking for responses after (re)connect"
        contents <- atomically $ ListT.toList $ SM.stream (clientDispatch cvs)
        forM_ contents $ \(rid, handler) -> do
            mresponse <- readResponse redis rid
            case mresponse of
                Nothing -> return ()
                Just response -> do
                    $logInfoS "jobQueueClient" $
                        "Found a missed result after reconnect:" ++ tshow rid
                    decodeOrThrow "jobQueueClient" response >>= handler
                    atomically $ SM.delete rid (clientDispatch cvs)
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
    let (ri, encoded) = prepareRequest config request (Proxy :: Proxy response)
        k = riRequest ri
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
            sendRequestIgnoringCache config redis ri encoded
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
    let (ri, encoded) = prepareRequest config request (Proxy :: Proxy response)
        k = riRequest ri
    mresponse <- checkForResponse redis k
    case mresponse of
        Nothing -> do
            sendRequestIgnoringCache config redis ri encoded
            return (k, Nothing)
        Just (Left _) -> do
            clearResponse redis k
            return (k, Nothing)
        Just (Right x) ->
            return (k, Just x)

-- Computes the 'RequestInfo' and encoded bytes for a request.
prepareRequest
    :: forall request response. (Sendable response, Sendable request)
    => ClientConfig
    -> request
    -> Proxy response
    -> (RequestInfo, ByteString)
prepareRequest config request _ = (ri, encoded)
  where
    ri = requestInfo (clientBackchannelId config) encoded
    encoded = toStrict $ encode JobRequest
        { jrRequestType = fromTypeRep (typeRep (Proxy :: Proxy request))
        , jrResponseType = fromTypeRep (typeRep (Proxy :: Proxy response))
        , jrBody = toStrict (encode request)
        }

-- Internal function to send a request without checking redis for an
-- existing response.
sendRequestIgnoringCache
    :: MonadCommand m
    => ClientConfig -> RedisInfo -> RequestInfo -> ByteString -> m ()
sendRequestIgnoringCache config redis ri encoded = do
    let expiry = clientRequestExpiry config
    pushRequest redis expiry ri encoded
    notifyRequestAvailable redis

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
       (MonadCommand m, MonadLogger m, Sendable response)
    => ClientVars m response
    -> RedisInfo
    -> RequestId
    -> (Either DistributedJobQueueException response -> m ())
    -> m ()
registerResponseCallback cvs redis k handler = do
    -- We register a callback before checking for responses, because
    -- checking for responses takes time and could potentially block.
    gotCalledRef <- newIORef False
    registerResponseCallbackInternal cvs k $ \response -> do
        writeIORef gotCalledRef True
        handler response
    -- If the response already came back, and the callback hasn't been
    -- called yet, then invoke it.
    mresponse <- checkForResponse redis k
    forM_ mresponse $ \response -> do
        alreadyGotCalled <- readIORef gotCalledRef
        unless alreadyGotCalled $ do
            atomically $ SM.delete k (clientDispatch cvs)
            logCallbackExceptions $ handler response

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
    runHandler = logCallbackExceptions . handler

-- Internal function used to catch and log exceptions which occur in
-- the callback handlers.
logCallbackExceptions :: (MonadCommand m, MonadLogger m) => m () -> m ()
logCallbackExceptions f =
     catchAny f $ \ex ->
         $logErrorS "JobQueue" ("logCallbackExceptions: " ++ tshow ex)

-- | Check for a response, give a 'RequestId'.
checkForResponse
    :: (MonadCommand m, Sendable response)
    => RedisInfo
    -> RequestId
    -> m (Maybe (Either DistributedJobQueueException response))
checkForResponse redis k = do
     mresponse <- readResponse redis k
     forM mresponse $ decodeOrThrow "checkForResponse"
