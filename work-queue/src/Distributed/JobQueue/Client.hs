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
    , DistributedJobQueueException(..)
    )
    where

import           ClassyPrelude
import           Control.Concurrent.Async (withAsync)
import           Control.Monad.Logger (MonadLogger, logErrorS)
import           Control.Monad.Trans.Control (control)
import           Data.Binary (encode)
import           Data.Streaming.NetworkMessage (Sendable)
import           Distributed.JobQueue.Heartbeat (checkHeartbeats)
import           Distributed.JobQueue.Shared
import           Distributed.RedisQueue
import           Distributed.RedisQueue.Internal
import           FP.Redis
import           Focus (Decision(Remove, Replace))
import qualified STMContainers.Map as SM
import           Data.ConcreteTypeRep (fromTypeRep)
import           Data.Typeable (typeRep)

-- | Variables used by 'jobQueueClient' / 'jobQueueRequest'.
data ClientVars m response = ClientVars
    { clientSubscribed :: TVar Bool
      -- ^ This is set to 'True' once the client is subscribed to its
      -- response backchannel, and so ready to send reqeusts.
      -- 'jobQueueRequest' blocks until this is 'True'.
    , clientDispatch :: SM.Map RequestId (Either DistributedJobQueueException response -> m ())
      -- ^ A map between 'RequestId's and their associated handlers.
      -- 'jobQueueClient' uses this to invoke the handlers inserted by
      -- 'jobQueueRequest'.
    } deriving (Typeable)

-- | Create a new 'ClientVars' value.
newClientVars :: IO (ClientVars m response)
newClientVars = ClientVars
    <$> newTVarIO False
    <*> SM.newIO

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

defaultClientConfig :: ClientConfig
defaultClientConfig = ClientConfig
    { clientHeartbeatCheckIvl = Seconds 30
    , clientRequestExpiry = Seconds 3600
    , clientBackchannelId = defaultBackchannel
    }

defaultBackchannel :: BackchannelId
defaultBackchannel = "all-servers"

withJobQueueClient
    :: (MonadConnect m, Sendable response)
    => ClientConfig -> RedisInfo -> (ClientVars m response -> m a) -> m a
withJobQueueClient config r f = do
    control $ \restore -> do
        cvs <- newClientVars
        withAsync (restore (jobQueueClient config cvs r)) $ \_ ->
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
jobQueueClient config cvs r = do
    control $ \restore ->
        withAsync (restore checker) $ \_ -> restore $
            subscribeToResponses r
                                 (clientBackchannelId config)
                                 (clientSubscribed cvs)
                                 handleResponse
  where
    checker = checkHeartbeats r (clientHeartbeatCheckIvl config)
    handleResponse rid = do
        -- Lookup the handler before fetching / deleting the response,
        -- as the message may get delivered to multiple clients.
        let lookupAndRemove handler = return (handler, Remove)
        mhandler <- atomically $
            SM.focus lookupAndRemove rid (clientDispatch cvs)
        forM_ mhandler $ \handler -> do
            mresponse <- readResponse r rid
            case mresponse of
                Nothing -> throwM (ResponseMissingException rid)
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
    :: ( MonadCommand m, MonadLogger m, MonadThrow m
       , Sendable request, Sendable response )
    => ClientConfig
    -> ClientVars m response
    -> RedisInfo
    -> request
    -> m response
jobQueueRequest config cvs redis request = do
    (rid, mresponse) <- sendRequest config redis request
    case mresponse of
        Just response -> return response
        Nothing -> do
            resultVar <- newEmptyMVar
            registerResponseCallback cvs rid $ putMVar resultVar
            eres <- takeMVar resultVar
            either throwM return eres

-- | Sends a request to the workers.  This yields a 'RequestId' for use with
sendRequest
    :: forall m request response.
       ( MonadCommand m, MonadLogger m, MonadThrow m
       , Sendable request, Sendable response )
    => ClientConfig
    -> RedisInfo
    -> request
    -> m (RequestId, Maybe response)
sendRequest config r request = do
    let jrRequestType = fromTypeRep (typeRep (Nothing :: Maybe request))
        jrResponseType = fromTypeRep (typeRep (Nothing :: Maybe response))
        jrBody = toStrict (encode request)
        encoded = toStrict (encode JobRequest {..})
        expiry = clientRequestExpiry config
        ri = requestInfo (clientBackchannelId config) encoded
        k = riRequest ri
    mresponse <- pushRequest r expiry ri encoded
    case mresponse of
        Nothing -> do
            notifyRequestAvailable r
            return (k, Nothing)
        Just response -> do
            eres <- decodeOrThrow "sendRequest" response
            case eres of
                Left (_ :: DistributedJobQueueException) -> do
                    -- Cached exceptions are cleared.
                    clearResponse r k
                    return (k, Nothing)
                Right x -> return (k, Just x)

registerResponseCallback
    :: forall m response.
       (MonadCommand m, MonadLogger m, MonadThrow m , Sendable response)
    => ClientVars m response
    -> RequestId
    -> (Either DistributedJobQueueException response -> m ())
    -> m ()
registerResponseCallback cvs k handler = do
    atomically $ SM.focus addOrExtend k (clientDispatch cvs)
  where
    addOrExtend Nothing = return ((), Replace runHandler)
    addOrExtend (Just old) = return ((), Replace (\x -> old x >> runHandler x))
    runHandler response =
        catchAny (handler response) $ \ex ->
            $logErrorS "JobQueue" $
                "jobQueueRequest' callbackHandler: " ++ tshow ex
