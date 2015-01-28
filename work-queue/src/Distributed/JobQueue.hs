{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}

-- | This module provides a particular way of using
-- "Distributed.WorkQueue" along with "Distributed.RedisQueue".  It
-- also makes rather non-generic decisions like using 'Vector' and
-- "STMContainers.Map".
module Distributed.JobQueue
    ( WorkerConfig(..)
    , jobQueueWorker
    , ClientVars(..)
    , newClientVars
    , jobQueueClient
    , jobQueueRequest
    , jobQueueRequest'
    ) where

import           ClassyPrelude
import           Control.Concurrent.Async (withAsync)
import           Control.Concurrent.Lifted (fork)
import           Control.Concurrent.STM (check)
import           Control.Monad.Logger (MonadLogger, logWarn, logError)
import           Control.Monad.Trans.Control (control)
import           Data.Binary (Binary, decode, encode)
import           Data.WorkQueue
import           Distributed.RedisQueue
import           FP.Redis.Types (MonadConnect, MonadCommand, Seconds(..))
import           Focus (Decision(Remove, Replace))
import           Network.HostName (getHostName)
import qualified STMContainers.Map as SM
import           System.Posix.Process (getProcessID)

-- | Configuration of a 'jobQueueWorker'.
data WorkerConfig = WorkerConfig
    { workerResponseDataExpiry :: Seconds
      -- ^ How many seconds the response data should be kept around.
      -- The longer it's kept around, the more opportunity there is to
      -- eliminate redundant work, if identical requests are made.  A
      -- longer expiry also allows more time between sending a
      -- response notification and the client reading its data.
    }

-- | This is intended for use as the body of the @inner@ function of a
-- master server.  It waits for a request encoded as @Vector payload@
-- and enqueues each item to a 'WorkQueue'.  It also runs
-- 'sendHeartbeats', in order to handle responding to heartbeat
-- requests.
--
-- Note that currently it will only work on one @Vector payload@
-- request at a time.  This is a limitation that will be fixed in the
-- future, if necessary.
--
-- Also note that the return type is @void@, indicating that this
-- function is never intended to return, as it is always waiting for
-- more work.
jobQueueWorker
    :: (MonadConnect m, Binary payload)
    => WorkerConfig
    -> RedisInfo
    -> WorkQueue payload result
    -> (Vector result -> m ByteString)
    -> m void
jobQueueWorker config r queue toResult = do
    wid <- getWorkerId r
    subscribed <- liftIO $ newTVarIO False
    let worker = WorkerInfo wid (workerResponseDataExpiry config)
        heartbeats = sendHeartbeats r worker subscribed
    control $ \restore -> withAsync (restore heartbeats) $ \_ -> restore $ do
        atomically $ check =<< readTVar subscribed
        forever $ do
            (requestId, bid, mrequest) <- popRequest r worker
            case mrequest of
                Nothing -> $logError $ "Request " ++ tshow requestId ++ " expired."
                Just request -> do
                    subresults <- mapQueue queue (decode (fromStrict request))
                    sendResponse r worker requestId bid =<< toResult subresults

-- | Variables and settings used by 'jobQueueClient' /
-- 'jobQueueRequest'.
data ClientVars m = ClientVars
    { clientSubscribed :: TVar Bool
      -- ^ This is set to 'True' once the client is subscribed to its
      -- backchannel, and so ready to send reqeusts.
      -- 'jobQueueRequest' blocks until this is 'True'.
    , clientDispatch :: SM.Map RequestId (ByteString -> m ())
      -- ^ A map between 'RequestId's and their associated handlers.
      -- 'jobQueueClient' uses this to invoke the handlers inserted by
      -- 'jobQueueRequest'.
    , clientHeartbeatCheckIvl :: Seconds
      -- ^ How often to send heartbeat requests to the workers, and
      -- check for responses.  This value should be the same for all
      -- clients.
    , clientInfo :: ClientInfo
      -- ^ Information about the client needed to invoke the client
      -- functions in "Distributed.RedisQueue".
    }

-- | Create a new 'ClientVars' value.  This uses a default method of
-- computing a 'BackChannelId', which combines the host name with the
-- process ID.  The user must provide values for
-- 'clientHeartbeatCheckIvl' and 'clientRequestExpiry' as arguments.
newClientVars :: Seconds -> Seconds -> IO (ClientVars m)
newClientVars heartbeatCheckIvl requestExpiry = ClientVars
    <$> newTVarIO False
    <*> SM.newIO
    <*> pure heartbeatCheckIvl
    <*> (ClientInfo <$> getBackchannelId <*> pure requestExpiry)

-- | Runs a listener for responses from workers, which dispatches to
-- callbacks registered with 'jobQueueRequest'.  It also runs
-- 'checkHeartbeats', to ensure that some server periodically checks
-- the worker heartbeats.
--
-- This function should be run in its own thread, as it never returns
-- (the return type is @void@).
jobQueueClient
    :: MonadConnect m
    => ClientVars m
    -> RedisInfo
    -> m void
jobQueueClient cvs r = do
    let checker = checkHeartbeats r (clientHeartbeatCheckIvl cvs)
        subscribe = subscribeToResponses r (clientInfo cvs) (clientSubscribed cvs)
    control $ \restore -> withAsync (restore checker) $ \_ ->
        restore $ subscribe $ \requestId -> do
            -- Lookup the handler before fetching / deleting the response,
            -- as the message may get delivered to multiple clients.
            let lookupAndRemove handler = return (handler, Remove)
            mhandler <- atomically $
                SM.focus lookupAndRemove requestId (clientDispatch cvs)
            case mhandler of
                -- TODO: Is a mere warning sufficient? Perhaps we need
                -- guarantees about uniqueness of back channel, and number
                -- of times a response is yielded, in order to have
                -- guarantees about delivery.
                Nothing -> $logWarn $
                    "Couldn't find handler to deal with response to " <>
                    tshow requestId
                Just handler -> do
                    response <- readResponse r requestId
                    handler response

-- | Once a 'jobQueueClient' has been run with the 'ClientVars' value,
-- this function can be used to make requests and block on their
-- response.  It's up to the user of this and 'jobQueueWorker' to
-- ensure that the types of @payload@ match up, and that the
-- 'ByteString' responses are encoded as expected.
jobQueueRequest
    :: (MonadCommand m, MonadLogger m, Binary payload)
    => ClientVars m
    -> RedisInfo
    -> Vector payload
    -> m ByteString
jobQueueRequest cvs r request = do
    resultVar <- newEmptyMVar
    jobQueueRequest' cvs r request $ putMVar resultVar
    takeMVar resultVar

-- | This is a non-blocking version of jobQueueRequest.  When the
-- response comes back, the provided callback is invoked.  One thing
-- to note is that exceptions thrown by the callback do not get
-- rethrown.  Instead, they're printed, due to jobQueueClient using
-- 'FP.Redis.withSubscriptionsWrapped'.
--
-- This command does block on the request being enqueued.  First, it
-- blocks on 'clientSubscribed', then it may also need to wait for the
-- Redis server to become available.
jobQueueRequest'
    :: (MonadCommand m, MonadLogger m, Binary request)
    => ClientVars m
    -> RedisInfo
    -> Vector request
    -> (ByteString -> m ())
    -> m ()
jobQueueRequest' cvs r request handler = do
    -- TODO: Does it make sense to block on subscription like this?
    -- Perhaps instead servers should block even accepting requests
    -- until it's subscribed.
    atomically $ check =<< readTVar (clientSubscribed cvs)
    (k, mresponse) <- pushRequest r (clientInfo cvs) (toStrict (encode request))
    case mresponse of
        Nothing -> atomically $ SM.focus addOrExtend k (clientDispatch cvs)
        Just response -> void $ fork $ runHandler response
  where
    addOrExtend Nothing = return ((), Replace runHandler)
    addOrExtend (Just old) = return ((), Replace (\x -> old x >> runHandler x))
    runHandler response =
        catchAny (handler response) $ \ex ->
            $logError $ "jobQueueRequest' callbackHandler: " ++ tshow ex

getBackchannelId :: IO BackchannelId
getBackchannelId = BackchannelId <$> getHostAndProcessId

getWorkerId :: MonadCommand m => RedisInfo -> m WorkerId
getWorkerId redis = getUnusedWorkerId redis =<< liftIO getHostAndProcessId

getHostAndProcessId :: IO ByteString
getHostAndProcessId = do
    hostName <- getHostName
    pid <- getProcessID
    return $ encodeUtf8 $ pack $ hostName <> ":" <> show pid
