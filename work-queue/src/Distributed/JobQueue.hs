{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE DeriveDataTypeable #-}
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
import           Control.Concurrent.Lifted (fork, threadDelay)
import           Control.Concurrent.STM (check)
import           Control.Monad.Logger (MonadLogger, logWarn, logError)
import           Control.Monad.Trans.Control (control)
import           Data.Binary (Binary, decode, encode)
import           Data.WorkQueue
import           Distributed.RedisQueue
import           FP.Redis.Types (MonadConnect, MonadCommand, Seconds(..))
import           Focus (Decision(Remove))
import           Network.HostName (getHostName)
import qualified STMContainers.Map as SM
import           System.Posix.Process (getProcessID)

data WorkerConfig = WorkerConfig
    { workerHeartbeat :: Int -- ^ Heartbeat frequency, in microseconds.
    , workerResponseDataExpiry :: Seconds
    }

-- | This is intended for use as the body of the @inner@ function of a
-- master server.  It takes requests encoded as @Vector payload@ and
-- enqueues each item to a 'WorkQueue'.
jobQueueWorker
    :: (MonadConnect m, Binary payload)
    => WorkerConfig
    -> RedisInfo
    -> WorkQueue payload result
    -> (Vector result -> m ByteString)
    -> m void
jobQueueWorker config r queue toResult = do
    wid <- getWorkerId r
    let worker = WorkerInfo wid (workerResponseDataExpiry config)
    _ <- fork $ forever $ do
        sendHeartbeat r worker
        void $ threadDelay (workerHeartbeat config)
    forever $ do
        (requestId, bid, mrequest) <- popRequest r worker
        case mrequest of
            Nothing -> $logError $ "Request " ++ tshow requestId ++ " expired."
            Just request -> do
                subresults <- mapQueue queue (decode (fromStrict request))
                sendResponse r worker requestId bid =<< toResult subresults

data ClientVars m = ClientVars
    { clientSubscribed :: TVar Bool
    , clientDispatch :: SM.Map RequestId (ByteString -> m ())
    , clientHeartbeatCheckIvl :: Seconds
    , clientInfo :: ClientInfo
    }

newClientVars :: Seconds -> Seconds -> IO (ClientVars m)
newClientVars heartbeatCheckIvl requestExpiry = ClientVars
    <$> newTVarIO False
    <*> SM.newIO
    <*> pure heartbeatCheckIvl
    <*> (ClientInfo <$> getBackchannelId <*> pure requestExpiry)

jobQueueClient
    :: MonadConnect m
    => ClientVars m
    -> RedisInfo
    -> m void
jobQueueClient cvs r = do
    let checker = periodicallyCheckHeartbeats r (clientHeartbeatCheckIvl cvs)
        subscribe = subscribeToResponses r (clientInfo cvs) (clientSubscribed cvs)
    control $ \restore -> withAsync (restore checker) $ \_ ->
        restore $ subscribe $ \requestId -> do
            -- Lookup the handler before fetching / deleting the response,
            -- as the message may get delivered to multiple clients.
            let lookupAndRemove r = return (r, Remove)
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

jobQueueRequest
    :: (MonadCommand m, MonadLogger m, Binary request)
    => ClientVars m
    -> RedisInfo
    -> Vector request
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
        Nothing ->
            atomically $ SM.insert handler k (clientDispatch cvs)
        Just response ->
            void $ fork $ catchAny (handler response) $ \ex ->
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
