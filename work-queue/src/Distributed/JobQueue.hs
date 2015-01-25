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
import           Control.Concurrent.Async.Lifted (withAsync)
import           Control.Concurrent.Lifted (fork, threadDelay)
import           Control.Concurrent.STM (check)
import           Control.Monad.Logger (logWarn)
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
jobQueueWorker config redis queue toResult = do
    wid <- getWorkerId redis
    let worker = WorkerInfo redis wid
    _ <- fork $ forever $ do
        sendHeartbeat worker
        void $ threadDelay (workerHeartbeat config)
    forever $ do
        (requestId, request) <- popRequest worker
        subresults <- mapQueue queue (decode (fromStrict request))
        sendResponse worker requestId =<< toResult subresults

data ClientVars m = ClientVars
    { clientSubscribed :: TVar Bool
    , clientDispatch :: SM.Map RequestId (ByteString -> m ())
    , clientBackchannel :: BackchannelId
    , clientHeartbeatCheckIvl :: Seconds
    }

newClientVars :: Seconds -> IO (ClientVars m)
newClientVars heartbeatCheckIvl = ClientVars
    <$> newTVarIO False
    <*> SM.newIO
    <*> getBackchannelId
    <*> pure heartbeatCheckIvl

jobQueueClient
    :: MonadConnect m
    => ClientVars m
    -> RedisInfo
    -> m void
jobQueueClient cvs redis = do
    let client = ClientInfo redis (clientBackchannel cvs)
        checker = periodicallyCheckHeartbeats redis (clientHeartbeatCheckIvl cvs)
    withAsync checker $ \_ ->
        subscribeToResponses client (clientSubscribed cvs) $ \requestId -> do
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
                    response <- readResponse redis requestId
                    handler response

jobQueueRequest
    :: (MonadCommand m, MonadThrow m, Binary request)
    => ClientVars m
    -> RedisInfo
    -> Vector request
    -> m ByteString
jobQueueRequest cvs redis request = do
    resultVar <- newEmptyMVar
    jobQueueRequest' cvs redis request $ putMVar resultVar
    takeMVar resultVar

-- | This is a non-blocking version of jobQueueRequest.  When the
-- response comes back, the provided callback is invoked.  One thing
-- to note is that exceptions thrown by the callback do not get
-- rethrown.  Instead, they're printed, due to jobQueueClient using
-- 'FP.Redis.withSubscriptionsWrapped'.
jobQueueRequest'
    :: (MonadCommand m, MonadThrow m, Binary request)
    => ClientVars m
    -> RedisInfo
    -> Vector request
    -> (ByteString -> m ())
    -> m ()
jobQueueRequest' cvs redis request handler = do
    let client = ClientInfo redis (clientBackchannel cvs)
    -- TODO: Does it make sense to block on subscription like this?
    -- Perhaps instead servers should block even accepting requests
    -- until it's subscribed.
    atomically $ check =<< readTVar (clientSubscribed cvs)
    k <- pushRequest client (toStrict (encode request))
    atomically $ SM.insert handler k (clientDispatch cvs)

getBackchannelId :: IO BackchannelId
getBackchannelId = BackchannelId <$> getHostAndProcessId

getWorkerId :: MonadCommand m => RedisInfo -> m WorkerId
getWorkerId redis = getUnusedWorkerId redis =<< liftIO getHostAndProcessId

getHostAndProcessId :: IO ByteString
getHostAndProcessId = do
    hostName <- getHostName
    pid <- getProcessID
    return $ encodeUtf8 $ pack $ hostName <> ":" <> show pid
