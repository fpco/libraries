{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE ViewPatterns #-}

-- This module provides a job-queue with heartbeat checking.
module Distributed.JobQueue.Worker
    (jobWorker, reenqueueWork) where

import ClassyPrelude
import Control.Monad.Logger
import Data.Bits (xor)
import Data.List.NonEmpty (NonEmpty((:|)))
import Data.Proxy
import Data.Serialize (encode)
import Data.Streaming.NetworkMessage (Sendable)
import Data.Time.Clock.POSIX (getPOSIXTime)
import Data.TypeFingerprint (typeFingerprint)
import Data.Typeable (typeOf)
import Data.UUID as UUID
import Data.UUID.V4 as UUID
import Distributed.Heartbeat (sendHeartbeats)
import Distributed.JobQueue.Internal
import Distributed.Redis
import Distributed.Types
import FP.Redis
import FP.ThreadFileLogger
import System.Posix.Process (getProcessID)

-- | Implements a compute node which responds to requests and never
-- returns.
jobWorker
    :: (MonadConnect m, Sendable request, Sendable response)
    => JobQueueConfig
    -> (Redis -> RequestId -> request -> m response)
    -- ^ This function is run by the worker, for every request it
    -- receives.
    -> m ()
jobWorker config@JobQueueConfig {..} f = do
    wid <- liftIO getWorkerId
    let withTag = withLogTag (LogTag ("worker-" ++ tshow (unWorkerId wid)))
    withTag $ withRedis jqcRedisConfig $ \r -> do
        setRedisSchemaVersion r
        heartbeatSentVar <- newEmptyMVar
        let heartbeatThread =
                sendHeartbeats jqcHeartbeatConfig r wid heartbeatSentVar
            workerThread = do
                $logDebug "Waiting for initial heartbeat send"
                takeMVarE heartbeatSentVar $ InternalJobQueueException $ concat
                    [ "Heartbeat checker thread died before sending initial heartbeat (cancelling computation).  "
                    , "This usually indicates that we lost the connection to redis and couldn't reconnect."
                    ]
                $logDebug "Initial heartbeat sent"
                (notify, unsub) <- subscribeToNotify r (requestChannel r)
                jobWorkerThread config r wid notify (f r) `finally` liftIO unsub
        heartbeatThread `raceLifted` workerThread

-- FIXME: bring back cancellation

jobWorkerThread
    :: forall request response m void.
       (MonadConnect m, Sendable request, Sendable response)
    => JobQueueConfig
    -> Redis
    -> WorkerId
    -> MVar ()
    -> (RequestId -> request -> m response)
    -> m void
jobWorkerThread JobQueueConfig {..} r wid notify f = forever $ do
    ereqbs <- try $ run r $ rpoplpush (requestsKey r) (LKey (activeKey r wid))
    case ereqbs of
        Left ex@(CommandException (isPrefixOf "WRONGTYPE" -> True)) -> do
            -- While it's rather unlikely that this happens without it
            -- being a HeartbeatFailure, check anyway, so that we
            -- don't mask some unrelated issue.
            val <- run r $ get (VKey (activeKey r wid))
            if val /= Just "HeartbeatFailure"
                then liftIO $ throwIO ex
                else do
                    $logInfo $ tshow wid <> " recovering from heartbeat failure"
                    run_ r $ del (activeKey r wid :| [])
        Left ex -> liftIO $ throwIO ex
        Right Nothing -> return ()
        Right (Just bs) -> do
            let rid = RequestId bs
            eres <- try $ do
                mreq <- receiveRequest r wid rid (Proxy :: Proxy response)
                case mreq of
                    Nothing -> return (Left (RequestMissingException rid))
                    Just req -> Right <$> f rid req
            let mres = case eres of
                    Left (fromException -> Just ReenqueueWork) -> Nothing
                    Left ex -> Just (Left (wrapException ex))
                    Right (Left ex) -> Just (Left ex)
                    Right (Right x) -> Just (Right x)
            case mres of
                Just res -> do
                    sendResponse r jqcResponseExpiry wid rid (encode res)
                    addRequestEvent r rid (RequestWorkFinished wid)
                Nothing -> do

                    addRequestEvent r rid (RequestWorkReenqueuedByWorker wid)
    $logDebug "Waiting for request notification"
    takeMVarE notify NoLongerWaitingForRequest
    $logDebug "Got notified of an available request"

--TODO: decouple protocol checking concern from job-queue

receiveRequest
    :: forall request response m.
       (MonadConnect m, Sendable request, Sendable response)
    => Redis
    -> WorkerId
    -> RequestId
    -> Proxy response
    -> m (Maybe request)
receiveRequest redis wid rid Proxy = do
    mreq <- run redis $ get (requestDataKey redis rid)
    case mreq of
        Nothing -> return Nothing
        Just req -> do
            addRequestEvent redis rid (RequestWorkStarted wid)
            let requestTypeFingerprint = typeFingerprint (Proxy :: Proxy request)
                responseTypeFingerprint = typeFingerprint (Proxy :: Proxy response)
            JobRequest{..} <- decodeOrThrow "jobWorker" req
            when (jrRequestTypeFingerprint /= requestTypeFingerprint ||
                  jrResponseTypeFingerprint /= responseTypeFingerprint) $ do
                liftIO $ throwIO TypeMismatch
                    { expectedResponseTypeFingerprint = responseTypeFingerprint
                    , actualResponseTypeFingerprint = jrResponseTypeFingerprint
                    , expectedRequestTypeFingerprint = requestTypeFingerprint
                    , actualRequestTypeFingerprint = jrRequestTypeFingerprint
                    }
            when (jrSchema /= redisSchemaVersion) $ do
                liftIO $ throwIO MismatchedRequestRedisSchemaVersion
                    { expectedRequestRedisSchemaVersion = redisSchemaVersion
                    , actualRequestRedisSchemaVersion = jrSchema
                    , schemaMismatchRequestId = rid
                    }
            fmap Just $ decodeOrThrow "jobWorker" jrBody

-- | Send a response for a particular request. Once the response is
-- successfully sent, this also removes the request data, as it's no
-- longer needed.
sendResponse
    :: MonadConnect m
    => Redis
    -> Seconds
    -> WorkerId
    -> RequestId
    -> ByteString
    -> m ()
sendResponse r expiry wid k x = do
    -- Store the response data, and notify the client that it's ready.
    run_ r $ set (responseDataKey r k) x [EX expiry]
    run_ r $ publish (responseChannel r) (unRequestId k)
    -- Remove the RequestId associated with this response, from the
    -- list of in-progress requests.
    let ak = LKey (activeKey r wid)
    removed <- try $ run r $ lrem ak 1 (unRequestId k)
    case removed :: Either RedisException Int64 of
        Right 1 -> do
            -- Remove the request data, as it's no longer needed.  We don't
            -- check if the removal succeeds, as this may not be the first
            -- time a response is sent for the request.  See the error message
            -- above.
            run_ r $ del (unVKey (requestDataKey r k) :| [])
        _ -> $logWarn $
            tshow k <>
            " isn't a member of active queue (" <>
            tshow ak <>
            "), likely indicating that a heartbeat failure happened, causing\
            \ it to be erroneously re-enqueued.  This doesn't affect\
            \ correctness, but could mean that redundant work is performed."
    -- Store when the response was stored.
    responseTime <- liftIO getPOSIXTime
    setRedisTime r (responseTimeKey r k) responseTime [EX expiry]

-- * Re-enqueuing work

data ReenqueueWork = ReenqueueWork
    deriving (Eq, Show, Typeable)

instance Exception ReenqueueWork

-- | Stop working on this item, and re-enqueue it for some other worker
-- to handle.
reenqueueWork :: MonadIO m => m ()
reenqueueWork = liftIO $ throwIO ReenqueueWork

-- * Utilities

wrapException :: SomeException -> DistributedException
wrapException ex =
    case ex of
        (fromException -> Just err) -> err
        (fromException -> Just err) -> NetworkMessageException err
        _ -> OtherException (tshow (typeOf ex)) (tshow ex)

getWorkerId :: IO WorkerId
getWorkerId = do
    pid <- getProcessID
    (w1, w2, w3, w4) <- toWords <$> UUID.nextRandom
    let w1' = w1 `xor` fromIntegral pid
    return $ WorkerId $ UUID.toASCIIBytes $ UUID.fromWords w1' w2 w3 w4
