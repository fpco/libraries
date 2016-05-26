{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveFoldable #-}
{-# LANGUAGE DeriveTraversable #-}

-- This module provides a job-queue with heartbeat checking.
module Distributed.JobQueue.Worker
    ( JobQueueConfig(..)
    , defaultJobQueueConfig
    , jobWorker
    , Reenqueue(..)
    ) where

import ClassyPrelude
import Control.Concurrent (threadDelay)
import Control.Monad.Logger
import Data.List.NonEmpty (NonEmpty((:|)))
import Data.Proxy
import Data.Streaming.NetworkMessage (Sendable)
import Data.Time.Clock.POSIX (getPOSIXTime)
import Data.TypeFingerprint (typeFingerprint)
import Data.Typeable (typeOf)
import Data.UUID as UUID
import Data.UUID.V4 as UUID
import Distributed.Heartbeat
import Distributed.JobQueue.Internal
import Distributed.Redis
import Distributed.Types
import FP.Redis
import FP.ThreadFileLogger
import qualified Control.Concurrent.Async.Lifted.Safe as Async
import Data.Serialize (encode, Serialize)

-- | Implements a compute node which responds to requests and never
-- returns.
--
-- REVIEW: It uses an atomic redis operation (rpop and lpush) to get stuff from redis,
-- it's not entirely clear who wins when there is contention.
-- REVIEW TODO: Here there is some ambiguity on when we "save" a worker: for example
-- some redis operations are 'try'd, some arent. I propose to never cach exceptions
-- apart in a top-level handler, and when we run the user provided function.
jobWorker :: forall m request response void.
       (MonadConnect m, Sendable request, Sendable response)
    => JobQueueConfig
    -> (Redis -> RequestId -> request -> m (Reenqueue response))
    -- ^ This function is run by the worker, for every request it
    -- receives.
    -> m void
jobWorker config@JobQueueConfig {..} f = do
    wid <- liftIO getWorkerId
    let withTag = withLogTag (LogTag ("worker-" ++ tshow (unWorkerId wid)))
    withTag $ withRedis jqcRedisConfig $ \r -> do
        $logInfo "Starting heartbeats"
        withHeartbeats jqcHeartbeatConfig r wid $ do
            $logInfo "Initial heartbeat sent, starting worker"
            withSubscribedNotifyChannel
                (rcConnectInfo jqcRedisConfig)
                jqcRequestNotificationFailsafeTimeout
                (requestChannel r)
                (\waitForReq -> do
                    -- writeIORef everConnectedRef True
                    jobWorkerThread config r wid waitForReq (f r))

data Reenqueue a
    = DontReenqueue !a
    | Reenqueue
    deriving (Eq, Show, Generic, Typeable, Functor, Foldable, Traversable)
instance (Serialize a) => Serialize (Reenqueue a)

data WorkerResult response
    = RequestGotCancelled
    | RequestShouldBeReenqueued
    | GotResponse (Either DistributedException response)

jobWorkerThread :: forall request response m void.
       (MonadConnect m, Sendable request, Sendable response)
    => JobQueueConfig
    -> Redis
    -> WorkerId
    -> m ()
    -- ^ This action, when executed, blocks until we think a new response is present.
    -> (RequestId -> request -> m (Reenqueue response))
    -> m void
jobWorkerThread JobQueueConfig{..} r wid waitForNewRequest f = forever $ do
    mbRidbs <- run r (rpoplpush (requestsKey r) (activeKey r wid))
    forM_ mbRidbs $ \ridBs -> do
        let rid = RequestId ridBs
        $logInfo (workerMsg ("Receiving request " ++ tshow rid))
        mbReqBs <- receiveRequest r wid rid
        case mbReqBs of
            Nothing -> do
                $logWarn (workerMsg ("Failed getting the content of request " ++ tshow rid ++ ". This can happen if the requset is cancelled."))
                -- In this case the best we can do is try to re-enqueue: we need to clear the current active key anyway.
                reenqueueRequest r wid rid
            Just reqBs -> do
                $logInfo (workerMsg ("Got contents of request " ++ tshow rid))
                mbResp :: WorkerResult response <- case checkRequest (Proxy :: Proxy response) rid reqBs of
                    Left err -> return (GotResponse (Left err))
                    Right req -> do
                        $logDebug (workerMsg ("Starting to work on request " ++ tshow ridBs))
                        cancelledOrResp :: Either () (Either SomeException (Reenqueue response)) <-
                            Async.race (watchForCancel r rid jqcCancelCheckIvl) (tryAny (f rid req))
                        case cancelledOrResp of
                            Left () -> return RequestGotCancelled
                            Right (Left err) -> return (GotResponse (Left (wrapException err)))
                            Right (Right Reenqueue) -> return RequestShouldBeReenqueued
                            Right (Right (DontReenqueue res)) -> return (GotResponse (Right res))
                let gotX msg = workerMsg ("Request " ++ tshow rid ++ " got " ++ msg)
                case mbResp of
                    RequestGotCancelled -> $logInfo (gotX "cancel")
                    RequestShouldBeReenqueued -> do
                        $logInfo (gotX "reenqueue")
                        reenqueueRequest r wid rid
                        addRequestEvent r rid (RequestWorkReenqueuedByWorker wid)
                    GotResponse res -> do
                        case res of
                            Left err -> $logWarn (gotX ("exception: " ++ tshow err))
                            Right _ -> $logInfo (gotX ("Request " ++ tshow rid ++ " got response"))
                        sendResponse r jqcResponseExpiry wid rid (encode res)
                        addRequestEvent r rid (RequestWorkFinished wid)
    -- Wait for notification only if we've already consumed all the available requests
    -- -- the notifications only tell us about _new_ requests, nothing about how many
    -- there might be backed up in the queue.
    unless (isJust mbRidbs) $ do
        $logDebug (workerMsg "Waiting for request notification")
        waitForNewRequest
        $logDebug (workerMsg "Got notified of an available request")
  where
    workerMsg msg = tshow (unWorkerId wid) ++ ": " ++ msg

--TODO: decouple protocol checking concern from job-queue

receiveRequest ::
       (MonadConnect m)
    => Redis
    -> WorkerId
    -> RequestId
    -> m (Maybe ByteString)
receiveRequest redis wid rid = do
    mbReq <- run redis $ get (requestDataKey redis rid)
    forM_ mbReq (\_ -> addRequestEvent redis rid (RequestWorkStarted wid))
    return mbReq

checkRequest :: forall request response.
     (Sendable request, Sendable response)
  => Proxy response -> RequestId -> ByteString -> Either DistributedException request
checkRequest _proxy rid req = do
    let requestTypeFingerprint = typeFingerprint (Proxy :: Proxy request)
        responseTypeFingerprint = typeFingerprint (Proxy :: Proxy response)
    JobRequest{..} <- decodeOrErr "jobWorker" req
    when (jrRequestTypeFingerprint /= requestTypeFingerprint ||
          jrResponseTypeFingerprint /= responseTypeFingerprint) $ do
        Left TypeMismatch
            { expectedResponseTypeFingerprint = responseTypeFingerprint
            , actualResponseTypeFingerprint = jrResponseTypeFingerprint
            , expectedRequestTypeFingerprint = requestTypeFingerprint
            , actualRequestTypeFingerprint = jrRequestTypeFingerprint
            }
    when (jrSchema /= redisSchemaVersion) $ do
        Left MismatchedRequestRedisSchemaVersion
            { expectedRequestRedisSchemaVersion = redisSchemaVersion
            , actualRequestRedisSchemaVersion = jrSchema
            , schemaMismatchRequestId = rid
            }
    decodeOrErr "jobWorker" jrBody

reenqueueRequest ::
       (MonadConnect m)
    => Redis -> WorkerId -> RequestId -> m ()
reenqueueRequest r (WorkerId wid) (RequestId rid) = do
    mbRid <- run r (rpoplpush (activeKey r (WorkerId wid)) (requestsKey r))
    case mbRid of
        Nothing -> do
            $logWarn ("We expected " ++ tshow rid ++ " to be in worker " ++ tshow wid ++ " active key, but instead we got nothing. This means that the worker has been detected as dead by a client.")
        Just rid' ->
            unless (rid == rid') $
                throwM (InternalJobQueueException ("We expected " ++ tshow rid ++ " to be in worker " ++ tshow wid ++ " active key, but instead we got " ++ tshow rid'))

-- | Send a response for a particular request. Once the response is
-- successfully sent, this also removes the request data, as it's no
-- longer needed.
-- REVIEW TODO: These "the request is done" tasks are not atomic. Is this a problem?
sendResponse :: 
       MonadConnect m
    => Redis
    -> Seconds
    -> WorkerId
    -> RequestId
    -> ByteString
    -> m ()
sendResponse r expiry wid k x = do
    -- Store the response data, and notify the client that it's ready.
    run_ r (set (responseDataKey r k) x [EX expiry])
    run_ r (publish (responseChannel r) (unRequestId k))
    -- Remove the RequestId associated with this response, from the
    -- list of in-progress requests.
    --
    -- Note that we expect this list to either be empty or to contain
    -- exactly this request id.
    let ak = activeKey r wid
    removed <- run r (lrem ak 1 (unRequestId k))
    if  | removed == 1 ->
            return ()
        | removed == 0 ->
            $logWarn $
                tshow k <>
                " isn't a member of active queue (" <>
                tshow ak <>
                "), likely indicating that a heartbeat failure happened, causing it to be erroneously re-enqueued.  This doesn't affect correctness, but could mean that redundant work is performed."
        | True ->
            throwM (InternalJobQueueException ("Expecting 0 or 1 removals from active key " ++ tshow ak ++ ", but got " ++ tshow removed))
    -- Check no active requests are present. This is just for safety.
    remaining <- run r (llen ak)
    unless (remaining == 0) $
        throwM (InternalJobQueueException ("Expecting 0 active requests for active key " ++ tshow ak ++ ", but got " ++ tshow remaining))
    -- Remove the request data, as it's no longer needed.  We don't
    -- check if the removal succeeds, since there might be contention
    -- in processing the request when a worker is detected to be dead
    -- when it shouldn't be.
    run_ r (del (unVKey (requestDataKey r k) :| []))
    -- Also remove from the requestsKey: it might be that the request has been
    -- erroneously reenqueued.
    removed' <- run r (lrem (requestsKey r) 0 (unRequestId k))
    when (removed' > 0) $
        $logWarn ("Expecting no request " <> tshow k <> " in requestsKey, but found " ++ tshow removed ++ ". This can happen with spurious heartbeat failures.")
    -- Store when the response was stored.
    responseTime <- liftIO getPOSIXTime
    setRedisTime r (responseTimeKey r k) responseTime [EX expiry]

watchForCancel :: forall m. (MonadConnect m) => Redis -> RequestId -> Seconds -> m ()
watchForCancel r k ivl = loop
  where
    loop :: m ()
    loop = do
        $logDebug "Checking for cancel"
        mres <- run r (get (cancelKey r k))
        case mres of
            Just res
                | res == cancelValue -> return ()
                | otherwise -> liftIO $ throwIO $ InternalJobQueueException
                    "Didn't get expected value at cancelKey."
            Nothing -> do
                $logDebug "No cancel, waiting"
                liftIO $ threadDelay (1000 * 1000 * fromIntegral (unSeconds ivl))
                loop

-- * Utilities

wrapException :: SomeException -> DistributedException
wrapException ex =
    case ex of
        (fromException -> Just err) -> err
        (fromException -> Just err) -> NetworkMessageException err
        _ -> OtherException (tshow (typeOf ex)) (tshow ex)

getWorkerId :: IO WorkerId
getWorkerId = do
    -- REVIEW TODO: Why do we do this process id?
    {-
    pid <- getProcessID
    (w1, w2, w3, w4) <- toWords <$> UUID.nextRandom
    let w1' = w1 `xor` fromIntegral pid
    -}
    WorkerId . UUID.toASCIIBytes <$> UUID.nextRandom
