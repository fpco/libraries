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
{-|
Module: Distributed.JobQueue.Worker
Description: Worker that fetches and performs jobs from a job queue.

The heart of this module is 'jobWorker'.  It spawns a worker that
continouusly pulls jobs from the queue, performing a given function on
them.

Sending of heartbeats, as well as watching for job cancellation, are
handled automatically.
-}

-- This module provides a job-queue with heartbeat checking.
module Distributed.JobQueue.Worker
    ( JobQueueConfig(..)
    , defaultJobQueueConfig
    , jobWorker
    , jobWorkerWithHeartbeats
    , Reenqueue(..)
    , jobWorkerWait
    ) where

import ClassyPrelude
import Control.Concurrent (threadDelay)
import Control.Monad.Logger
import Data.List.NonEmpty (NonEmpty((:|)))
import Data.Proxy
import Data.Time.Clock.POSIX (getPOSIXTime)
import Data.Streaming.NetworkMessage
import Data.Store.TypeHash
import Distributed.Heartbeat
import Distributed.JobQueue.Internal
import Distributed.Redis
import Distributed.Types
import FP.Redis
import qualified Control.Concurrent.Mesosync.Lifted.Safe as Async
import Data.Store (encode, Store)
import Data.Typeable (typeOf)

-- | Implements a compute node which responds to requests and never
-- returns.
--
-- This function will continuously monitor the job queue, picking one
-- job at a time for execution.
--
-- For each job it executes, it will perform the provided function,
-- and send the result back.
--
-- While performing a job, it will repeatedly check if the job has
-- been canceled, in which case it will abort the job and pick the
-- next.
--
-- Heartbeats are sent periodically to signal that the worker is alive
-- and connected to the job queue.
jobWorker :: forall m request response void.
       (MonadConnect m, Sendable request, Sendable response)
    => JobQueueConfig
    -> Redis
    -> Heartbeating
    -> (RequestId -> request -> m (Reenqueue response))
    -- ^ This function is run by the worker, for every request it
    -- receives.
    -> m void
jobWorker config redis hb f = jobWorkerWait config redis hb (return ()) f

jobWorkerWithHeartbeats ::
       (MonadConnect m, Sendable request, Sendable response)
    => JobQueueConfig
    -> Redis
    -> (RequestId -> request -> m (Reenqueue response))
    -- ^ This function is run by the worker, for every request it
    -- receives.
    -> m void
jobWorkerWithHeartbeats config redis f = do
    withHeartbeats (jqcHeartbeatConfig config) redis (\hb -> jobWorker config redis hb f)

-- | Exactly like 'jobWorker', but also allows to delay the loop using the second argument.
jobWorkerWait :: forall m request response void.
       (MonadConnect m, Sendable request, Sendable response)
    => JobQueueConfig
    -> Redis
    -> Heartbeating
    -> m ()
    -- ^ The worker has a loop that keeps popping requests. This action
    -- will be called at the beginning of every loop iteration, and can
    -- be useful to "limit" the loop (e.g. have some timer in between,
    -- or wait for the worker to be free of processing requests if we
    -- are doing something else too).
    -> (RequestId -> request -> m (Reenqueue response))
    -- ^ This function is run by the worker, for every request it
    -- receives.
    -> m void
jobWorkerWait config@JobQueueConfig{..} r (heartbeatingWorkerId -> wid) wait f = do
    withSubscribedNotifyChannel
        (rcConnectInfo jqcRedisConfig)
        jqcRequestNotificationFailsafeTimeout
        (requestChannel r)
        (\waitForReq -> do
            -- writeIORef everConnectedRef True
            jobWorkerThread config r wid waitForReq f wait)

-- | A specialised 'Maybe' to indicate the result of a job: If the job
-- successfull produced a result @a@, it will yield it via
-- @DontReenqueue a@.  If it was not successful, it returns
-- @Reenqueue@ to signal that the job should be run again.
data Reenqueue a
    = DontReenqueue !a
    | Reenqueue
    deriving (Eq, Show, Generic, Typeable, Functor, Foldable, Traversable)
instance (Store a) => Store (Reenqueue a)

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
    -> m ()
    -> m void
jobWorkerThread JobQueueConfig{..} r wid waitForNewRequest f wait = forever $ do
    wait
    mbRidbs <- run r (rpoplpush (requestsKey r) (activeKey r wid))
    forM_ mbRidbs $ \ridBs -> do
        let rid = RequestId ridBs
        $logInfo (workerMsg ("Receiving request " ++ tshow rid))
        mbReqBs <- receiveRequest r wid rid
        case mbReqBs of
            Nothing -> do
                $logInfo (workerMsg ("Failed getting the content of request " ++ tshow rid ++ ". This can happen if the request is expired or cancelled. The request will be dropped."))
                -- In this case the best we can do is to just drop the request id we got. This is mostly to
                -- mitigate against the case of a stale request id being reenqueued forever. This can
                -- happen, for example, if a worker dies at the wrong moment. If the request data has expired,
                -- we will never be able to process the request anyway, so we might as well drop it.
                checkPoppedActiveKey wid rid =<< run r (rpop (activeKey r wid))
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
    let requestTypeHash = typeHash (Proxy :: Proxy request)
        responseTypeHash = typeHash (Proxy :: Proxy response)
    JobRequest{..} <- decodeOrErr "jobWorker" req
    when (jrRequestTypeHash /= requestTypeHash ||
          jrResponseTypeHash /= responseTypeHash) $ do
        Left TypeMismatch
            { expectedResponseTypeHash = responseTypeHash
            , actualResponseTypeHash = jrResponseTypeHash
            , expectedRequestTypeHash = requestTypeHash
            , actualRequestTypeHash = jrRequestTypeHash
            }
    when (jrSchema /= redisSchemaVersion) $ do
        Left MismatchedRequestRedisSchemaVersion
            { expectedRequestRedisSchemaVersion = redisSchemaVersion
            , actualRequestRedisSchemaVersion = jrSchema
            , schemaMismatchRequestId = rid
            }
    decodeOrErr "jobWorker" jrBody

checkPoppedActiveKey :: (MonadConnect m) => WorkerId -> RequestId -> Maybe ByteString -> m ()
checkPoppedActiveKey (WorkerId wid) (RequestId rid) mbRid = do
    case mbRid of
        Nothing -> do
            $logWarn ("We expected " ++ tshow rid ++ " to be in worker " ++ tshow wid ++ " active key, but instead we got nothing. This means that the worker has been detected as dead by a client.")
        Just rid' ->
            unless (rid == rid') $
                throwM (InternalJobQueueException ("We expected " ++ tshow rid ++ " to be in worker " ++ tshow wid ++ " active key, but instead we got " ++ tshow rid'))

reenqueueRequest ::
       (MonadConnect m)
    => Redis -> WorkerId -> RequestId -> m ()
reenqueueRequest r (WorkerId wid) rid = do
    checkPoppedActiveKey (WorkerId wid) rid =<< run r (rpoplpush (activeKey r (WorkerId wid)) (requestsKey r))
    addRequestEvent r rid (RequestWorkReenqueuedByWorker (WorkerId wid))

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
    void $ setRedisTime r (responseTimeKey r k) responseTime [EX expiry]

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
