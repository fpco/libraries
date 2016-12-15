{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE LambdaCase #-}
{-|
Module: Distributed.JobQueue.Client
Description: Submit requests to a job queue, and collect their results.

This module implements the client side of a job queue: submitting
results, cancelling them, and collecting their results.

The function 'withJobClient' can be used to create a 'JobClient'.  A
'JobClient' is needed to submit requests to the job queue, and to get
results of requests.  Additionally, a client will check the heartbeats
of all connected worker nodes as long as it is running.
-}
module Distributed.JobQueue.Client
    ( -- * Types and configuration
      JobQueueConfig(..)
    , defaultJobQueueConfig
    , JobClient (..) -- TODO: should we hide the constructor? It's not intended to create a JobClient manually.
      -- * Provide a 'JobClient'
    , withJobClient
      -- * Submitting requests and retrieving results
    , submitRequest
    , submitRequestUrgent
    , waitForResponse
    , waitForResponse_
    , waitForResponses
    , checkForResponse
    , checkForResponses
      -- * Cancel requests
    , cancelRequest
      -- * Explicitly re-enqueue requests from dead workers
    , reenqueueRequests
      -- * Auxilliary functions
    , uniqueRequestId
    , requestHashRequestId
    ) where

import           ClassyPrelude
import           Control.Concurrent.STM (retry, orElse)
import           Control.Monad.Logger.JSON.Extra
import           Data.List.NonEmpty (NonEmpty((:|)))
import qualified Data.List.NonEmpty as NE
import qualified Data.Store as S
import           Data.Streaming.NetworkMessage (Sendable)
import           Data.Store.TypeHash
import           Data.Time.Clock.POSIX (getPOSIXTime)
import           Data.Typeable (Proxy(..))
import           Distributed.Heartbeat (checkHeartbeats, performHeartbeatCheck)
import qualified Control.Concurrent.Async.Lifted.Safe as Async
import           Distributed.JobQueue.Internal
import           Distributed.JobQueue.StaleKeys
import           Distributed.Redis
import           Distributed.Types
import           FP.Redis
import           FP.ThreadFileLogger
import qualified Data.HashMap.Strict as HMS
import           Data.Void (absurd)
import qualified Data.UUID as UUID
import qualified Data.UUID.V4 as UUID.V4
import qualified Crypto.Hash.SHA1 as SHA1
import qualified Data.ByteString.Base64 as Base64

-- | A 'JobClient' is needed to submit requests and retrieve results.
--
-- Use 'withJobClient' to provide a 'JobClient'.
data JobClient response = JobClient
    { jcConfig :: !JobQueueConfig
    , jcResponseWatchers :: !ResponseWatchers
    , jcRedis :: !Redis
    }

data ResponseWatcher = ResponseWatcher
    { rwWatchers :: !Int
    , rwGotNotification :: !Bool
    }

type ResponseWatchers = MVar (HMS.HashMap RequestId (TVar ResponseWatcher))

-- | Start a new job-queue client, and perform the continuation passed on it.
--
-- As long as the client runs (i.e., until it is garbage collected), it
--
-- * can be used to submit new requests via 'submitRequest'
--
-- * can be used to query/wait for responses using 'waitForResponse',
-- 'waitForResponse_', and 'checkForResponse'
--
-- * repeatedly checks for worker heartbeats.  When it detects a
-- heartbeat failure, it will remove the worker from the list of
-- connected workers, and put the request it was processing back on
-- the queue for rescheduling.
--
-- REVIEW: We need a thread here for the client to check the heartbeats and
-- to subscribe to the channel receiving incoming response notifications.
withJobClient :: forall m response a.
       (MonadConnect m, Sendable response)
    => JobQueueConfig
    -> (JobClient response -> m a)
    -> m a
withJobClient config@JobQueueConfig{..} cont = do
    watchers <- newMVar HMS.empty
    withRedis jqcRedisConfig $ \r -> do
        let jc :: JobClient response
            jc = JobClient
                { jcConfig = config
                , jcResponseWatchers = watchers
                , jcRedis = r
                }
        fmap (either absurd id) $
            Async.race (jobClientThread jc) (cont jc)

jobClientThread :: forall response void m.
       (Sendable response, MonadConnect m)
    => JobClient response -> m void
jobClientThread JobClient{..} = do
    setRedisSchemaVersion jcRedis
    fmap (either absurd absurd) (Async.race checker subscriber)
  where
    heartbeatChecker =
        checkHeartbeats (jqcHeartbeatConfig jcConfig) jcRedis $ handleHeartbeatFailures jcRedis
    staleChecker = checkStaleKeys jcConfig jcRedis

    checker = fmap (either absurd absurd) (Async.race heartbeatChecker staleChecker)

    subscriber =
        withLogTag (LogTag "jobClient") $ do
            withSubscriptionsManaged (redisConnectInfo jcRedis) (responseChannel jcRedis :| []) $ \getRid -> forever $ do
                (_chan, rid) <- getRid
                handleResponse (RequestId rid)

    handleResponse rid = do
        watchers <- HMS.lookup rid <$> readMVar jcResponseWatchers
        forM_ watchers $ \watchVar -> do
            atomically (modifyTVar watchVar (\rw -> rw{rwGotNotification = True}))

-- | Submits a new request.
submitRequest :: forall request response m.
       (MonadConnect m, Sendable request, Sendable response)
    => JobClient response
    -- ^ Client that will schedule the request (to generate a client,
    -- use 'withJobClient')
    -> RequestId
    -- ^ Unique Id that will reference this request.  Can be generated
    -- with 'uniqueRequestId'.
    -> request
    -> m ()
submitRequest = submitRequestWithPriority PriorityNormal

-- | Submits a new request that will be de-enqueued sooner than any job request submitted via 'submitRequest'.
submitRequestUrgent :: forall request response m.
       (MonadConnect m, Sendable request, Sendable response)
    => JobClient response
    -- ^ Client that will schedule the request (to generate a client,
    -- use 'withJobClient')
    -> RequestId
    -- ^ Unique Id that will reference this request.  Can be generated
    -- with 'uniqueRequestId'.
    -> request
    -> m ()
submitRequestUrgent = submitRequestWithPriority PriorityUrgent


-- | As 'submitRequest', but allows giving the job a custom 'RequestPriority'.
submitRequestWithPriority :: forall request response m.
       (MonadConnect m, Sendable request, Sendable response)
    => RequestPriority
    -- ^ Priority of the request.
    -> JobClient response
    -- ^ Client that will schedule the request (to generate a client,
    -- use 'withJobClient')
    -> RequestId
    -- ^ Unique Id that will reference this request.  Can be generated
    -- with 'uniqueRequestId'.
    -> request
    -> m ()
submitRequestWithPriority priority JobClient{..} rid request = do
    let encoded = S.encode JobRequest
            { jrRequestTypeHash = typeHash (Proxy :: Proxy request)
            , jrResponseTypeHash = typeHash (Proxy :: Proxy response)
            , jrSchema = redisSchemaVersion
            , jrBody = S.encode request
            }
    $logDebugSJ "sendRequest" $ "Pushing request " <> tshow rid
    when (priority == PriorityUrgent) (run_ jcRedis (sadd (urgentRequestsSetKey jcRedis) (unRequestId rid :| [])))
    -- NOTE: It's crucial for the body to be added _before_ the request id in the requests
    -- list, since the worker will just drop the request id if the body is missing.
    added <- run jcRedis (set (requestDataKey jcRedis rid) encoded [EX (jqcRequestExpiry jcConfig), NX])
    if not added
        then $logWarnSJ "submitRequest" $
            "Didn't submit request " <> tshow rid <> " because it already exists in redis."
        else do
            let queueKey = case priority of
                    PriorityNormal -> requestsKey jcRedis
                    PriorityUrgent -> urgentRequestsKey jcRedis
            run_ jcRedis (lpush queueKey (unRequestId rid :| []))
            $logDebugSJ "submitRequest" $ "Notifying about request " <> tshow rid
            sendNotify jcRedis (requestChannel jcRedis)
            addRequestEnqueuedEvent jcConfig jcRedis rid

-- | Checks if the specified 'RequestId' exists, in the form of request
-- or response data.
requestExists
    :: MonadConnect m
    => Redis -> RequestId -> m Bool
requestExists r k = do
    dataExists <- run r $ exists (unVKey (requestDataKey r k))
    if dataExists then return True else do
        run r $ exists (unVKey (responseDataKey r k))

-- | A simpler 'waitForResponse', that automatically blocks on the response
-- arriving, and throws the 'DistributedException'.
waitForResponse_ :: forall m response.
       (MonadConnect m, Sendable response)
    => JobClient response
    -> RequestId
    -> m (Maybe response)
waitForResponse_ jc rid =
    waitForResponse jc rid (\m -> either throwIO return =<< atomically m)

-- | Returns an action that blocks until the response is present.
-- 'Nothing' if the request doesn't exist.
waitForResponse :: forall m response a.
       (MonadConnect m, Sendable response)
    => JobClient response
    -> RequestId
    -> (STM (Either DistributedException response) -> m a)
    -> m (Maybe a)
waitForResponse jc rid cont = waitForResponses jc [rid] $ \hm -> case HMS.lookup rid hm of
    Nothing -> return Nothing
    Just m -> Just <$> cont m

-- | Wait for a bunch of responses. The 'RequestId's that do not exist
-- _at the time when waitForResponses is called_ iwill not be present in the
-- 'HMS.HashMap'.
waitForResponses :: forall m response a.
       (MonadConnect m, Sendable response)
    => JobClient response
    -> [RequestId]
    -> (HMS.HashMap RequestId (STM (Either DistributedException response)) -> m a)
    -> m a
waitForResponses jc@JobClient{..} rids0 cont = do
    let candidateRequests :: HMS.HashMap RequestId () = HMS.fromList (zip rids0 (repeat ()))
    existantRequests <-
        fmap HMS.fromList $ filterM (\(reqId, ()) -> requestExists jcRedis reqId) (HMS.toList candidateRequests)
    -- One TVar per request
    tvars :: HMS.HashMap RequestId (TMVar (Either DistributedException response)) <-
        traverse (\() -> liftIO newEmptyTMVarIO) existantRequests
    let getResps :: HMS.HashMap RequestId (STM (Either DistributedException response)) = fmap readTMVar tvars
    Async.withAsync (delayLoop existantRequests tvars) $ \delayLoopAsync ->
        Async.withAsync (watcher tvars) $ \watcherAsync ->
            Async.withAsync (cont getResps) $ \contAsync -> do
                -- Explicitly wait on workers to catch and re-throw exceptions they might
                -- throw
                let waitOnAll mbWorker1 mbWorker2 action = do
                        let waitWorker = maybe retry Async.waitSTM
                        res <- orElse
                            (Left <$> (orElse (Left <$> waitWorker mbWorker1) (Right <$> waitWorker mbWorker2)))
                            (Right <$> Async.waitSTM action)
                        case res of
                            Left (Left ()) -> waitOnAll Nothing mbWorker2 action
                            Left (Right ()) -> waitOnAll mbWorker1 Nothing action
                            Right x -> return x
                atomically (waitOnAll (Just delayLoopAsync) (Just watcherAsync) contAsync)
  where
    lookupReqTMVar ::
           HMS.HashMap RequestId (TMVar (Either DistributedException response))
        -> RequestId
        -> m (TMVar (Either DistributedException response))
    lookupReqTMVar vars rid = case HMS.lookup rid vars of
        Nothing -> throwM (InternalJobQueueException ("Could not find TVar for request " <> tshow rid <> " in response watcher."))
        Just var -> return var

    delayLoop ::
           HMS.HashMap RequestId () -- ^ Requests still to wait on
        -> HMS.HashMap RequestId (TMVar (Either DistributedException response))
        -> m ()
    delayLoop rids vars = if HMS.null rids
        then return ()
        else do
            finishedReqs <- checkForResponses jc (HMS.keys rids)
            forM_ (HMS.toList finishedReqs) $ \(rid, resp) -> do
                var <- lookupReqTMVar vars rid
                void (atomically (tryPutTMVar var resp))
            let missingReqs = rids `HMS.difference` finishedReqs
            liftIO (threadDelay (fromIntegral (unMilliseconds (jqcWaitForResponseNotificationFailsafeTimeout jcConfig) * 1000)))
            -- Before resuming, see if some request got deleted -- this can happen,
            -- and we don't want to be stuck on them forever
            vanishedRequests <- filterM (\(rid, ()) -> not <$> requestExists jcRedis rid) (HMS.toList missingReqs)
            forM_ vanishedRequests $ \(rid, ()) -> do
                -- Record them as vanished
                let err = "Was waiting on request " <> tshow rid <> ", but it disappeared. This is possible if the caching duration for requests is too low and the request expired while waiting for it."
                $logWarnJ err
                var <- lookupReqTMVar vars rid
                void (atomically (tryPutTMVar var (Left (InternalJobQueueException err))))
            delayLoop (missingReqs `HMS.difference` HMS.fromList vanishedRequests) vars

    watcher ::
           HMS.HashMap RequestId (TMVar (Either DistributedException response))
        -> m ()
    watcher tvars = bracket startWatching stopWatching watchLoop
      where
        startWatching :: m (HMS.HashMap RequestId (TVar ResponseWatcher))
        startWatching = modifyMVar jcResponseWatchers $ \hm0 -> do
            let addToWatchers (hm, watchers) rid = do
                    case HMS.lookup rid hm of
                        Nothing -> do
                            var <- liftIO (newTVarIO ResponseWatcher{rwWatchers = 1, rwGotNotification = False})
                            return (HMS.insert rid var hm, HMS.insert rid var watchers)
                        Just var -> do
                            atomically (modifyTVar var (\rw -> rw{rwWatchers = rwWatchers rw + 1}))
                            return (hm, HMS.insert rid var watchers)
            foldM addToWatchers (hm0, HMS.empty) (HMS.keys tvars)

        stopWatching :: HMS.HashMap RequestId (TVar ResponseWatcher) -> m ()
        stopWatching watchers = modifyMVar_ jcResponseWatchers $ \hm0 -> do
            let removeWatcher hm rid = case HMS.lookup rid hm of
                    Just var -> do
                        watchersCount <- atomically $ do
                            rw <- readTVar var
                            let rw' = rw{rwWatchers = rwWatchers rw - 1}
                            writeTVar var rw'
                            return (rwWatchers rw')
                        return $ if (watchersCount < 1)
                            then HMS.delete rid hm
                            else hm
                    Nothing -> throwM (InternalJobQueueException ("Could not find entry for request " <> tshow rid <> " in response watchers."))
            foldM removeWatcher hm0 (HMS.keys watchers)

        watchLoop :: HMS.HashMap RequestId (TVar ResponseWatcher) -> m ()
        watchLoop watchers = if HMS.null watchers
            then return ()
            else do
                rid <- atomically $ do
                    let waitOnAll = \case
                            [] -> retry
                            (rid, rwVar) : rwVars -> do
                                rw <- readTVar rwVar
                                if rwGotNotification rw
                                    then return rid
                                    else waitOnAll rwVars
                    waitOnAll (HMS.toList watchers)
                mbResp <- checkForResponse jc rid
                case mbResp of
                    Nothing -> do
                        $logWarnJ ("Got notification for request " <> tshow rid <> ", but then found no response. This is possible but unlikely, re-subscribing.")
                        watchLoop watchers
                    Just resp -> do
                        var <- lookupReqTMVar tvars rid
                        void (atomically (tryPutTMVar var resp))
                        -- This remove is just an optimization. Consider for example
                        -- that we do not care about the responses that the 'delayLoop' gets
                        -- -- we just wait on them redundantly.
                        watchLoop (HMS.delete rid watchers)

-- | Returns immediately with the response, if present.
checkForResponse ::
       (MonadConnect m, Sendable response)
    => JobClient response
    -> RequestId
    -> m (Maybe (Either DistributedException response))
checkForResponse jc rid = HMS.lookup rid <$> checkForResponses jc [rid]

checkForResponses ::
       (MonadConnect m, Sendable response)
    => JobClient response
    -> [RequestId]
    -> m (HMS.HashMap RequestId (Either DistributedException response))
checkForResponses JobClient{..} rids0 = case NE.nonEmpty rids0 of
    Nothing -> return mempty
    Just rids -> do
        mresponses0 <- zip rids0 . toList <$> run jcRedis (mget (fmap (responseDataKey jcRedis) rids))
        let mresponses = do
                (rid, mresponse) <- mresponses0
                case mresponse of
                    Nothing -> []
                    Just resp -> [(rid, resp)]
        fmap HMS.fromList $ forM mresponses $ \(rid, resp) -> do
            result <- decodeOrThrow "checkForResponses" resp
            addRequestEvent jcRedis rid RequestResponseRead
            return (rid, result)

-- | Generate a unique (UUID-based) 'RequestId'.
uniqueRequestId :: (MonadIO m) => m RequestId
uniqueRequestId = liftIO (RequestId . UUID.toASCIIBytes <$> UUID.V4.nextRandom)

-- | Generate a 'RequestId' based on the request contents.
requestHashRequestId :: (Sendable request) => request -> RequestId
requestHashRequestId = RequestId . Base64.encode . SHA1.hash . S.encode

-- | Cancel a request. This has the effect of erasing the presence of
-- the 'Request' from the system, so that:
--
-- * If the request is currently enqueued, it'll disappear from the queue.
-- * If the request is completed, the response will be erased.
-- * If the request is currently being worked on, the work will cease.
--
-- Note that work might not stop immediately, but it guaranteed to eventually
-- stop (by default the delay can be up to 10 seconds).
--
-- In this window, the worker might be able to complete the request, in which
-- case the request response might be available.
cancelRequest :: MonadConnect m => Seconds -> Redis -> RequestId -> m ()
cancelRequest expiry redis k = do
    $logInfoJ ("Cancelling request " ++ tshow k)
    run_ redis (set (cancelKey redis k) cancelValue [EX expiry])
    run_ redis (del (unVKey (requestDataKey redis k) :| []))
    run_ redis (del (unVKey (responseDataKey redis k) :| []))
    run_ redis (lrem (requestsKey redis) 1 (unRequestId k))
    run_ redis (lrem (urgentRequestsKey redis) 1 (unRequestId k))
    run_ redis (srem (urgentRequestsSetKey redis) (unRequestId k :| []))

-- | Manually trigger re-enqueuement of jobs of dead workers.
--
-- When a worker is detected as dead by the heartbeat mechanism, its
-- job is automatically re-enqueued.  However, since the heartbeat
-- checks are only performed if there is at least one client running,
-- jobs will not be re-enqueued if there are no clients.
--
-- This function explicitly performs a heartbeat check, and causes
-- jobs of dead workers to be re-enqueued.
reenqueueRequests :: MonadConnect m => Redis -> m ()
reenqueueRequests r = do
    currentTime <- liftIO getPOSIXTime
    performHeartbeatCheck r currentTime $ handleHeartbeatFailures r


handleHeartbeatFailures :: MonadConnect m
                           => Redis
                           -> NonEmpty WorkerId -> m () -> m ()
handleHeartbeatFailures r inactive cleanup = do
    reenqueuedSome <- or <$> forM inactive
        (\wid -> isJust <$> reenqueueRequest ReenqueuedAfterHeartbeatFailure r wid)
    cleanup
    when reenqueuedSome $ do
        $logDebugSJ "JobClient" ("Notifying that some requests were re-enqueued"::Text)
        sendNotify r (requestChannel r)
