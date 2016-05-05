{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE ViewPatterns #-}
module Distributed.JobQueue.Client
    ( JobClient
    , withJobClient
    , submitRequest
    , waitForResponse
    , checkForResponse
    , cancelRequest
    ) where

import           ClassyPrelude
import           Control.Concurrent (threadDelay)
import           Control.Concurrent.STM (retry)
import           Control.Monad.Logger
import           Data.List.NonEmpty (NonEmpty((:|)))
import           Data.Serialize (encode)
import           Data.Streaming.NetworkMessage (Sendable)
import           Data.TypeFingerprint
import           Data.Typeable (Proxy(..))
import           Distributed.Heartbeat (checkHeartbeats)
import qualified Control.Concurrent.Async.Lifted.Safe as Async
import           Distributed.JobQueue.Internal
import           Distributed.Redis
import           Distributed.Types
import           FP.Redis
import           FP.ThreadFileLogger
import qualified Data.HashMap.Strict as HMS
import           Data.Void (absurd)

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

-- | Start a new job-queue client, which will run forever. For most
-- usecases, this should only be invoked once for the process, usually
-- on initialization.
--
-- The client does two things:
--
-- 1. Listening for responses from workers.
--
-- 2. Checking for worker heartbeat responses, and handling heartbeat
--    failures.
--
-- REVIEW TODO: The client is not run forever anymore. It's now garbage
-- collected. Update comment.
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
    checker =
        checkHeartbeats (jqcHeartbeatConfig jcConfig) jcRedis $ \inactive cleanup -> do
            reenqueuedSome <- or <$> forM inactive (handleWorkerFailure jcRedis)
            cleanup
            when reenqueuedSome $ do
                $logDebugS "JobClient" "Notifying that some requests were re-enqueued"
                sendNotify jcRedis (requestChannel jcRedis)

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
submitRequest
    :: forall request response m.
       (MonadConnect m, Sendable request, Sendable response)
    => JobClient response
    -> RequestId
    -> request
    -> m ()
submitRequest JobClient{..} rid request = do
    let encoded = encode JobRequest
            { jrRequestTypeFingerprint = typeFingerprint (Proxy :: Proxy request)
            , jrResponseTypeFingerprint = typeFingerprint (Proxy :: Proxy response)
            , jrSchema = redisSchemaVersion
            , jrBody = encode request
            }
    $logDebugS "sendRequest" $ "Pushing request " <> tshow rid
    added <- run jcRedis (set (requestDataKey jcRedis rid) encoded [EX (jqcRequestExpiry jcConfig), NX])
    if not added
        then $logWarnS "submitRequest" $
            "Didn't submit request " <> tshow rid <> " because it already exists in redis."
        else do
            run_ jcRedis (lpush (requestsKey jcRedis) (unRequestId rid :| []))
            $logDebugS "submitRequest" $ "Notifying about request " <> tshow rid
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

-- | Blocks until the response is present. 'Nothing' if the request doesn't exist.
waitForResponse :: forall m response.
       (MonadConnect m, Sendable response)
    => JobClient response
    -> RequestId
    -> m (Maybe (Either DistributedException response))
waitForResponse jc@JobClient{..} rid = do
    -- First, ensure that the request actually exists.
    reqExists <- requestExists jcRedis rid
    if not reqExists
        then return Nothing
        else fmap (either id id) (Async.race delayLoop watcher)
  where
    delayLoop :: m (Maybe (Either DistributedException response))
    delayLoop = do
        mbResp <- checkForResponse jc rid
        case mbResp of
            Just resp -> return (Just resp)
            Nothing -> do
                -- Check 10 times a second anyway, we can't rely on notifications
                liftIO (threadDelay (100 * 1000))
                -- Before resuming, see if the request got deleted -- this can happen,
                -- and we don't want to be stuck forever.
                reqExists <- requestExists jcRedis rid
                if reqExists
                    then delayLoop
                    else do
                        $logWarn ("Was waiting on request " <> tshow rid <> ", but it disappeared. This is possible but very unlikely.")
                        return Nothing

    watcher :: m (Maybe (Either DistributedException response))
    watcher = do
        mbResp <- bracket startWatching stopWatching $ \var -> do
            atomically $ do
                rw <- readTVar var
                unless (rwGotNotification rw) retry
            checkForResponse jc rid
        case mbResp of
            Nothing -> do
                $logWarn ("Got notification for request " <> tshow rid <> ", but then found no response. This is possible but unlikely, re-subscribing.")
                watcher
            Just resp -> return (Just resp)
      where
        startWatching :: m (TVar ResponseWatcher)
        startWatching = modifyMVar jcResponseWatchers $ \hm -> do
            case HMS.lookup rid hm of
                Nothing -> do
                    var <- liftIO (newTVarIO ResponseWatcher{rwWatchers = 1, rwGotNotification = False})
                    return (HMS.insert rid var hm, var)
                Just var -> do
                    atomically (modifyTVar var (\rw -> rw{rwWatchers = rwWatchers rw + 1}))
                    return (hm, var)

        stopWatching :: TVar ResponseWatcher -> m ()
        stopWatching _var = modifyMVar_ jcResponseWatchers $ \hm -> do
            case HMS.lookup rid hm of
                Just var -> do
                    watchers <- atomically $ do
                        rw <- readTVar var
                        let rw' = rw{rwWatchers = rwWatchers rw - 1}
                        writeTVar var rw'
                        return (rwWatchers rw')
                    return $ if (watchers < 1)
                        then HMS.delete rid hm
                        else hm
                Nothing -> throwM (InternalJobQueueException ("Could not find entry for request " <> tshow rid <> " in response watchers."))

-- | Returns immediately with the request, if present.
checkForResponse ::
       (MonadConnect m, Sendable response)
    => JobClient response
    -> RequestId
    -> m (Maybe (Either DistributedException response))
checkForResponse JobClient{..} rid = do
    mresponse <- run jcRedis (get (responseDataKey jcRedis rid))
    forM mresponse $ \response -> do
        result <- decodeOrThrow "checkForResponse" response
        addRequestEvent jcRedis rid RequestResponseRead
        return result

-- | Cancel a request. (Note that this does
-- *not* guarantee that the worker actually manages to cancel its work).
--
-- FIXME: I think there's a race here where deleting the data could
-- cause a 'RequestMissingException'
cancelRequest :: MonadConnect m => Seconds -> Redis -> RequestId -> m ()
cancelRequest expiry redis k = do
    run_ redis (del (unVKey (requestDataKey redis k) :| []))
    run_ redis (lrem (requestsKey redis) 1 (unRequestId k))
    run_ redis (set (cancelKey redis k) cancelValue [EX expiry])

handleWorkerFailure :: (MonadConnect m) => Redis -> WorkerId -> m Bool
handleWorkerFailure r wid = do
    -- REVIEW TODO it would be nice to check that after this the activeKey is empty,
    -- but we cannot do it (the worker might still be alive and adding a request to
    -- the activeKey.)
    mbRid <- run r (rpoplpush (activeKey r wid) (requestsKey r))
    case mbRid of
        Nothing -> do
            $logWarnS "JobQueue" $ tshow wid <> " failed its heartbeat, but didn't have an item to re-enqueue."
        Just rid -> do
            $logWarnS "JobQueue" $ tshow wid <> " failed its heartbeat, and " <> tshow rid <> " was re-enqueued."
    return (isJust mbRid)
