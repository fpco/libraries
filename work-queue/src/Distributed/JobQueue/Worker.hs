{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE ViewPatterns #-}

-- This module provides a job-queue with heartbeat checking.
module Distributed.JobQueue.Worker
    ( jobWorker
    -- , runJQWorker
    ) where

import ClassyPrelude
import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (cancel, cancelWith, waitEither, race, withAsync)
import Control.Exception (AsyncException)
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
import Data.Serialize (Serialize)
import Distributed.Heartbeat
import Distributed.JobQueue.Internal
import Distributed.Redis
import Distributed.Types
import FP.Redis
import FP.ThreadFileLogger
import System.Posix.Process (getProcessID)
import Control.Monad.Trans.Control (control)
import Data.Void (absurd)
import qualified Control.Concurrent.Async.Lifted.Safe as Async

-- | Implements a compute node which responds to requests and never
-- returns.
--
-- REVIEW: It uses an atomic redis operation (rpop and lpush) to get stuff from redis,
-- it's not entirely clear who wins when there is contention.
-- REVIEW TODO: Here there is some ambiguity on when we "save" a worker: for example
-- some redis operations are 'try'd, some arent. I propose to never cach exceptions
-- apart in a top-level handler, and when we run the user provided function.
jobWorker
    :: (MonadConnect m, Sendable request, Sendable response)
    => JobQueueConfig
    -> (Redis -> RequestId -> request -> m (Either CancelOrReenqueue response))
    -- ^ This function is run by the worker, for every request it
    -- receives.
    -> m void
jobWorker config@JobQueueConfig {..} f = do
    wid <- liftIO getWorkerId
    let withTag = withLogTag (LogTag ("worker-" ++ tshow (unWorkerId wid)))
    withTag $ withRedis jqcRedisConfig $ \r -> do
        $logDebug "Starting heartbeats"
        withHeartbeats jqcHeartbeatConfig r wid $ do
            $logDebug "Initial heartbeat sent, starting worker"
            -- REVIEW TODO: Restore notifications once we have faith in redis-fp
            notifyVar :: MVar () <- newEmptyMVar
            let notifyLoop = forever $ liftIO $ do
                    threadDelay (25 * 1000)
                    void (tryPutMVar notifyVar ())
            fmap (either id absurd) $ Async.race
                (jobWorkerThread config r wid (takeMVar notifyVar) (f r))
                notifyLoop

data CancelOrReenqueue
    = Cancel
    | Reenqueue
    deriving (Eq, Show)

jobWorkerThread
    :: forall request response m void.
       (MonadConnect m, Sendable request, Sendable response)
    => JobQueueConfig
    -> Redis
    -> WorkerId
    -> m ()
    -- ^ This action, when executed, blocks until we think a new response is present.
    -> (RequestId -> request -> m (Either CancelOrReenqueue response))
    -> m void
jobWorkerThread JobQueueConfig{..} r wid waitForNewRequest f = forever $ do
    mbRidbs <- run r (rpoplpush (requestsKey r) (LKey (activeKey r wid)))
    forM_ mbRidbs $ \ridBs -> do
        let rid = RequestId ridBs
        mbReqBs <- receiveRequest r wid rid
        -- Nothing if it has to be re-enqueued.
        mbRespOrErr :: Maybe (Either DistributedException response) <- case mbReqBs of
            Nothing ->
                return (Just (Left (RequestMissingException rid)))
            Just reqBs -> case checkRequest (Proxy :: Proxy response) rid reqBs of
                Left err -> return (Just (Left err))
                Right req -> do
                    cancelledOrResp :: Either () (Either SomeException (Either CancelOrReenqueue response)) <-
                        Async.race (watchForCancel r rid jqcCancelCheckIvl) (try (f rid req))
                    case cancelledOrResp of
                        Left () -> return (Just (Left (RequestCanceled rid)))
                        Right (Left (fromException -> Just (err :: AsyncException))) ->
                            liftIO (throwIO err)
                        Right (Left err) -> return (Just (Left (wrapException err)))
                        Right (Right (Left Reenqueue)) -> return Nothing
                        Right (Right (Left Cancel)) -> return (Just (Left (RequestCanceled rid)))
                        Right (Right (Right res)) -> return (Just (Right res))
        let gotX msg = do
            $logInfo ("Request " ++ tshow rid ++ " got " ++ msg)
        case mbRespOrErr of
            Nothing -> do
                gotX "reenqueue"
                addRequestEvent r rid (RequestWorkReenqueuedByWorker wid)
            Just res -> do
                case res of
                    Left err -> gotX ("exception: " ++ tshow err)
                    Right _ -> $logDebug ("Request " ++ tshow rid ++ " got respose")
                sendResponse r jqcResponseExpiry wid rid (encode res)
                addRequestEvent r rid (RequestWorkFinished wid)
    -- Wait for notification only if we've already consumed all the available requests
    -- -- the notifications only tell us about _new_ requests, nothing about how many
    -- there might be backed up in the queue.
    unless (isJust mbRidbs) $ do
        $logDebug "Waiting for request notification"
        waitForNewRequest
        $logDebug "Got notified of an available request"

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
            run_ r (del (unVKey (requestDataKey r k) :| []))
        _ -> $logWarn $
            tshow k <>
            " isn't a member of active queue (" <>
            tshow ak <>
            "), likely indicating that a heartbeat failure happened, causing it to be erroneously re-enqueued.  This doesn't affect correctness, but could mean that redundant work is performed."
    -- Store when the response was stored.
    responseTime <- liftIO getPOSIXTime
    setRedisTime r (responseTimeKey r k) responseTime [EX expiry]

watchForCancel :: forall m void. (MonadConnect m) => Redis -> RequestId -> Seconds -> m ()
watchForCancel r k ivl = loop
  where
    loop :: m ()
    loop = do
        mres <- run r (get (cancelKey r k))
        case mres of
            Just res
                | res == cancelValue -> return ()
                | otherwise -> liftIO $ throwIO $ InternalJobQueueException
                    "Didn't get expected value at cancelKey."
            Nothing -> do
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
    pid <- getProcessID
    (w1, w2, w3, w4) <- toWords <$> UUID.nextRandom
    let w1' = w1 `xor` fromIntegral pid
    return $ WorkerId $ UUID.toASCIIBytes $ UUID.fromWords w1' w2 w3 w4

-- * Running a JobQueue worker

{-
data MasterOrSlave = Idle | Slave | Master
    deriving (Eq, Ord, Show)

-- REVIEW: To request slaves, there is a separate queue from normal requests, the
-- reason being that we want to prioritize slave requests over normal requests.
runJQWorker
    :: (Sendable request, Sendable response)
    => LogFunc
    -> JobQueueConfig
    -> (Redis -> WorkerConnectInfo -> IO ())
    -> (Redis -> RequestId -> request -> IO response)
    -- REVIEW: How to get the master's WorkerConnectInfo is specific to your
    -- particular master/slave setup.
    -> IO ()
runJQWorker logFunc config slaveFunc masterFunc = do
    stateVar <- newTVarIO Idle
    void $
        runLoggingT (handleWorkerRequests stateVar) logFunc `race`
        runLoggingT (handleRequests stateVar) logFunc
  where
    handleWorkerRequests stateVar =
        withRedis (jqcRedisConfig config) $ \redis ->
            -- Fetch connect requests. The first argument is an STM
            -- action which determines whether we want to receive slave
            -- requests. We only want to become a slave if we're not in
            -- 'Master' mode.
            withConnectRequests ((==Idle) <$> readTVar stateVar) redis $ \wci ->
                liftIO $ slaveFunc redis wci
    handleRequests stateVar =
        jobWorker config $ \redis rid request -> liftIO $
            -- Ensure that the node was idle before becoming a master
            -- node. If we got a slave request simultaneously with
            -- getting a work request, the slave request gets
            -- prioritized and the work gets re-enqueud.
            bracket (transitionIdleTo Master stateVar)
                    (\_ -> backToIdle stateVar) $ \wasIdle -> do
                when (not wasIdle) $ reenqueueWork rid
                masterFunc redis rid request
    backToIdle stateVar = atomically $ writeTVar stateVar Idle
    transitionIdleTo state' stateVar = atomically $ do
        state <- readTVar stateVar
        if state == Idle
            then do
                writeTVar stateVar state'
                return True
             else return False
-}