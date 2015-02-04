{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE ViewPatterns #-}

-- | This module provides a work-queue based on Redis.  It's
-- recommended that the higher level "Distributed.JobQueue" be used
-- instead, as it uses this API properly.  The Redis queue has the
-- following features:
--
-- (1) Many clients can enqueue work and block on responses, without
-- knowing anything about the workers.
--
-- (2) Many workers can ask for incoming work requests, and block on
-- these.
--
-- (3) A guarantee that enqueued work will not be lost until it's
-- completed, even in the presence of server failure.  A failure in
-- Redis persistence can invalidate this guarantee.
--
--     - This guarantee requires that workers run a 'sendHeartbeats'
--     thread, and clients run a 'checkHeartbeats' thread.
--
--     - SIDENOTE: We may need a high reliability redis configuration
--     for this guarantee as well. The redis wikipedia article
--     mentions that the default config can lose changes received
--     during the 2 seconds before failure.
--
-- One caveat is that it does not current make the guarantee that
-- results are only delivered once.  It's up to the client to deal
-- with this. The higher level "Distributed.JobQueue" handles this
-- gracefully by deregistering the callback when a response comes
-- back.
--
-- Along with being idempotent, this also requires that computations
-- be essentially pure.  This is because requests are hashed in order
-- to figure out their 'RequestId'.  If a response already exists for
-- a given 'RequestId', then it is returned instead of enqueuing a
-- redundant computation.
module Distributed.RedisQueue
    ( ClientInfo(..), WorkerInfo(..), RedisInfo(..)
    , RequestId(..), BackchannelId(..), WorkerId(..)
    , DistributedRedisQueueException(..)
    , withRedisInfo
    , pushRequest
    , popRequest
    , sendResponse
    , readResponse
    , subscribeToResponses
    , sendHeartbeats
    , checkHeartbeats
    , getUnusedWorkerId
    ) where

import           ClassyPrelude
import           Control.Concurrent (threadDelay)
import           Control.Monad.Logger (MonadLogger, logError)
import qualified Crypto.Hash.SHA1 as SHA1
import           Data.Binary (Binary, encode, decode)
import qualified Data.ByteString.Char8 as BS8
import           Data.List.NonEmpty (NonEmpty((:|)), nonEmpty)
import           FP.Redis
import           FP.Redis.Mutex
import           System.Random (randomRIO)

-- | Info required to submit requests to the queue ('pushRequest'),
-- wait for responses ('subscribeToResponses'), and retrieve them
-- ('readResponse').
data ClientInfo = ClientInfo
    { clientBackchannelId :: BackchannelId
    , clientRequestExpiry :: Seconds
    }

-- | Info required to wait for incoming requests ('popRequest'), and
-- yield corresponding responses ('sendResponse').
data WorkerInfo = WorkerInfo
    { workerId :: WorkerId
    , workerResponseExpiry :: Seconds
    }

-- | Common information about redis, used by both client and worker.
data RedisInfo = RedisInfo
    { redisConnection :: Connection
    , redisConnectInfo :: ConnectInfo
    , redisKeyPrefix :: ByteString
    }

-- | ID of a redis channel used for notifications about a particular
-- request.  One way to use this is to give each client server its own
-- 'BackchannelId', so that it is only informed of responses
-- associated with the requests it makes.
newtype BackchannelId = BackchannelId { unBackchannelId :: ByteString }
    deriving (Eq, Show, Binary, IsString)

-- | Every worker server has a 'WorkerId' to uniquely identify it.
-- It's needed for the fault tolerance portion - in the event that a
-- worker goes down we need to be able to re-enqueue its work.
newtype WorkerId = WorkerId { unWorkerId :: ByteString }
    deriving (Eq, Show, Binary, IsString)

-- | This is the key used for enqueued requests, and, later, the
-- response associated with it.  It's the hash of the request, which
-- allows responses to be cached.
newtype RequestId = RequestId { unRequestId :: ByteString }
    deriving (Eq, Show, Binary, Hashable)

-- | This acquires a Redis 'Connection' and wraps it up along with a
-- key prefix into a 'RedisInfo' value.  This 'RedisInfo' value can
-- then be used to run the various functions in this module.
withRedisInfo
    :: MonadConnect m
    => ByteString -> ConnectInfo -> (RedisInfo -> m ()) -> m ()
withRedisInfo redisKeyPrefix redisConnectInfo f =
    withConnection redisConnectInfo $ \redisConnection -> f RedisInfo {..}

-- | Pushes a request to the compute workers.  If the result has been
-- computed previously, and the result is still cached, then it's
-- returned as a 'Just' value in the 'snd' part of the result.  The
-- 'RequestId' provided in the result can later be used to associate a
-- response with the request (see 'subscribeToResponses' and
-- 'readResponse').
--
-- The request data is given an expiry of 'clientRequestExpiry'.  If
-- the request data expires before being dequeued, then the request is
-- silently dropped.
pushRequest
    :: MonadCommand m
    => RedisInfo -> ClientInfo -> ByteString -> m (RequestId, Maybe ByteString)
pushRequest r (ClientInfo bid expiry) request = do
    -- Check if the response has already been computed.  If so, then
    -- yield it.
    let k = RequestId (SHA1.hash request)
    result <- run r $ get (responseDataKey r k)
    case result of
        Just _ -> return (k, result)
        Nothing -> do
            -- Store the request data as a normal redis value.
            run_ r $ set (requestDataKey r k) request [EX expiry]
            -- Enqueue its ID on the requests list.
            run_ r $ lpush (requestsKey r) (toStrict (encode (k, bid)) :| [])
            return (k, Nothing)

-- | This function is used by the compute workers to take work off of
-- the queue.  When work is taken off the queue, it also gets moved to
-- an in-progress queue specific to the worker, atomically.  This is
-- done so that in the event of server failure, the work items can be
-- re-enqueued.
--
-- If this worker has been erroneously failed the heartbeat check,
-- then it should exit.  When this circumstance occurs, this function
-- throws a 'WorkerHeartbeatFailed' exception.
popRequest
    :: (MonadCommand m, MonadThrow m)
    => RedisInfo -> WorkerInfo -> m (RequestId, BackchannelId, Maybe ByteString)
popRequest r (WorkerInfo wid _) = do
    -- 'inProgressKey' can either be a list or a dummy string value.
    -- It gets replaced by a string when its in-progress items get
    -- moved to the requests queue.  This causes this command to exit
    -- without taking action.  So, the use of 'LKey' here is not
    -- entirely valid, and this can cause 'brpoplpush' to yield a
    -- 'CommandException'.
    let src = requestsKey r
        dest = LKey $ inProgressKey r wid
    eres <- try $ run r $ brpoplpush src dest (Seconds 0)
    case eres of
        Left (CommandException (isPrefixOf "WRONGTYPE" -> True)) ->
            throwM (WorkerHeartbeatFailed wid)
        Left ex ->
            throwM ex
        Right Nothing ->
            fail "impossible: brpoplpush with 0 timeout reported timeout"
        Right (Just (decode . fromStrict -> (k, bid))) -> do
            mx <- run r $ get (requestDataKey r k)
            return (k, bid, mx)

-- | Send a response for a particular request.  This is done by the
-- compute workers once they're done with the computation, and have
-- results to send.
sendResponse
    :: (MonadCommand m, MonadLogger m)
    => RedisInfo -> WorkerInfo -> RequestId -> BackchannelId -> ByteString -> m ()
sendResponse r (WorkerInfo wid expiry) k bid x = do
    -- Store the response data, and notify the client that it's ready.
    run_ r $ set (responseDataKey r k) x [EX expiry]
    run_ r $ publish (responseChannel r bid) (unRequestId k)
    -- Remove the RequestId associated with this response, from the
    -- list of in-progress requests.  This is wrapped in a 'try'
    -- because inProgressKey might point to a dummy value rather than
    -- a list (see handleWorkerFailure).
    let ipk = LKey $ inProgressKey r wid
    removed <- try $ run r $ lrem ipk 1 (toStrict (encode (k, bid)))
    case removed :: Either RedisException Int64 of
        Right 1 -> return ()
        _ -> $logError $
            tshow k <>
            " isn't a member of in-progress queue (" <>
            tshow ipk <>
            "), likely indicating that a heartbeat failure happened, causing\
            \ it to be erroneously re-enqueued.  This doesn't affect\
            \ correctness, but could mean that redundant work is performed."
    -- Remove the request data, as it's no longer needed.  We don't
    -- check if the removal succeeds, as this may not be the first
    -- time a response is sent for the request.  See the error message
    -- above.
    run_ r $ del (unVKey (requestDataKey r k) :| [])

-- | Retrieves the response for the specified 'RequestId'.  This
-- function is usually called in the body of a 'subscribeToResponses'
-- handler, in order to fetch the response after notification of its
-- existence.  It throws a 'ResponseMissing' error if there is no
-- response for the specified 'RequestId'.
readResponse
    :: MonadCommand m
    => RedisInfo -> RequestId -> m ByteString
readResponse r k = do
    mx <- run r $ get (responseDataKey r k)
    case mx of
        Nothing -> liftIO $ throwIO (ResponseMissing k)
        Just x -> return x

-- | Subscribes to responses on the channel specified by this client's
-- 'BackchannelId'.  It changes the @subscribed@ 'TVar' to 'True' when
-- the subscription is established, and 'False' when unsubscribed.  In
-- order to be sure to receive the response, clients should wait for
-- the subscription to be established before enqueueing requests.
subscribeToResponses
    :: MonadConnect m
    => RedisInfo -> ClientInfo -> TVar Bool -> (RequestId -> m ()) -> m void
subscribeToResponses r (ClientInfo bid _) subscribed f = do
    let sub = subscribe (responseChannel r bid :| [])
    withSubscriptionsWrapped (redisConnectInfo r) (sub :| []) $
        trackSubscriptionStatus subscribed $ \_ k ->
            f (RequestId k)

-- | This listens for a notification telling the worker to send a
-- heartbeat.  In this case, that means the worker needs to remove its
-- key from a Redis set.  If this doesn't happen in a timely fashion,
-- then the worker will be considered to be dead, and its work items
-- get re-enqueued.
--
-- The @TVar Bool@ is changed to 'True' once the subscription is made
-- and the 'WorkerId' has been added to the list of active workers.
sendHeartbeats
    :: MonadConnect m => RedisInfo -> WorkerInfo -> TVar Bool -> m void
sendHeartbeats r (WorkerInfo wid _) ready = do
    let sub = subscribe (heartbeatChannel r :| [])
    withSubscriptionsWrapped (redisConnectInfo r) (sub :| []) $ \msg ->
        case msg of
            Subscribe {} -> do
                run_ r $ sadd (activeKey r) (unWorkerId wid :| [])
                atomically $ writeTVar ready True
            Unsubscribe {} ->
                atomically $ writeTVar ready False
            Message {} ->
                remInactive
  where
    remInactive = run_ r $ srem (inactiveKey r) (unWorkerId wid :| [])

-- | Periodically check worker heartbeats.  This uses
-- 'periodicActionWrapped' to share the responsibility of checking the
-- heartbeats amongst multiple client servers.  All invocations of
-- this should use the same time interval.
checkHeartbeats
    :: MonadConnect m => RedisInfo -> Seconds -> m void
checkHeartbeats r ivl =
    periodicActionWrapped (redisConnection r) (heartbeatTimeKey r) ivl $ do
        -- Check if the last iteration of this heartbeat check ran
        -- successfully.  If it did, then we can use the contents of
        -- the inactive list.  The flag also gets set to False here,
        -- such that if a failure happens in the middle, the next run
        -- will know to not use the data.
        functioning <- fmap (fmap (decode . fromStrict)) $
            run r $ getset (heartbeatFunctioningKey r) (toStrict (encode False))
        inactive <- if functioning == Just True
            then do
                -- Fetch the list of inactive workers and move their jobs
                -- back to the requests queue.
                inactive <- run r $ smembers (inactiveKey r)
                mapM_ (handleWorkerFailure r . WorkerId) inactive
                return inactive
            else return []
        -- Remove the inactive workers from the list of workers.
        mapM_ (run_ r . srem (activeKey r)) (nonEmpty inactive)
        -- Populate the list of inactive workers for the next
        -- heartbeat.
        workers <- run r $ smembers (activeKey r)
        run_ r $ del (unSKey (inactiveKey r) :| [])
        mapM_ (run_ r . sadd (inactiveKey r)) (nonEmpty workers)
        -- Ask all of the workers to remove their IDs from the inactive
        -- list.
        -- TODO: Remove this threadDelay (see #26)
        liftIO $ threadDelay (100 * 1000)
        run_ r $ publish (heartbeatChannel r) ""
        -- Record that the heartbeat check was successful.
        run_ r $ set (heartbeatFunctioningKey r) (toStrict (encode True)) []

handleWorkerFailure
    :: (MonadCommand m, MonadLogger m) => RedisInfo -> WorkerId -> m ()
handleWorkerFailure r wid = do
    success <- run r $ (eval script
        [inProgressKey r wid, unLKey (requestsKey r)]
        [] :: CommandRequest Bool)
    when (not success) $
        $logError $ "Failed to read in-progress list for " <> tshow wid <> "."
  where
    script = BS8.unlines
        [ "local xs = redis.pcall('lrange', KEYS[1], 0, -1)"
        , "if xs['err'] then"
        , "    return false"
        , "else"
               -- A step size of 1000 is a conservative choice.
               -- Really this ought to be (LUAI_MAXCSTACK - 2), which
               -- is usually 7998.  However, unfortunately there
               -- doesn't seem to be a way to access LUAI_MAXCSTACK.
        , "    local step = 1000"
        , "    local len = table.getn(xs)"
        , "    for i=1,len,step do"
        , "        local upper = math.min(i + step - 1, len)"
        , "        redis.call('rpush', KEYS[2], unpack(xs, i, upper))"
        , "    end"
        , "    redis.pcall('del', KEYS[1])"
               -- This is rather tricky - the inProgressKey is
               -- replaced with a dummy string value.  This causes
               -- popRequest's invocation of brpoplpush to exit
               -- without causing any mutation.
        , "    redis.pcall('set', KEYS[1], 'done')"
        , "    return true"
        , "end"
        ]

-- | Given a name to start with, this finds a 'WorkerId' which has
-- never been used before.  It also adds the new 'WorkerId' to the set
-- of all worker IDs.
getUnusedWorkerId
    :: MonadCommand m => RedisInfo -> ByteString -> m WorkerId
getUnusedWorkerId r initial = go (0 :: Int)
  where
    go n = do
        let toWord8 = fromIntegral . fromEnum
        postfix <- liftIO $ replicateM n $ randomRIO (toWord8 'a', toWord8 'z')
        let k | n == 0 = initial
              | otherwise = initial <> "-" <> postfix
        numberAdded <- run r $ sadd (workersKey r) (k :| [])
        if numberAdded == 0
            then go (n+1)
            else return (WorkerId k)

-- * Functions to compute Redis keys

-- List of "Data.Binary" encoded @(RequestId, BackchannelId)@.
requestsKey :: RedisInfo -> LKey
requestsKey  r = LKey $ Key $ redisKeyPrefix r <> "requests"

-- Given a 'RequestId', computes the key for the request or response
-- data.
requestDataKey, responseDataKey :: RedisInfo -> RequestId -> VKey
requestDataKey  r k = VKey $ Key $ redisKeyPrefix r <> "request:" <> unRequestId k
responseDataKey r k = VKey $ Key $ redisKeyPrefix r <> "response:" <> unRequestId k

-- Given a 'BackchannelId', computes the name of the 'Channel'.
responseChannel :: RedisInfo -> BackchannelId -> Channel
responseChannel r k =
    Channel $ redisKeyPrefix r <> "responses:" <> unBackchannelId k

inProgressKey :: RedisInfo -> WorkerId -> Key
-- Given a 'WorkerId', computes the key of its in-progress list.
inProgressKey r k = Key $ redisKeyPrefix r <> "in-progress:" <> unWorkerId k

inactiveKey, activeKey, workersKey :: RedisInfo -> SKey
-- A set of 'WorkerId' who have not yet removed their keys (indicating
-- that they're still alive and responding to heartbeats).
inactiveKey r = SKey $ Key $ redisKeyPrefix r <> "heartbeat:inactive"
-- A set of 'WorkerId's that are currently thought to be running.
activeKey r = SKey $ Key $ redisKeyPrefix r <> "heartbeat:active"
-- A set of all 'WorkerId's that have ever been known.
workersKey r = SKey $ Key $ redisKeyPrefix r <> "heartbeat:workers"

-- Stores a "Data.Binary" encoded 'Bool'.
heartbeatFunctioningKey :: RedisInfo -> VKey
heartbeatFunctioningKey r = VKey $ Key $ redisKeyPrefix r <> "heartbeat:functioning"

-- Channel used for requesting that the workers remove their
-- 'WorkerId' from the set at 'inactiveKey'.
heartbeatChannel :: RedisInfo -> Channel
heartbeatChannel r = Channel $ redisKeyPrefix r <> "heartbeat:channel"

-- Prefix used for the 'periodicActionWrapped' invocation, which is
-- used to share the responsibility of periodically checking
-- heartbeats.
heartbeatTimeKey :: RedisInfo -> PeriodicPrefix
heartbeatTimeKey r = PeriodicPrefix $ redisKeyPrefix r <> "heartbeat:time"

-- * Redis utilities

run :: MonadCommand m => RedisInfo -> CommandRequest a -> m a
run = runCommand . redisConnection

run_ :: MonadCommand m => RedisInfo -> CommandRequest a -> m ()
run_ = runCommand_ . redisConnection

-- * Exceptions

data DistributedRedisQueueException
    = ResponseMissing RequestId
    | WorkerHeartbeatFailed WorkerId
    deriving (Eq, Show, Typeable)

instance Exception DistributedRedisQueueException
