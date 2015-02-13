{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
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
    , withRedis
    -- * Request API used by clients
    , pushRequest
    , readResponse
    , subscribeToResponses
    -- * Compute API used by workers
    , popRequest
    , sendResponse
    ) where

import           ClassyPrelude
import           Control.Monad.Logger (MonadLogger, logErrorS)
import qualified Crypto.Hash.SHA1 as SHA1
import           Data.Binary (encode, decode)
import           Data.List.NonEmpty (NonEmpty((:|)))
import           Distributed.RedisQueue.Internal
import           FP.Redis

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
            let encoded = toStrict (encode (RequestInfo k bid))
            run_ r $ lpush (requestsKey r) (encoded :| [])
            return (k, Nothing)

-- | This function is used by the compute workers to take work off of
-- the queue.  When work is taken off the queue, it also gets moved to
-- an active queue specific to the worker, atomically.  This is done
-- so that in the event of server failure, the work items can be
-- re-enqueued.
--
-- If there isn't any work available, 'Nothing' is returned.
--
-- If the request data is missing, then 'RequestMissing' is thrown.
popRequest
    :: (MonadCommand m, MonadThrow m)
    => RedisInfo
    -> WorkerInfo
    -> m (Maybe (RequestId, BackchannelId, ByteString))
popRequest r (WorkerInfo wid _) = do
    mreq <- run r $ rpoplpush (requestsKey r) (activeKey r wid)
    case mreq of
        Nothing -> return Nothing
        Just (decode . fromStrict -> RequestInfo k bid) -> do
            mx <- run r $ get (requestDataKey r k)
            case mx of
                Nothing -> throwM (RequestMissing k)
                Just x -> return (Just (k, bid, x))

-- | Send a response for a particular request.  This is done by the
-- compute workers once they're done with the computation, and have
-- results to send.  Once the response is successfully sent, this also
-- removes the request data, as it's no longer needed.
sendResponse
    :: (MonadCommand m, MonadLogger m)
    => RedisInfo -> WorkerInfo -> RequestId -> BackchannelId -> ByteString -> m ()
sendResponse r (WorkerInfo wid expiry) k bid x = do
    -- Store the response data, and notify the client that it's ready.
    run_ r $ set (responseDataKey r k) x [EX expiry]
    run_ r $ publish (responseChannel r bid) (unRequestId k)
    -- Remove the RequestId associated with this response, from the
    -- list of in-progress requests.
    let ak = activeKey r wid
    removed <- run r $ lrem ak 1 (toStrict (encode (k, bid)))
    if removed == 1
        then return ()
        else $logErrorS "RedisQueue" $
            tshow k <>
            " isn't a member of active queue (" <>
            tshow ak <>
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
    :: (MonadCommand m, MonadThrow m)
    => RedisInfo -> RequestId -> m ByteString
readResponse r k = do
    mx <- run r $ get (responseDataKey r k)
    case mx of
        Nothing -> throwM (ResponseMissing k)
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

-- * Exceptions

data DistributedRedisQueueException
    = ResponseMissing RequestId
    | RequestMissing RequestId
    deriving (Eq, Show, Typeable)

instance Exception DistributedRedisQueueException
