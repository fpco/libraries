{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE ViewPatterns #-}

-- | This module contains definitions used by
-- "Distributed.JobQueue.Worker" and "Distributed.JobQueue.Client"
module Distributed.JobQueue.Shared
    ( JobRequest(..)
    , notifyRequestAvailable
    , requestChannel
    , DistributedJobQueueException(..)
    , wrapException
    ) where

import ClassyPrelude
import Data.Binary (Binary)
import Data.ConcreteTypeRep (ConcreteTypeRep)
import Data.Streaming.NetworkMessage (NetworkMessageException)
import Data.Text.Binary ()
import Data.Typeable (typeOf)
import Distributed.RedisQueue
import Distributed.RedisQueue.Internal (run_)
import FP.Redis

data JobRequest = JobRequest
    { jrRequestType, jrResponseType :: ConcreteTypeRep
    , jrBody :: ByteString
    } deriving (Generic, Show, Typeable)

instance Binary JobRequest

notifyRequestAvailable :: MonadCommand m => RedisInfo -> m ()
notifyRequestAvailable r = run_ r $ publish (requestChannel r) ""

-- | 'Channel' which is used to notify idle workers that there is a new
-- client request or slave request available.
requestChannel :: RedisInfo -> Channel
requestChannel r = Channel $ redisKeyPrefix r <> "request-channel"

-- * Exceptions

-- | Exceptions which are returned to the client by the job-queue.
data DistributedJobQueueException
    = WorkStillInProgress WorkerId
    -- ^ Thrown when the worker stops being a master but there's still
    -- work on its active queue.  This occuring indicates an error in
    -- the library.
    | RequestMissingException RequestId
    -- ^ Exception thrown when a worker can't find the request body.
    -- This means that the request body expired in redis
    -- (alternatively, it could indicate a bug in this library).
    | ResponseMissingException RequestId
    -- ^ Exception thrown when the client can't find the response
    -- body. This means that the response body expired in redis
    -- (alternatively, it could indicate a bug in this library).
    | TypeMismatch
        { expectedRequestType :: ConcreteTypeRep
        , actualRequestType :: ConcreteTypeRep
        , expectedResponseType :: ConcreteTypeRep
        , actualResponseType :: ConcreteTypeRep
        }
    -- ^ Thrown when the client makes a request with the wrong request
    -- / response types.
    | NetworkMessageException NetworkMessageException
    -- ^ Exceptions thrown by "Data.Streaming.NetworkMessage"
    | OtherException Text Text
    -- ^ This is used to return exceptions to the client, when
    -- exceptions occur while running the job.
    deriving (Eq, Typeable, Generic)

instance Exception DistributedJobQueueException
instance Binary DistributedJobQueueException

instance Show DistributedJobQueueException where
    show (WorkStillInProgress wid) =
        "WorkStillInProgress (" ++
        show wid ++
        ") {- This indicates a bug in the work queue library. -}"
    show (RequestMissingException wid) =
        "RequestMissingException (" ++
        show wid ++
        ") {- This likely means that the request body expired in redis. -}"
    show (ResponseMissingException rid) =
        "ResponseMissingException (" ++
        show rid ++
        ") {- This likely means that the response body expired in redis. -}"
    show (TypeMismatch {..}) =
        "TypeMismatch { " ++
        "expectedResponseType = " ++ show expectedResponseType ++
        "actualResponseType = " ++ show actualResponseType ++
        "expectedRequestType = " ++ show expectedRequestType ++
        "actualRequestType = " ++ show actualRequestType ++
        " }"
    show (NetworkMessageException nme) =
        "NetworkMessageException (" ++ show nme ++ ")"
    show (OtherException ty txt) =
        "OtherException " ++ show ty ++ " " ++ show txt

wrapException :: SomeException -> DistributedJobQueueException
wrapException ex =
    case ex of
        (fromException -> Just err) -> err
        (fromException -> Just err) -> NetworkMessageException err
        _ -> OtherException (tshow (typeOf ex)) (tshow ex)