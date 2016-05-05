{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

-- REVIEW TODO: Add explicit exports or move types to relevant modules
module Distributed.Types where

import           ClassyPrelude hiding ((<>))
import qualified Data.Aeson as Aeson
import qualified Data.ByteString.Char8 as BS8
import           Data.Serialize (Serialize)
import           Data.Serialize.Orphans ()
import           Data.Streaming.NetworkMessage (NetworkMessageException)
import qualified Data.Text as T
import           Data.TypeFingerprint

-- * IDs used in Redis

-- | Every worker has a 'WorkerId' to uniquely identify it. It's needed
-- for the fault tolerance portion - in the event that a worker goes
-- down we need to be able to re-enqueue its work.
--
-- REVIEW: This is used only for job queue, not work queue.
newtype WorkerId = WorkerId { unWorkerId :: ByteString }
    deriving (Eq, Ord, Show, Serialize, IsString, Typeable, Hashable)

instance Aeson.ToJSON WorkerId where
    toJSON = Aeson.String . T.pack . BS8.unpack . unWorkerId

-- | This is the key used for enqueued requests, and, later, the
-- response associated with it. It's the hash of the request, which
-- allows responses to be cached.
--
-- REVIEW: This is used only for job queue, not work queue.
newtype RequestId = RequestId { unRequestId :: ByteString }
    deriving (Eq, Ord, Show, Serialize, Hashable, Typeable)

instance Aeson.ToJSON RequestId where
    toJSON = Aeson.String . T.pack . BS8.unpack . unRequestId

-- * Job-queue

{-

-- * Information for connecting to a worker

data WorkerConnectInfo = WorkerConnectInfo
    { wciHost :: !ByteString
    , wciPort :: !Int
    }
    deriving (Eq, Show, Ord, Generic, Typeable)

instance Serialize WorkerConnectInfo
-}

-- * Exceptions

-- | Exceptions which are returned to the client by the job-queue.
data DistributedException
    = WorkStillInProgress !WorkerId
    -- ^ Thrown when the worker stops being a master but there's still
    -- work on its active queue.  This occuring indicates an error in
    -- the library.
    | RequestMissingException !RequestId
    -- ^ Exception thrown when a worker can't find the request body.
    -- This means that the request body expired in redis
    -- (alternatively, it could indicate a bug in this library).
    | ResponseMissingException !RequestId
    -- ^ Exception thrown when the client can't find the response
    -- body. This means that the response body expired in redis
    -- (alternatively, it could indicate a bug in this library).
    | TypeMismatch
        { expectedRequestTypeFingerprint :: !TypeFingerprint
        , actualRequestTypeFingerprint :: !TypeFingerprint
        , expectedResponseTypeFingerprint :: !TypeFingerprint
        , actualResponseTypeFingerprint :: !TypeFingerprint
        }
    -- ^ Thrown when the client makes a request with the wrong request
    -- / response types.
    | NoRequestForCallbackRegistration !RequestId
    -- ^ Exception thrown when registering a callback for a non-existent
    -- request.
    | MismatchedRedisSchemaVersion
        { expectedRedisSchemaVersion :: !ByteString
        , actualRedisSchemaVersion :: !ByteString
        }
    -- ^ Exception thrown on initialization of work-queue
    | MismatchedRequestRedisSchemaVersion
        { expectedRequestRedisSchemaVersion :: !ByteString
        , actualRequestRedisSchemaVersion :: !ByteString
        , schemaMismatchRequestId :: !RequestId
        }
    -- ^ Exception thrown when request is received with the wrong schema
    -- version.
    | NoLongerWaitingForResult
    -- ^ Exception thrown by job-queue client functions that block
    -- waiting for response.
    | NoLongerWaitingForRequest
    -- ^ Exception thrown by worker functions that block waiting for
    -- request.
    | NoLongerWaitingForWorkerRequest
    -- ^ Exception thrown by worker functions that block waiting for
    -- worker requests.
    | RequestCanceled !RequestId
    -- ^ Exception returned to the user when the request has been
    -- cancelled, and is no longer being worked on.
    | NetworkMessageException !NetworkMessageException
    -- ^ Exceptions thrown by "Data.Streaming.NetworkMessage"
    | InternalJobQueueException !Text
    -- ^ Used for unexpected conditions.
    | DecodeError
        { decodeErrorLocation :: !Text
        , decodeErrorError :: !Text
        }
    -- ^ Used when we couldn't decode some binary blob.
    -- Since the users of 'decodeOrThrow' attempt to ensure that types
    -- and executable hashes match up, the occurance of this exception
    -- indicates a bug in the library.
    | OtherException !Text !Text
    -- ^ This is used to return exceptions to the client, when
    -- exceptions occur while running the job.
    deriving (Eq, Typeable, Generic)

instance Exception DistributedException
instance Serialize DistributedException

instance Show DistributedException where
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
        "TypeMismatch " ++
        "{ expectedResponseTypeFingerprint = " ++ show expectedResponseTypeFingerprint ++
        ", actualResponseTypeFingerprint = " ++ show actualResponseTypeFingerprint ++
        ", expectedRequestTypeFingerprint = " ++ show expectedRequestTypeFingerprint ++
        ", actualRequestTypeFingerprint = " ++ show actualRequestTypeFingerprint ++
        "}"
    show (NoRequestForCallbackRegistration rid) =
        "NoRequestForCallbackRegistration (" ++
        show rid ++
        ")"
    show (MismatchedRedisSchemaVersion {..}) =
        "MismatchedRedisSchemaVersion " ++
        "{ expectedRedisSchemaVersion = " ++ show expectedRedisSchemaVersion ++
        ", actualRedisSchemaVersion = " ++ show actualRedisSchemaVersion ++
        "}"
    show (MismatchedRequestRedisSchemaVersion {..}) =
        "MismatchedRequestRedisSchemaVersion " ++
        "{ expectedRequestRedisSchemaVersion = " ++ show expectedRequestRedisSchemaVersion ++
        ", actualRequestRedisSchemaVersion = " ++ show actualRequestRedisSchemaVersion ++
        ", schemaMismatchRequestId = " ++ show schemaMismatchRequestId ++
        "}"
    show NoLongerWaitingForResult = concat
        [ "NoLongerWaitingForResult "
        , "{- This indicates that the jobQueueClient threads are no longer running.  "
        , "This is usually because lost the connection to redis, and couldn't reconnect. -}"
        ]
    show NoLongerWaitingForRequest = concat
        [ "NoLongerWaitingForRequest "
        , "{- This is usually because we lost the connection to redis, and couldn't reconnect. -}"
        ]
    show NoLongerWaitingForWorkerRequest = concat
        [ "NoLongerWaitingForWorkerRequest "
        , "{- This is usually because we lost the connection to redis, and couldn't reconnect. -}"
        ]
    show (RequestCanceled rid) =
        "RequestCanceled (" ++ show rid ++ ")"
    show (NetworkMessageException nme) =
        "NetworkMessageException (" ++ show nme ++ ")"
    show (InternalJobQueueException txt) =
        "InternalJobQueueException " ++ show txt
    show (OtherException ty txt) =
        "OtherException " ++ show ty ++ " " ++ show txt
    show (DecodeError loc err) =
        "DecodeError " ++ show loc ++ " " ++ show err