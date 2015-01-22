{-# LANGUAGE NoImplicitPrelude, OverloadedStrings, ScopedTypeVariables, TypeFamilies,
             DeriveDataTypeable, FlexibleContexts, FlexibleInstances, RankNTypes, GADTs,
             ConstraintKinds, NamedFieldPuns, GeneralizedNewtypeDeriving #-}

-- | Redis internal types.

module FP.Redis.Types.Internal where

import ClassyPrelude.Conduit hiding (Builder)
import Blaze.ByteString.Builder (Builder)
import Control.Concurrent.Async (Async)
import Control.Concurrent.BroadcastChan
import Control.Monad.Catch (MonadCatch)
import Control.Monad.Logger
import Control.Retry

import Control.Concurrent.STM.TSQueue

-- | Monads for connecting.
type MonadConnect m = (MonadCommand m, MonadLogger m, MonadCatch m)

-- | Monads for running commands.
type MonadCommand m = (MonadIO m, MonadBaseControl IO m)

-- Newtype wrapper for redis top level key names.
newtype Key = Key ByteString
    deriving (Eq, Show, Ord, Result, Argument, IsString)

-- Newtype wrapper for redis channel names.
newtype Channel = Channel ByteString
    deriving (Eq, Show, Ord, Result, Argument, IsString)

-- Newtype wrapper for redis hash fields.
newtype HashField = HashField ByteString
    deriving (Eq, Show, Ord, Result, Argument, IsString)

-- Newtype wrapper time delta (usually a timeout), stored in seconds.
newtype Seconds = Seconds Int64
    deriving (Eq, Show, Ord, Result, Argument)

-- Newtype wrapper time delta (usually a timeout), stored in
-- milliseconds.
newtype Milliseconds = Milliseconds Int64
    deriving (Eq, Show, Ord, Result, Argument)

-- | A pub/sub subscription message.  See <http://redis.io/topics/pubsub>.
data Message = Subscribe Channel Int64
             | Unsubscribe Channel Int64
             | Message Channel ByteString
    deriving (Show)

-- | Options for 'set'.
data SetOption = EX Seconds -- ^ Set the specified expire time, in seconds
               | PX Milliseconds -- ^ Set the specified expire time, in milliseconds
               | NX -- ^ Only set the key if it does not already exist
               | XX -- ^ Only set the key if it already exists

-- | Used to notify main thread that connection is ready.
type ConnectionMVar = MVar (Either SomeException (Async () -> Connection))

-- | Connection mode.
data Mode = Normal -- ^ Normal connection that receives commands and returns responses.
          | Subscribed -- ^ Connection that is subscribed to pub/sub channels.
    deriving (Show)

-- | Queue of requests.
type RequestQueue = TSQueue Request

-- | Queue of requests that are still awaiting responses.
type PendingResponseQueue = TSQueue Request

-- | Connection to the Redis server used for pub/sub subscriptions.
data SubscriptionConnection = SubscriptionConnection Connection (BroadcastChan In [Response])

-- | Requests used by pub/sub subscription connections.
newtype SubscriptionRequest = SubscriptionRequest {unSubscriptionRequest :: Request}

-- | Information to connect to Redis server.
data ConnectInfo = ConnectInfo
    { -- | Server's hostname/IP address
      connectHost                 :: !ByteString
      -- | Server's port (default 6379)
    , connectPort                 :: !Int
      -- | Initial commands to send on connection.  These will be re-sent if the connection needs
      -- to be re-established.  Most commonly used would be 'auth' and 'select'.  Use 'ignoreResult'
      -- if the command you need doesn't return '()'.
    , connectInitialCommands      :: [CommandRequest ()]
      -- | Initial subscriptions for connection.  These will be re-subscribed if the connection
      -- needs to be re-established.
    , connectInitialSubscriptions :: [SubscriptionRequest]
      -- | TODO: hide this
    , connectSubscriptionCallback :: !(Maybe ([Response] -> IO ()))
      -- | Retry policy for reconnecting if a connection is lost.
    , connectRetryPolicy          :: !(Maybe RetryPolicy)
      -- | Log source string for MonadLogger messages
    , connectLogSource            :: !Text
      -- | Maximum number of requests to send together in a batch.  Should be less than
      -- 'connectMaxPendingResponses'.
    , connectRequestsPerBatch     :: !Int
      -- | Maximum number of pending responses in the pipeline before blocking new requests.
    , connectMaxPendingResponses  :: !Int }

-- | Connection to the Redis server used for regular commands.
data Connection = Connection
    { connectionInfo_ :: !ConnectInfo
        -- ^ Original connection information
    , connectionRequestQueue :: !RequestQueue
        -- ^ Queue of requests pending being sent
    , connectionPendingResponseQueue :: !PendingResponseQueue
        -- ^ Queue of requests awaiting a response
    , connectionThread :: !(Async ())
        -- ^ Thread that manages the connection
    }

-- | Regular command request.
data CommandRequest a = (Result a) => CommandRequest (ResponseCallback -> Request)
                      | CommandPure a
                      | forall x. CommandAp (CommandRequest (x -> a)) (CommandRequest x)

instance Functor CommandRequest where
    fmap f (CommandPure x) = CommandPure (f x)
    fmap f (CommandAp x y) = CommandAp (fmap (f.) x) y
    fmap f x = CommandAp (CommandPure f) x

instance Applicative CommandRequest where
    pure = CommandPure
    (<*>) = CommandAp

-- | A request to Redis
data Request = Command !Builder ResponseCallback
                  -- ^ A normal command request that expects a response
             | Subscription !Builder
                  -- ^ A subscription request

-- | Callback to receive responses.  This will be called with asynchronous exceptions masked.
type ResponseCallback = (IO () -> IO ()) -- ^ Restore async exceptions
                      -> Response -- ^ The response
                      -> IO ()

-- | Types that can be passed as arguments to Redis commands.
class Argument a where
    encodeArg :: a -> ByteString -- ^ Encode argument to ByteString

instance Argument ByteString where
    encodeArg = id

instance Argument Int64 where
    encodeArg = encodeUtf8 . tshow

instance Argument Double where
    encodeArg = encodeUtf8 . tshow

-- | Types that Redis responses can be converted to.
class Result a where
    decodeResponse :: Response -> Maybe a -- ^ Decode from Redis 'Response'
    encodeResponse :: a -> Response -- ^ Encode to Redis 'Response'

instance Result Response where
    decodeResponse = Just
    encodeResponse = id

instance Result ByteString where
    decodeResponse (SimpleString result) = Just result
    decodeResponse (BulkString (Just result)) = Just result
    decodeResponse _ = Nothing
    encodeResponse = BulkString . Just

instance Result Bool where
    decodeResponse (SimpleString "OK") = Just True
    decodeResponse (Integer 0) = Just False
    decodeResponse (Integer 1) = Just True
    decodeResponse (BulkString Nothing) = Just False
    decodeResponse (Array Nothing) = Just False
    decodeResponse _ = Nothing
    encodeResponse b = if b then Integer 1 else BulkString Nothing

instance Result (Maybe ByteString) where
    decodeResponse (SimpleString bs) = Just (Just bs)
    decodeResponse (BulkString mbs) = Just mbs
    decodeResponse _ = Nothing
    encodeResponse = BulkString

instance Result Int64 where
    decodeResponse (Integer val) = Just val
    decodeResponse _ = Nothing
    encodeResponse = Integer

instance (Result a, Result b) => Result (Maybe (a, b)) where
    decodeResponse (Array Nothing) =
        Just Nothing
    decodeResponse (Array (Just [key, value])) = do
        key' <- decodeResponse key
        value' <- decodeResponse value
        return $ Just (key', value')
    decodeResponse _ =
        Nothing
    encodeResponse mp =
        case mp of
            Nothing -> Array Nothing
            Just (a,b) -> Array (Just [encodeResponse a
                                      ,encodeResponse b])

instance Result [ByteString] where
    decodeResponse (Array (Just vals)) =
        let maybeVals = map decodeResponse vals
        in if any isNothing maybeVals
            then Nothing
            else Just (catMaybes maybeVals)
    decodeResponse _ = Nothing
    encodeResponse xs = Array (Just (map encodeResponse xs))

instance Result [Maybe ByteString] where
    decodeResponse (Array (Just vals)) =
        Just (map dr vals)
      where
        dr val = case decodeResponse val of
                     Nothing -> Nothing
                     Just d -> d
    decodeResponse _ = Nothing
    encodeResponse xs = Array (Just (map encodeResponse xs))

instance Result () where
    decodeResponse _ = Just ()
    encodeResponse _ = BulkString Nothing

-- | Low-level representation of responses from the Redis server.
data Response = SimpleString ByteString
              | Error ByteString
              | Integer Int64
              | BulkString (Maybe ByteString)
              | Array (Maybe [Response])
    deriving (Eq, Show)

-- | Exceptions thrown by Redis connection.
data RedisException = ConnectionFailedException SomeException -- ^ Unable to connect to server
                    | DisconnectedException -- ^ Unexpected disconnection from server
                    | ProtocolException -- ^ Invalid data received for protocol
                    | CommandException ByteString -- ^ The server reported an error for your command
                    | DecodeResponseException -- ^ Response couldn't decoded to desired type
    deriving (Show, Typeable)

instance Exception RedisException
