{-# LANGUAGE NoImplicitPrelude, OverloadedStrings, ScopedTypeVariables, TypeFamilies,
             DeriveDataTypeable, FlexibleContexts, FlexibleInstances, RankNTypes, GADTs,
             ConstraintKinds, NamedFieldPuns, GeneralizedNewtypeDeriving,
             DeriveGeneric #-}

-- | Redis internal types.

module FP.Redis.Types.Internal where

import ClassyPrelude.Conduit hiding (Builder)
import Blaze.ByteString.Builder (Builder)
import Control.Monad.Logger
import Control.Monad.Trans.Unlift (MonadBaseUnlift)
import qualified Data.ByteString.Char8 as BS8
import Data.Data (Data)
import qualified Network.Socket as NS
import Control.Retry (RetryPolicy)

-- | Monads for connecting.
type MonadConnect m = (MonadCommand m, MonadLogger m, MonadCatch m, MonadMask m)

-- | Monads for running commands.
type MonadCommand m = (MonadIO m, MonadBaseUnlift IO m)

-- Newtype wrapper for redis top level key names.
newtype Key = Key { unKey :: ByteString }
    deriving (Eq, Show, Ord, Data, Typeable, Generic, Result, Argument, IsString)

-- Key which is known to refer to a "FP.Redis.Command.String"
newtype VKey = VKey { unVKey :: Key }
    deriving (Eq, Show, Ord, Data, Typeable, Generic, Result, Argument, IsString)

-- Key which is known to refer to a "FP.Redis.Command.List"
newtype LKey = LKey { unLKey :: Key }
    deriving (Eq, Show, Ord, Data, Typeable, Generic, Result, Argument, IsString)

-- Key which is known to refer to a "FP.Redis.Command.Hash"
newtype HKey = HKey { unHKey :: Key }
    deriving (Eq, Show, Ord, Data, Typeable, Generic, Result, Argument, IsString)

-- Key which is known to refer to a "FP.Redis.Command.Set"
newtype SKey = SKey { unSKey :: Key }
    deriving (Eq, Show, Ord, Data, Typeable, Generic, Result, Argument, IsString)

-- Key which is known to refer to a "FP.Redis.Command.SortedSet"
newtype ZKey = ZKey { unZKey :: Key }
    deriving (Eq, Show, Ord, Data, Typeable, Generic, Result, Argument, IsString)

-- Newtype wrapper for redis channel names.
newtype Channel = Channel { unChannel :: ByteString }
    deriving (Eq, Show, Ord, Data, Typeable, Generic, Result, Argument, IsString, Hashable)

-- Newtype wrapper for redis hash fields.
newtype HashField = HashField { unHashField :: ByteString }
    deriving (Eq, Show, Ord, Data, Typeable, Generic, Result, Argument, IsString)

-- Newtype wrapper time delta (usually a timeout), stored in seconds.
newtype Seconds = Seconds { unSeconds :: Int64 }
    deriving (Eq, Show, Ord, Data, Typeable, Generic, Result, Argument)

-- Newtype wrapper time delta (usually a timeout), stored in
-- milliseconds.
newtype Milliseconds = Milliseconds { unMilliseconds :: Int64 }
    deriving (Eq, Show, Ord, Data, Typeable, Generic, Result, Argument)

-- | A pub/sub subscription message.  See <http://redis.io/topics/pubsub>.
data Message = Subscribe Channel Int64
             | Unsubscribe Channel Int64
             | Message Channel ByteString
    deriving (Eq, Show, Ord, Data, Typeable, Generic)

-- | Options for 'set'.
data SetOption = EX Seconds -- ^ Set the specified expire time, in seconds
               | PX Milliseconds -- ^ Set the specified expire time, in milliseconds
               | NX -- ^ Only set the key if it does not already exist
               | XX -- ^ Only set the key if it already exists
    deriving (Eq, Show, Ord, Data, Typeable, Generic)

-- | Connection to the Redis server used for pub/sub subscriptions.
newtype SubscriptionConnection = SubscriptionConnection Connection
    deriving (Typeable, Generic)

-- | Requests used by pub/sub subscription connections.
newtype SubscriptionRequest = SubscriptionRequest {unSubscriptionRequest :: Request}

-- | Information to connect to Redis server.
data ConnectInfo = ConnectInfo
    { -- | Server's hostname/IP address
      connectHost                 :: !ByteString
      -- | Server's port (default 6379)
    , connectPort                 :: !Int
      -- | Log source string for MonadLogger messages
    , connectLogSource            :: !Text
    , connectRetryPolicy          :: !(Maybe RetryPolicy)
    } deriving (Typeable, Generic)

-- | Connection to the Redis server used for regular commands.
--
-- This connection is _not_ thread safe.
data Connection = Connection
    { connectionInfo_ :: !ConnectInfo
        -- ^ Original connection information
    , connectionSocket :: !NS.Socket
    , connectionLeftover :: !(IORef ByteString)
    } deriving (Typeable, Generic)

data CommandRequest a = (Result a) => CommandRequest !Request

type Request = Builder

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

instance Result (Maybe Double) where
    decodeResponse (SimpleString bs) = Just <$> readMay (BS8.unpack bs)
    decodeResponse (BulkString (Just bs)) = Just <$> readMay (BS8.unpack bs)
    decodeResponse (BulkString Nothing) = Just Nothing
    decodeResponse _ = Nothing
    encodeResponse = BulkString . fmap (BS8.pack . show)

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

instance Result [Key] where
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
    deriving (Eq, Show, Ord, Data, Typeable, Generic)

-- | Exceptions thrown by Redis connection.
data RedisException = ProtocolException Text
                    -- ^ Invalid data received for protocol
                    | CommandException ByteString
                    -- ^ The server reported an error for your command
                    | DecodeResponseException ByteString Response
                    -- ^ Response couldn't decoded to desired type.
                    -- The bytestring is the request which caused the
                    -- decoding error.
    deriving (Show, Typeable, Generic)

instance Exception RedisException
