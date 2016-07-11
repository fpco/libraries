{-# LANGUAGE NoImplicitPrelude, OverloadedStrings, ScopedTypeVariables, TypeFamilies,
             DeriveDataTypeable, FlexibleContexts, FlexibleInstances, RankNTypes, GADTs,
             ConstraintKinds, NamedFieldPuns, BangPatterns, TemplateHaskell #-}

-- | Redis publish/subscribe.
-- See <http://redis.io/commands#pubsub>.

module FP.Redis.PubSub
    ( module FP.Redis.Command.PubSub
    , withSubscriptions
    , withSubscriptionsManaged
    , subscribe
    , psubscribe
    , unsubscribe
    , punsubscribe
    -- , makeSubscription
    , sendSubscription
    , sendSubscriptions
    ) where

--TODO SHOULD: use a connection pool, and re-use a single connection for all subscriptions

import ClassyPrelude.Conduit hiding (connect)
import Control.DeepSeq (deepseq)
import Control.Monad.Logger
import Data.List.NonEmpty (NonEmpty)
import Data.Void (absurd, Void)
import qualified Control.Concurrent.Mesosync.Lifted.Safe as Async
import qualified Data.HashSet as HS

import FP.Redis.Connection
import FP.Redis.Internal
import FP.Redis.Types.Internal
import FP.Redis.Command.PubSub

-- | Simplified version of 'withSubscriptions' that lets us try to recover from
-- connection death better.
--
-- Note that:
-- * Some messages might be lost
-- * The continuation will be called multiple times -- when the connection is reconnected
-- we call it again
-- * The continuation is expected to be interrupted at any time.
withSubscriptionsManaged :: forall m a.
       (MonadConnect m)
    => ConnectInfo
    -> NonEmpty Channel -- ^ Channels to subscribe to
    -> (m (Channel, ByteString) -> m a)
    -> m a
withSubscriptionsManaged cinfo chans cont = go
  where
    chansSet = HS.fromList (toList chans)

    pickRightMsgs waitForMsg = do
        msg <- waitForMsg
        case msg of
            Message chan msgBytes -> if HS.member chan chansSet
                then return (chan, msgBytes)
                else throwM (ProtocolException ("Got message on unsubscribed channel " ++ tshow chan))
            _ -> throwM (ProtocolException ("Excepted Message but got " ++ tshow msg))

    go = do
        -- We retry if we fail _after_ we've sent the initial subscriptions. This should
        -- give us a very good indication that after connecting the connection is healthy,
        -- and we won't retry forever.
        --
        -- REVIEW TODO: unsubscribe at the end.
        mbIoErr <-
            withConnection cinfo $ \conn -> withSubscriptionsInternal conn $ \subConn waitForMsg -> do
                -- Subscribe to all channels
                sendSubscription subConn (subscribe chans)
                -- Wait for all channels messages
                forM_ chans $ \chan -> do
                    msg <- waitForMsg
                    case msg of
                        Subscribe chan' _count | chan == chan' -> return ()
                        _ -> throwM (ProtocolException ("Excepted Subscribe " ++ tshow chan ++ " Message, but got " ++ tshow msg))
                -- Continue
                cont (pickRightMsgs waitForMsg)
        case mbIoErr of
            Right x -> return x
            Left err -> do
                $logWarn ("Got IOError " ++ tshow err ++ " in subscribed connection, retrying")
                go

-- Note that this will fail as soon as the redis connection fails, and won't
-- recover. You'll have to implement your own retrying mechanisms.
--
-- The 'SubscriptionConnection' is _not_ thread safe
withSubscriptions ::
       (MonadConnect m)
    => ConnectInfo
    -> (SubscriptionConnection -> m Message -> m a)
    -- ^ The provided action blocks until a message is present
    -> m a
withSubscriptions cinfo cont =
    either throwM return =<< withConnection cinfo (\conn -> withSubscriptionsInternal conn cont)

withSubscriptionsInternal :: forall m a.
       (MonadConnect m)
    => Connection
    -> (SubscriptionConnection -> m Message -> m a)
    -> m (Either IOError a)
withSubscriptionsInternal conn cont = do
    chan :: TChan Message <- liftIO newTChanIO
    let getMsg = atomically (readTChan chan)
    res :: Either (Either IOError Void) a <- Async.race (try (go chan)) (cont (SubscriptionConnection conn) getMsg)
    return (either (Left . either id absurd) Right res)
  where
    go chan = forever $ do
        resp <- readResponse conn
        msg <- case resp of
            Array (Just message) -> case decodeMessage message of
                Nothing -> throwM (ProtocolException "Could not decode subscription Message")
                Just msg -> return msg
            _ -> throwM (ProtocolException ("Expecting Array, got " ++ tshow resp))
        atomically (writeTChan chan msg)

    decodeMessage [BulkString (Just "subscribe"),channel,subscriptionCount] =
        case (decodeResponse channel, decodeResponse subscriptionCount) of
            (Just channel', Just subscriptionCount') ->
                Just (Subscribe channel' subscriptionCount')
            _ -> Nothing
    decodeMessage [BulkString (Just "unsubscribe"),channel,subscriptionCount] =
        case (decodeResponse channel, decodeResponse subscriptionCount) of
            (Just channel', Just subscriptionCount') ->
                Just (Unsubscribe channel' subscriptionCount')
            _ -> Nothing
    decodeMessage [BulkString (Just "message"),channel,payload] =
        case (decodeResponse channel, decodeResponse payload) of
            (Just channel', Just payload') ->
                Just (Message channel' payload')
            _ -> Nothing
    decodeMessage _ = Nothing

-- | Send multiple subscription commands.
sendSubscriptions :: ( MonoFoldable (t SubscriptionRequest)
                     , MonadCommand m
                     , Element (t SubscriptionRequest) ~ SubscriptionRequest )
                  => SubscriptionConnection -- ^ Connection
                  -> t SubscriptionRequest -- ^ Subscription commands
                  -> m ()
sendSubscriptions conn = mapM_ (sendSubscription conn)

-- | Send a subscription command.
sendSubscription :: (MonadCommand m)
                 => SubscriptionConnection -- ^ Connection
                 -> SubscriptionRequest -- ^ Subscription command
                 -> m ()
sendSubscription (SubscriptionConnection conn) (SubscriptionRequest request) =
    writeRequest conn request

-- | Subscribes the client to the specified channels.
subscribe :: NonEmpty Channel -- ^ Channels
          -> SubscriptionRequest
subscribe channels = makeSubscription "SUBSCRIBE" (fmap encodeArg (toList channels))

-- | Subscribes the client to the given patterns.
psubscribe :: NonEmpty ByteString -- ^ Patterns
           -> SubscriptionRequest
psubscribe channelPatterns = makeSubscription "PSUBSCRIBE" (fmap encodeArg (toList channelPatterns))

-- | Unsubscribes the client from the given channels, or from all of them if none is given.
unsubscribe :: [Channel] -- ^ Channels
            -> SubscriptionRequest
unsubscribe channels = makeSubscription "UNSUBSCRIBE" (fmap encodeArg channels)

-- | Unsubscribes the client from the given patterns, or from all of them if none is given.
punsubscribe :: [ByteString] -- Patterns
             -> SubscriptionRequest
punsubscribe channelPatterns = makeSubscription "PUNSUBSCRIBE" (fmap encodeArg channelPatterns)

-- | Make a subscription request
makeSubscription :: ByteString -> [ByteString] -> SubscriptionRequest
makeSubscription !cmd !args =
    deepseq args $
    SubscriptionRequest (renderRequest (encodeArg cmd : args))
