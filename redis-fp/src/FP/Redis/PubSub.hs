{-# LANGUAGE NoImplicitPrelude, OverloadedStrings, ScopedTypeVariables, TypeFamilies,
             DeriveDataTypeable, FlexibleContexts, FlexibleInstances, RankNTypes, GADTs,
             ConstraintKinds, NamedFieldPuns, BangPatterns #-}

-- | Redis publish/subscribe.
-- See <http://redis.io/commands#pubsub>.

module FP.Redis.PubSub
    ( module FP.Redis.Command.PubSub
    , withSubscriptions
    , subscribe
    , psubscribe
    {-
    , unsubscribe
    , punsubscribe
    , makeSubscription
    , sendSubscription
    , sendSubscriptions
    -}
    ) where

--TODO SHOULD: use a connection pool, and re-use a single connection for all subscriptions

import ClassyPrelude.Conduit hiding (connect)
import Control.DeepSeq (deepseq)
import Control.Exception.Lifted (BlockedIndefinitelyOnMVar(..))
import Control.Monad.Logger
import Data.List.NonEmpty (NonEmpty)
import Data.Void (absurd)
import qualified Control.Concurrent.Async.Lifted.Safe as Async

import FP.Redis.Connection
import FP.Redis.Internal
import FP.Redis.Types.Internal
import FP.Redis.Command.PubSub

-- Note that this will fail as soon as the redis connection fails, and won't
-- recover. You'll have to implement your own retrying mechanisms.
--
-- TODO implement a function that reconnets
-- TODO properly unsubscribe and close the connection
withSubscriptions ::
       (MonadConnect m)
    => ConnectInfo
    -> NonEmpty SubscriptionRequest
    -> (m Message -> m a) -- ^ The provided action blocks until a message is present
    -> m a
withSubscriptions cinfo subscriptions cont = bracket
    (connect cinfo)
    disconnectNoQuit -- Subscribed connection don't support QUIT
    (\conn -> do
        mapM_ (writeRequest conn . unSubscriptionRequest) subscriptions
        chan :: TChan Message <- liftIO newTChanIO
        let getMsg = atomically (readTChan chan)
        fmap (either id absurd) (Async.race (cont getMsg) (go conn chan)))
  where
    go conn chan = forever $ do
        resp <- readResponse conn
        msg <- case resp of
            Array (Just message) -> case decodeMessage message of
                Nothing -> throwM ProtocolException -- TODO more informative message
                Just msg -> return msg
            _ -> throwM ProtocolException -- TODO more informative message
        atomically (writeTChan chan msg)

    -- TODO this is a bit weird -- we probably want to error out on unsubscribe,
    -- and ignore subscribe.
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

--TODO SHOULD: Resurrect this code when we have support for re-establishing subscriptions after a
--reconnect.
{-}
-- | Open a connection to listen for pub/sub messages
withSubscriptionConnection :: forall m a. ( MonadCommand m
                                          , MonadLogger m
                                          , MonadCatch m )
                           => ConnectInfo -- ^ Server connection info
                           -> (SubscriptionConnection -> m a) -- ^ Inner action
                           -> m a
withSubscriptionConnection cinfo inner = do
    messageBChan <- liftIO newBroadcastChan
    withConnection
        (cinfo{connectSubscriptionCallback = Just (writeBChan messageBChan)})
        (\conn -> inner (SubscriptionConnection conn messageBChan))

-- | Listen for pub/sub messages.
listenSubscriptionMessages :: forall m. (MonadIO m)
                           => SubscriptionConnection -- ^ Server connection
                           -> ([Response] -> m ()) -- ^ Callback to receive messages
                           -> m ()
listenSubscriptionMessages (SubscriptionConnection _ messageBChan) callback = do
    --TODO: TEST connection dies and can't recover
    listener <- liftIO (newBChanListener messageBChan)
    forever (callback =<< liftIO (readBChan listener))
--}

{-
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
    sendRequest conn request
-}

-- | Subscribes the client to the specified channels.
subscribe :: NonEmpty Channel -- ^ Channels
          -> SubscriptionRequest
subscribe channels = makeSubscription "SUBSCRIBE" (fmap encodeArg (toList channels))

-- | Subscribes the client to the given patterns.
psubscribe :: NonEmpty ByteString -- ^ Patterns
           -> SubscriptionRequest
psubscribe channelPatterns = makeSubscription "PSUBSCRIBE" (fmap encodeArg (toList channelPatterns))

{-
-- | Unsubscribes the client from the given channels, or from all of them if none is given.
unsubscribe :: [Channel] -- ^ Channels
            -> SubscriptionRequest
unsubscribe channels = makeSubscription "UNSUBSCRIBE" (fmap encodeArg channels)

-- | Unsubscribes the client from the given patterns, or from all of them if none is given.
punsubscribe :: [ByteString] -- Patterns
             -> SubscriptionRequest
punsubscribe channelPatterns = makeSubscription "PUNSUBSCRIBE" (fmap encodeArg channelPatterns)
-}

-- | Make a subscription request
makeSubscription :: ByteString -> [ByteString] -> SubscriptionRequest
makeSubscription !cmd !args =
    deepseq args $
    SubscriptionRequest (renderRequest (encodeArg cmd : args))
