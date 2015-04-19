{-# LANGUAGE NoImplicitPrelude, OverloadedStrings, ScopedTypeVariables, TypeFamilies,
             DeriveDataTypeable, FlexibleContexts, FlexibleInstances, RankNTypes, GADTs,
             ConstraintKinds, NamedFieldPuns #-}

-- | Redis publish/subscribe.
-- See <http://redis.io/commands#pubsub>.

module FP.Redis.PubSub
    ( module FP.Redis.Command.PubSub
    , withSubscriptionsWrapped
    , withSubscriptionsWrappedConn
    , withSubscriptionsEx
    , withSubscriptionsExConn
    , disconnectSub
    , subscribe
    , psubscribe
    , unsubscribe
    , punsubscribe
    , makeSubscription
    , sendSubscription
    , sendSubscriptions )
    where

--TODO SHOULD: use a connection pool, and re-use a single connection for all subscriptions

import ClassyPrelude.Conduit hiding (connect)
import Control.Exception.Lifted (BlockedIndefinitelyOnMVar(..))
import Control.Monad.Logger
import Data.List.NonEmpty (NonEmpty)

import FP.Redis.Connection
import FP.Redis.Internal
import FP.Redis.Types.Internal
import FP.Redis.Command.PubSub

-- | Like 'withSubscriptionsEx', but wraps the callback in an exception handler so it continues
-- to be called for new messages even if it throws an exception.
withSubscriptionsWrapped :: MonadConnect m
                         => ConnectInfo -- ^ Redis connection info
                         -> NonEmpty SubscriptionRequest -- ^ List of subscriptions
                         -> (Message -> m ()) -- ^ Callback to receive messages
                         -> m void
withSubscriptionsWrapped connectionInfo_ subscriptions callback =
    withSubscriptionsWrappedConn connectionInfo_ subscriptions (\_ -> return callback)

-- | This provides the connection being used for the subscription, so
-- that it can be cancelled with 'disconnect'.  Ideally, other
-- approaches would allow this:
-- https://github.com/fpco/libraries/issues/34
withSubscriptionsWrappedConn :: MonadConnect m
                             => ConnectInfo -- ^ Redis connection info
                             -> NonEmpty SubscriptionRequest -- ^ List of subscriptions
                             -> (SubscriptionConnection -> m (Message -> m ())) -- ^ Callback to receive messages
                             -> m void
withSubscriptionsWrappedConn connectionInfo_ subscriptions callback' =
    withSubscriptionsExConn connectionInfo_ subscriptions wrappedCallback
    where
      wrappedCallback conn = do
          callback <- callback' conn
          return $ \msg -> catchAny (callback msg) callbackHandler
      callbackHandler ex =
          logErrorNS (connectLogSource connectionInfo_)
                     ("withSubscriptionsWrappedConn callbackHandler: " ++ tshow ex)

-- | Open a connection that will listen for pub/sub messages.  Since a connection subscribed
-- to any channels switches to a different mode, regular command and subscriptions cannot
-- be multiplexed in a single connection.
--
-- Note: pub/sub channels exist in a namespace that does not consider the 'select'ed database,
-- so be sure to prefix them if you need to separate namespaces.
--
-- TODO MAYBE: Auto-resubscribe to any subscriptions that were active when auto-reconnecting,
-- instead of just the `initialSubscriptions'.
withSubscriptionsEx :: MonadConnect m
                    => ConnectInfo -- ^ Redis connection info
                    -> NonEmpty SubscriptionRequest -- ^ List of subscriptions
                    -> (Message -> m ()) -- ^ Callback to receive messages
                    -> m void
withSubscriptionsEx connectionInfo_ subscriptions callback =
    withSubscriptionsExConn connectionInfo_ subscriptions (\_ -> return callback)

-- | This provides the connection being used for the subscription, so
-- that it can be cancelled with 'disconnect'.  Ideally, other
-- approaches would allow this:
-- https://github.com/fpco/libraries/issues/34
withSubscriptionsExConn :: MonadConnect m
                        => ConnectInfo -- ^ Redis connection info
                        -> NonEmpty SubscriptionRequest -- ^ List of subscriptions
                        -> (SubscriptionConnection -> m (Message -> m ())) -- ^ Callback to receive messages
                        -> m void
withSubscriptionsExConn connectionInfo_ subscriptions callback' = do
    messageChan <- newChan
    bracket
        (SubscriptionConnection <$> connect connectionInfo_
            {connectSubscriptionCallback = Just (writeChan messageChan)
            ,connectInitialSubscriptions =
            connectInitialSubscriptions connectionInfo_ ++ toList subscriptions})
        disconnectSub
        (\conn -> do
            callback <- callback' conn
            forever (loop callback messageChan))
  where
    loop callback messageChan = do
        msg <- catch (readChan messageChan)
                     (\BlockedIndefinitelyOnMVar -> throwM DisconnectedException)
        case decodeMessage msg of
            Just msg' -> callback msg'
            Nothing -> throwM ProtocolException
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

-- | Disconnect from Redis server.  This does /not/ issue a QUIT command
-- because doing so while subscribed is not permitted by the Redis protocol.
disconnectSub :: MonadCommand m => SubscriptionConnection -> m ()
disconnectSub (SubscriptionConnection conn) =
    --TODO: would be cleanest to unsubscribe from all channels then issue a QUIT command.
    disconnect' conn Nothing

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
makeSubscription cmd args =
    SubscriptionRequest (Subscription (renderRequest (encodeArg cmd:(toList args))))
