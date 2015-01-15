{-# LANGUAGE NoImplicitPrelude, OverloadedStrings, ScopedTypeVariables, TypeFamilies,
             DeriveDataTypeable, FlexibleContexts, FlexibleInstances, RankNTypes, GADTs,
             ConstraintKinds, NamedFieldPuns #-}

-- | Redis publish/subscribe.
-- See <http://redis.io/commands#pubsub>.

module FP.Redis.PubSub
    ( module FP.Redis.Command.PubSub
    , withSubscriptionsWrapped
    , withSubscriptionsEx
    , subscribe
    , psubscribe
    , unsubscribe
    , punsubscribe
    , makeSubscription
    , sendSubscription
    , sendSubscriptions
    , trackSubscriptionStatus )
    where

--TODO SHOULD: use a connection pool, and re-use a single connection for all subscriptions

import ClassyPrelude.Conduit
import Control.Concurrent.Chan.Lifted (newChan, writeChan, readChan)
import Control.Exception.Lifted (BlockedIndefinitelyOnMVar(..))
import Control.Monad.Logger

import FP.Redis.Connection
import FP.Redis.Internal
import FP.Redis.Types.Internal
import FP.Redis.Command.PubSub

-- | Like 'withSubscriptionsEx', but wraps the callback in an exception handler so it continues
-- to be called for new messages even if it throws an exception.
withSubscriptionsWrapped :: forall m. (MonadConnect m)
                         => ConnectInfo -- ^ Redis connection info
                         -> [SubscriptionRequest] -- ^ List of subscriptions
                         -> (Message -> m ()) -- ^ Callback to receive messages
                         -> m ()
withSubscriptionsWrapped connectionInfo_ subscriptions callback =
    withSubscriptionsEx connectionInfo_ subscriptions wrappedCallback
  where
    wrappedCallback msg = catchAny (callback msg) callbackHandler
    callbackHandler ex =
        logErrorNS (connectLogSource connectionInfo_)
                   ("withSubscrptionsWrapped callbackHandler: " ++ tshow ex)

-- | Open a connection that will listen for pub/sub messages.  Since a connection subscribed
-- to any channels switches to a different mode, regular command and subscriptions cannot
-- be multiplexed in a single connection.
--
-- Note: pub/sub channels exist in a namespace that does not consider the 'select'ed database,
-- so be sure to prefix them if you need to separate namespaces.
--
-- TODO MAYBE: Auto-resubscribe to any subscriptions that were active when auto-reconnecting,
-- instead of just the `initialSubscriptions'.
withSubscriptionsEx :: forall m. (MonadConnect m)
                    => ConnectInfo -- ^ Redis connection info
                    -> [SubscriptionRequest] -- ^ List of subscriptions
                    -> (Message -> m ()) -- ^ Callback to receive messages
                    -> m ()
withSubscriptionsEx _ [] _ = return ()
withSubscriptionsEx connectionInfo_ subscriptions callback = do
    messageChan <- newChan
    withConnection
        (connectionInfo_{connectSubscriptionCallback = Just (writeChan messageChan)
                        ,connectInitialSubscriptions =
                            connectInitialSubscriptions connectionInfo_ ++ subscriptions})
        (\_ -> forever (loop messageChan))
  where
    loop messageChan = do
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
sendSubscription (SubscriptionConnection conn _) (SubscriptionRequest request) =
    sendRequest conn request

-- | Subscribes the client to the specified channels.
--
-- TODO: use MinLen or NonEmpty to ensure at least one subscription?
subscribe :: [ByteString] -- ^ Channels
          -> SubscriptionRequest
subscribe channels = makeSubscription "SUBSCRIBE" (map encodeArg channels)

-- | Subscribes the client to the given patterns.
psubscribe :: [ByteString] -- ^ Patterns
           -> SubscriptionRequest
psubscribe channelPatterns = makeSubscription "PSUBSCRIBE" (map encodeArg channelPatterns)

-- | Unsubscribes the client from the given channels, or from all of them if none is given.
unsubscribe :: [ByteString] -- ^ Channels
            -> SubscriptionRequest
unsubscribe channels = makeSubscription "UNSUBSCRIBE" (map encodeArg channels)

-- | Unsubscribes the client from the given patterns, or from all of them if none is given.
punsubscribe :: [ByteString] -- Patterns
             -> SubscriptionRequest
punsubscribe channelPatterns = makeSubscription "PUNSUBSCRIBE" (map encodeArg channelPatterns)

-- | Make a subscription request
makeSubscription :: ByteString -> [ByteString] -> SubscriptionRequest
makeSubscription cmd args =
    SubscriptionRequest (Subscription (renderRequest (encodeArg cmd:args)))

-- | Utility intended to aid implementing the (Message -> m ())
--   callback.  When subscription is ready, the 'TVar' is set to
--   'True'.  It is set to 'TVar' when it unsubscribes.
trackSubscriptionStatus :: MonadIO m
                        => TVar Bool
                        -> (ByteString -> ByteString -> m ())
                        -> (Message -> m ())
trackSubscriptionStatus subscribed _ (Subscribe {}) =
    liftIO $ atomically $ writeTVar subscribed True
trackSubscriptionStatus subscribed _ (Unsubscribe {}) =
    liftIO $ atomically $ writeTVar subscribed False
trackSubscriptionStatus _ f (Message k x) = f k x
