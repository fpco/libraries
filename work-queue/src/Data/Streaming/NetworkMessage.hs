{-# LANGUAGE DeriveDataTypeable         #-}
{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE NoImplicitPrelude          #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE ConstraintKinds            #-}
{-# LANGUAGE TemplateHaskell            #-}
{-# LANGUAGE ViewPatterns               #-}
-- | Pass well-typed messages across a network connection, including heartbeats.
--
-- Built on top of "Data.Streaming.Network". This module handles all heartbeat
-- checks and ping sending, and uses a handshake protocol to ensure that both
-- sides of the connection intend to send the same type of data.
--
-- Note that if the two sides of your connection are compiled against different
-- versions of libraries, it's entirely possible that 'Typeable' and 'Binary'
-- instances may be incompatible, in which cases guarantees provided by the
-- handshake will not be accurate.
module Data.Streaming.NetworkMessage
    ( -- * Types
      NMApp
    , NMAppData
    , Sendable
    , NetworkMessageException (..)
      -- * Functions
    , runNMApp
    , nmWrite
    , nmRead
    , nmAppData
      -- * Settings
    , NMSettings
    , defaultNMSettings
    , setNMHeartbeat
    , getNMHeartbeat
    ) where

import           ClassyPrelude
import           Control.Concurrent          (threadDelay, myThreadId, throwTo)
import qualified Control.Concurrent.Async    as A
import           Control.Exception           (AsyncException(ThreadKilled))
import           Control.Monad.Base          (liftBase)
import           Control.Monad.Trans.Control (MonadBaseControl, control)
import qualified Data.Binary                 as B
import qualified Data.Binary.Get             as B
import           Data.ConcreteTypeRep        (ConcreteTypeRep, fromTypeRep)
import           Data.Function               (fix)
import           Data.Streaming.Network      (AppData, appRead)
import           Data.Streaming.Network.NoSignal (appWrite')
import           Data.Typeable               (Proxy(..), typeRep)
import           Data.Vector.Binary          () -- commonly needed orphans
import           Data.Void                   (absurd)
import           GHC.IO.Exception            (IOException(ioe_type), IOErrorType(ResourceVanished))
import           System.Executable.Hash      (executableHash)

-- | A network message application.
--
-- This type synonym has four type parameters: the type of messages our side of
-- the connection sends (@iSend@), the type of messages the other side sends
-- (@youSend@), the monad we live in (@m@), and the return value of the
-- application (@a@). Restrictions on these types:
--
-- * @iSend@ and @youSend@ must both be instances of 'Sendable'.  In
-- other words, they must both implement 'Typeable' and 'Binary'.
--
-- * @m@ must be an instance of 'MonadBaseControl' 'IO'.
--
-- * When writing a server, @a@ must be unit, @()@, otherwise no restrictions.
--
-- Like the "Data.Streaming.Network" API, your application takes a value- in
-- this case of type 'NMAppData'- which is used to interact with the other side
-- of the connection. Please see relevant functions exposed by this module to
-- interact with that datatype.
--
-- You can convert an @NMApp@ into an application using 'runNMApp', and then
-- run it using the functions provided by "Data.Streaming.Network".
type NMApp iSend youSend m a = NMAppData iSend youSend -> m a

-- | Constraint synonym for the constraints required to send data from
-- / to an 'NMApp'.
type Sendable a = (B.Binary a, Typeable a)

-- | Provides an 'NMApp' with a means of communicating with the other side of a
-- connection. See other functions provided by this module.
data NMAppData iSend youSend = NMAppData
    { _nmAppData :: AppData
    , _nmWrite   :: iSend -> IO ()
    , _nmRead    :: IO youSend
    } deriving (Typeable)

-- | Get the raw @AppData@. This is useful, for example, to get the @SockAddr@ for
-- this connection. While technically you can use @appRead@ and @appWrite@ as
-- well, it's highly recommended that you don't do so, as you will almost
-- certainly break the communication protocol provided by this module.
nmAppData :: NMAppData iSend youSend -> AppData
nmAppData = _nmAppData

-- | Send a message to the other side of the connection.
nmWrite :: MonadIO m => NMAppData iSend youSend -> iSend -> m ()
nmWrite nm = liftIO . _nmWrite nm

-- | Receive a message from the other side of the connection. Blocks until data
-- is available.
nmRead :: MonadIO m => NMAppData iSend youSend -> m youSend
nmRead = liftIO . _nmRead

data Handshake = Handshake
    { hsISend     :: ConcreteTypeRep
    , hsYouSend   :: ConcreteTypeRep
    , hsHeartbeat :: Int
    , hsExeHash   :: Maybe ByteString
    }
    deriving (Generic, Show, Eq, Typeable)
instance B.Binary Handshake

mkHandshake :: forall iSend youSend m a. (Typeable iSend, Typeable youSend)
            => NMApp iSend youSend m a -> Int -> Maybe ByteString -> Handshake
mkHandshake _ hb eh = Handshake
    { hsISend = fromTypeRep (typeRep (Proxy :: Proxy iSend))
    , hsYouSend = fromTypeRep (typeRep (Proxy :: Proxy youSend))
    , hsHeartbeat = hb
    , hsExeHash = eh
    }

-- | Convert an 'NMApp' into an "Data.Streaming.Network" application.
runNMApp :: forall iSend youSend m a.
            (MonadBaseControl IO m, Sendable iSend, Sendable youSend)
         => NMSettings
         -> NMApp iSend youSend m a
         -> AppData
         -> m a
runNMApp (NMSettings heartbeat exeHash) nmApp ad = do
    (yourHS, leftover) <- liftBase $ do
        let myHS = mkHandshake nmApp heartbeat exeHash
        forM_ (toChunks $ B.encode myHS) (appWrite' ad)
        mgot <- appGet mempty (appRead ad)
        case mgot of
            Just (yourHS, leftover) -> do
                when (hsISend myHS /= hsYouSend yourHS ||
                      hsYouSend myHS /= hsISend yourHS ||
                      hsExeHash myHS /= hsExeHash yourHS) $ do
                    throwIO $ NMMismatchedHandshakes myHS yourHS
                return (yourHS, leftover)
            Nothing -> throwIO NMConnectionDropped
    control $ \runInBase -> do
        -- FIXME use a bounded chan perhaps? (Make the queue size
        -- configuration in NMSettings.) Since any data sent will
        -- count as a ping, this would not interfere with the
        -- heartbeat.
        outgoing <- newChan :: IO (Chan ByteString)
        incoming <- newChan :: IO (Chan (Either NetworkMessageException youSend))

        -- Our heartbeat logic involves two threads: the recvWorker thread
        -- increments lastPing every time it reads a chunk of data, and the
        -- checkHeartbeat thread calls threadDelay and then checks that
        -- lastPing has been incremented over this run. If it hasn't been
        -- incremented, one of the following has occurred:
        --
        -- - The connection has been closed, as indicated by active. If active
        -- if False, then the heartbeat thread knows it should have failed and
        -- simply shuts down.
        --
        -- - Due to how GHC green threads work, it's possible that the receive
        -- thread has been asleep this entire time. Therefore, we check the
        -- blocked IORef. If it's True, it means that the receive thread is
        -- currently blocking on a call to recv, which means that we have not
        -- received any data from the client. In this case, throw a
        -- HeartbeatFailure exception.
        --
        -- - If blocked is False, then we know that the receive thread is
        -- simply asleep, and have no data about whether the heartbeat has
        -- actually failed. In this case, threadDelay and check again.
        lastPing <- newIORef (0 :: Int)
        active <- newIORef True
        blocked <- newIORef False

        let nad = NMAppData
                { _nmAppData = ad
                , _nmWrite = send . Payload
                , _nmRead = do
                    eres <- readChan incoming
                    case eres of
                        Right res -> return res
                        Left err -> do
                            -- Put it back on the channel so that
                            -- subsequent requests get the same
                            -- message.
                            writeChan incoming (Left err)
                            throwIO err
                }
            send :: Message iSend -> IO ()
            send x = writeChan outgoing $! toStrict (B.encode x)
        -- Ping / heartbeat are managed by withAsync, so that they're
        -- interrupted once the other threads have exited.
        A.withAsync (sendPing send yourHS `A.race`
                     checkHeartbeat lastPing active blocked) $ \pingThread -> do
            linkNoThreadKilled pingThread $
                A.runConcurrently $
                    A.Concurrently (recvWorker incoming lastPing active blocked leftover) *>
                    A.Concurrently (sendWorker outgoing incoming active) *>
                    A.Concurrently (runInBase (nmApp nad) `finally`
                                    send Complete `finally`
                                    finished incoming active NMConnectionClosed)
  where
    -- If an exception is thrown in the heartbeat thread, then it
    -- should be rethrown to the main thread.  If it's a ThreadKilled
    -- exception, then it isn't rethrown, because we expect it to get
    -- killed when we exit the 'withAsync'.
    linkNoThreadKilled pingThread inner = do
        tid <- myThreadId
        flip A.withAsync (\_ -> inner) $ do
            eres <- A.waitCatch pingThread
            case eres of
                Right (Left x) -> absurd x
                Right (Right x) -> absurd x
                Left (fromException -> Just ThreadKilled) -> return ()
                Left err -> throwTo tid err

    sendPing send yourHS = forever $ do
        () <- send Ping
        threadDelay $ hsHeartbeat yourHS

    checkHeartbeat lastPing active blocked = forever $ do
        start <- readIORef lastPing
        threadDelay $ heartbeat * 2
        lastPing' <- readIORef lastPing
        when (start == lastPing')
            $ whenM (readIORef active)
            $ whenM (readIORef blocked)
            $ throwIO NMHeartbeatFailure

    -- It is unnecessary to use atomic operations when modifying the
    -- IORefs passed into this thread.  This is because lastPing and
    -- blocked are only read by the 'checkHeartbeat' thread.  That
    -- checking is done periodically at a user specified interval, and
    -- so reordering of concurrent reads / writes is acceptable.
    recvWorker incoming lastPing active blocked = fix $ \loop leftover -> do
        mgot <- appGet leftover (do
            writeIORef blocked True
            bs <- appRead ad
            writeIORef blocked False
            modifyIORef' lastPing (+ 1)
            return bs) `catch` \ex ->
                if isResourceVanished ex
                    then return Nothing
                    else throwIO ex
        case mgot of
            Just (Ping, leftover') ->
                loop leftover'
            Just (Payload p, leftover') ->
                writeChan incoming (Right p) >> loop leftover'
            -- We're done when the "Complete" message is received
            -- or the connection is closed
            Just (Complete, _) -> finished incoming active NMConnectionClosed
            Nothing -> finished incoming active NMConnectionDropped

    isResourceVanished ex = ioe_type ex == ResourceVanished

    finished incoming active ex = do
        atomicWriteIORef active False
        writeChan incoming (Left ex)

    sendWorker outgoing incoming active = fix $ \loop -> do
        bs <- readChan outgoing
        -- NOTE: even though bs could be quite large, appRead will
        -- receive small chunks of it.  This means that 'appRead'
        -- won't block for very long, and the heartbeat code will
        -- function properly.
        if bs == toStrict (B.encode (Complete :: Message iSend))
            then do
                appWrite' ad bs `catch` \ex ->
                    -- Ignore resource vanished if the connection is done.
                    if isResourceVanished ex
                        then return ()
                        else throwIO ex
            else do
                appWrite' ad bs `catch` \ex ->
                    -- ResourceVanished indicates that the connection
                    -- got dropped, so throw that nicer exception
                    -- instead.
                    if isResourceVanished ex
                        then do
                            finished incoming active NMConnectionDropped
                            throwIO NMConnectionDropped
                        else throwIO ex
                loop

-- | Streaming decode function.  If the function to get more bytes
-- yields "", then it's assumed to be the end of the input, and
-- 'Nothing' is returned.
appGet :: B.Binary a
       => ByteString -- ^ leftover bytes from previous parse.
       -> IO ByteString -- ^ function to get more bytes
       -> IO (Maybe (a, ByteString)) -- ^ result and leftovers
appGet bs0 readChunk =
    loop (B.runGetIncremental B.get `B.pushChunk` bs0)
  where
    loop (B.Fail _ _ str) = throwIO (NMDecodeFailure str)
    loop (B.Partial f) = do
        bs <- readChunk
        if null bs
            then return Nothing
            else loop (f (Just bs))
    loop (B.Done bs _ res) = return (Just (res, bs))

data Message payload
    = Ping
    | Payload payload
    | Complete
    deriving (Generic, Typeable)
instance B.Binary payload => B.Binary (Message payload)

data NetworkMessageException
    -- | This is thrown by 'runNMApp', during the initial handshake,
    -- when the two sides of the connection disagree about the
    -- datatypes being sent, or when there is a mismatch in executable
    -- hash.
    = NMMismatchedHandshakes Handshake Handshake
    -- | This is thrown by 'runNMApp' if we haven't received a data
    -- packet or ping from the other side of the connection within
    -- @heartbeat * 2@ time.
    | NMHeartbeatFailure
    -- | This is thrown by 'nmRead' when the connection is closed.
    | NMConnectionClosed
    -- | This is thrown by 'nmRead' and 'runNMApp' when the connection
    -- gets unexpectedly terminated.
    | NMConnectionDropped
    -- | This is thrown by 'runNMApp' when there's an error decoding
    -- data sent by the other side of the connection.  This either
    -- indicates a bug in this library, or a misuse of 'nmAppData'.
    | NMDecodeFailure String
    deriving (Show, Typeable, Eq, Generic)
instance Exception NetworkMessageException
instance B.Binary NetworkMessageException

-- | Settings to be used by 'runNMApp'. Use 'defaultNMSettings' and modify with
-- setter functions.
data NMSettings = NMSettings
    { _nmHeartbeat :: !Int
    , _nmExeHash :: !(Maybe ByteString)
    } deriving (Typeable)

-- | Default settings value.
--
-- This is in the IO monad because it reads / computes the
-- executable's hash.
--
-- Heartbeat set at 200ms. This value is quite low, and intended for
-- low-latency LAN connections. You may need to set this higher, depending on
-- your needs.
defaultNMSettings :: IO NMSettings
defaultNMSettings = do
    exeHash <- $(executableHash)
    return NMSettings
        { _nmHeartbeat = 200000
        , _nmExeHash = exeHash
        }

-- | Set the heartbeat timeout to the given number of microseconds.
--
-- This determines the `threadDelay` between sending a 'Ping' to the
-- other side of the connection.
--
-- The heartbeat is sent as part of the handshake, so that the other
-- side knows how often it should a ping.  The other 'NMApp' will wait
-- @heartbeat * 2@ microseconds of not receiving any packets before
-- assuming that the other server is dead and throwing
-- 'HeartbeatFailure'.
setNMHeartbeat :: Int -> NMSettings -> NMSettings
setNMHeartbeat x y = y { _nmHeartbeat = x }

-- | Gets the heartbeat time, in microseconds.
getNMHeartbeat :: NMSettings -> Int
getNMHeartbeat = _nmHeartbeat
