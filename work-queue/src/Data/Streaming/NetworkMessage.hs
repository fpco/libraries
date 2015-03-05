{-# LANGUAGE DeriveDataTypeable         #-}
{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE NoImplicitPrelude          #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE ConstraintKinds  #-}
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
    ) where

import           ClassyPrelude
import           Control.Concurrent          (threadDelay)
import qualified Control.Concurrent.Async    as A
import           Control.Monad.Base          (liftBase)
import           Control.Monad.Trans.Control (MonadBaseControl, control)
import qualified Data.Binary                 as B
import qualified Data.Binary.Get             as B
import           Data.ConcreteTypeRep        (ConcreteTypeRep, cTypeOf)
import           Data.Function               (fix)
import           Data.Streaming.Network      (AppData, appRead, appWrite)
import           Data.Vector.Binary          () -- commonly needed orphans
import           System.Executable.Hash      (executableHash)

-- | A network message application.
--
-- This type synonym has four type parameters: the type of messages our side of
-- the connection sends (@iSend@), the type of messages the other side sends
-- (@youSend@), the monad we live in (@m@), and the return value of the
-- application (@a@). Restrictions on these types:
--
-- * @iSend@ and @youSend@ must both be instances of 'Typeable' and 'Binary'.
--
-- * @m@ must be an instance of 'MonadBaseControl' 'IO'.
--
-- * When writing a server, @a@ must be unit (@()@), otherwise no restrictions.
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
    }

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
    { hsISend = cTypeOf (error "impossible: iSend shouldn't be evaluated" :: iSend)
    , hsYouSend = cTypeOf (error "impossible: youSend shouldn't be evaluated" :: youSend)
    , hsHeartbeat = hb
    , hsExeHash = eh
    }

-- | Convert an 'NMApp' into an "Data.Streaming.Network" application.
runNMApp :: (MonadBaseControl IO m, Sendable iSend, Sendable youSend)
         => NMSettings
         -> NMApp iSend youSend m a
         -> AppData
         -> m a
runNMApp (NMSettings heartbeat exeHash) app ad = do
    (yourHS, leftover) <- liftBase $ do
        let myHS = mkHandshake app heartbeat exeHash
        forM_ (toChunks $ B.encode myHS) (appWrite ad)
        mgot <- appGet mempty (appRead ad)
        case mgot of
            Just (yourHS, leftover) -> do
                when (hsISend myHS /= hsYouSend yourHS ||
                      hsYouSend myHS /= hsISend yourHS ||
                      hsExeHash myHS /= hsExeHash yourHS) $ do
                    throwIO $ MismatchedHandshakes myHS yourHS
                return (yourHS, leftover)
            Nothing -> throwIO ConnectionClosedBeforeHandshake
    control $ \runInBase -> do
        -- FIXME use a bounded chan perhaps? will mess up heartbeat...
        -- actually, with the blocked variable below, we should be able to
        -- avoid heartbeat issues on the receiving side. And since any data
        -- sent will count as a ping, it should be safe on the outgoing side
        -- too. So should be safe to add this. (Make the queue size
        -- configuration in NMSettings.)
        outgoing <- newChan
        incoming <- newChan

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
        -- blocked thread. If it's True, it means that the receive thread is
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
                , _nmWrite = writeChan outgoing . Payload
                , _nmRead = join $ readChan incoming
                }
        A.runConcurrently $
            A.Concurrently (sendPing outgoing yourHS active) *>
            A.Concurrently (checkHeartbeat lastPing active blocked) *>
            A.Concurrently (recvWorker leftover incoming lastPing active blocked) *>
            A.Concurrently (sendWorker outgoing) *>
            A.Concurrently (runInBase (app nad) `finally`
                            writeChan outgoing Complete `finally`
                            writeIORef active False)
  where
    while ref inner = do
        loop
      where
        loop = whenM (readIORef ref) (inner >> loop)

    sendPing outgoing yourHS active = while active $ do
        writeChan outgoing Ping
        threadDelay $ hsHeartbeat yourHS

    checkHeartbeat lastPing active blocked = while active $ do
        start <- readIORef lastPing
        threadDelay $ heartbeat * 2
        lastPing' <- readIORef lastPing
        when (start == lastPing')
            $ whenM (readIORef active)
            $ whenM (readIORef blocked)
            $ throwIO HeartbeatFailure

    recvWorker leftover0 incoming lastPing active blocked =
        loop leftover0
      where
        loop leftover = do
            mgot <- appGet leftover $ do
               writeIORef blocked True
               bs <- appRead ad
               writeIORef blocked False
               modifyIORef lastPing (+ 1)
               return bs
            case mgot of
                Just (Ping, leftover') ->
                    loop leftover'
                Just (Payload p, leftover') ->
                    writeChan incoming (return p) >> loop leftover'
                -- We're done when the "Complete" message is received
                -- or the connection is closed
                Just (Complete, _) -> done
                Nothing -> done
        done = do
            writeChan incoming (throwIO NMConnectionClosed)
            writeIORef active False

    sendWorker outgoing = fix $ \loop -> do
        x <- readChan outgoing
        -- FIXME Manny's going to write an equivalent to toByteStringIO, use that instead
        forM_ (toChunks $ B.encode x) (appWrite ad)
        case x of
            Complete -> return ()
            _ -> loop

-- | Streaming decode function.
appGet :: B.Binary a
       => ByteString -- ^ leftover bytes from previous parse.
       -> IO ByteString -- ^ function to get more bytes
       -> IO (Maybe (a, ByteString)) -- ^ result and leftovers
appGet bs0 readChunk
    | null bs0 = loop initial
    | otherwise =
        case initial of
            B.Partial f -> loop $ f $ Just bs0
            -- neither of the following two cases should ever occur
            B.Fail _ _ str -> throwIO (DecodeFailure str)
            B.Done bs _ res -> return (Just (res, bs0 ++ bs))
  where
    initial = B.runGetIncremental B.get

    loop (B.Fail _ _ str) = throwIO (DecodeFailure str)
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
    deriving Generic
instance B.Binary payload => B.Binary (Message payload)

data NetworkMessageException
    = MismatchedHandshakes Handshake Handshake
    | ConnectionClosedBeforeHandshake
    | HeartbeatFailure
    | NMConnectionClosed
    | DecodeFailure String
    deriving (Show, Typeable, Eq)
instance Exception NetworkMessageException

-- | Settings to be used by 'runNMApp'. Use 'defaultNMSettings' and modify with
-- setter functions.
data NMSettings = NMSettings
    { _nmHeartbeat :: !Int
    , _nmExeHash :: !(Maybe ByteString)
    }

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
    exeHash <- executableHash
    return NMSettings
        { _nmHeartbeat = 200000
        , _nmExeHash = exeHash
        }

-- | Set the heartbeat timeout to the given number of microseconds (to be used
-- by 'threadDelay').
setNMHeartbeat :: Int -> NMSettings -> NMSettings
setNMHeartbeat x y = y { _nmHeartbeat = x }
