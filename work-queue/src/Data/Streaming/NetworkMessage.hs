{-# LANGUAGE DeriveDataTypeable         #-}
{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase                 #-}
{-# LANGUAGE NoImplicitPrelude          #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE ConstraintKinds            #-}
{-# LANGUAGE TemplateHaskell            #-}
{-# LANGUAGE ViewPatterns               #-}
{-# LANGUAGE RankNTypes                 #-}
{-# LANGUAGE RecordWildCards #-}
{-|
Module: Data.Streaming.NetworkMessage
Description: Pass well-typed messages across a network connection.

Built on top of "Data.Streaming.Network". This module uses a handshake
protocol to ensure that both sides of the connection intend to send
the same type of data.

Valid message types must be instances of 'Store' for serialisation,
and 'HasTypeHash' for the handshake.

In addition to the types of the messages, the handshake also includes
a hash of the executable.  Thus,

* Both parties of the connection have to be implemented in the same
  executable

* It is guaranteed that there is no mismatch in the serialisation
  formats due to different library versions.
-}
module Data.Streaming.NetworkMessage
    ( -- * Types
      NMApp
    , NMAppData
    , nmAppData
    , Sendable
    , NetworkMessageException(..)
      -- * Running an 'NMApp'
    , runNMApp
      -- * Functions for communication
    , nmWrite
    , nmRead
    , nmReadByteString
    , nmWaitReadSTM
    , nmWaitReadByteString
      -- * Settings
    , NMSettings
    , defaultNMSettings
      -- * Utils
    , getPortAfterBind
    ) where

import           ClassyPrelude
import qualified Control.Concurrent.STM as STM
import           Data.Store (Store, PeekException (..))
import qualified Data.Store.Streaming as S
import           Control.Exception (BlockedIndefinitelyOnMVar(..))
import           System.IO.ByteBuffer (ByteBuffer)
import qualified System.IO.ByteBuffer as BB
import           Data.Streaming.Network (AppData, appRead, appWrite)
import           Data.Streaming.Network (ServerSettings, setAfterBind, appRawSocket)
import           Data.Store.TypeHash
import           Data.Typeable (Proxy(..))
import           Network.Socket (socketPort, fdSocket)
import           System.Executable.Hash (executableHash)
import           System.Posix.Types (Fd (..))
import           FP.Redis (MonadConnect)
import           GHC.Conc (threadWaitReadSTM)
import           Control.Concurrent.Lifted (fork)

-- | Exceptions specific to "Data.Streaming.NetworkMessage".
data NetworkMessageException
    -- | This is thrown by 'runNMApp', when the initial handshake
    -- fails.  This either means that the two sides of the connection
    -- disagree about the datatypes being sent, or that they are not
    -- part of the same executable.
    = NMMismatchedHandshakes Handshake Handshake
    -- | This is thrown by 'runNMApp' when there's an error decoding
    -- data sent by the other side of the connection.  This either
    -- indicates a bug in this library, or a misuse of 'nmAppData'.
    | NMDecodeFailure String
    deriving (Show, Typeable, Eq, Generic)
instance Exception NetworkMessageException
instance Store NetworkMessageException

-- | A network message application.
--
-- This type synonym has four type parameters: the type of messages our side of
-- the connection sends (@iSend@), the type of messages the other side sends
-- (@youSend@), the monad we live in (@m@), and the return value of the
-- application (@a@). Restrictions on these types:
--
-- * @iSend@ and @youSend@ must both be instances of 'Sendable'.  In
-- other words, they must both implement 'HasTypeHash' and 'Store'.
--
-- * @m@ must be an instance of 'MonadBaseControl' 'IO'.
--
-- * When writing a server, @a@ must be unit, @()@, otherwise no restrictions.
--
-- Like the "Data.Streaming.Network" API, your application takes a value -in
-- this case of type 'NMAppData'- which is used to interact with the other side
-- of the connection. Please see relevant functions exposed by this module to
-- interact with that datatype.
--
-- You can convert an @NMApp@ into an application using 'runNMApp', and then
-- run it using the functions provided by "Data.Streaming.Network".
type NMApp iSend youSend m a = NMAppData iSend youSend -> m a

-- | Constraint synonym for the constraints required to send data from
-- / to an 'NMApp'.
type Sendable a = (Store a, HasTypeHash a)

-- | Provides an 'NMApp' with a means of communicating with the other side of a
-- connection. See other functions provided by this module.
data NMAppData iSend youSend = NMAppData
    { nmAppData :: !AppData
      -- ^ 'Data.Streaming.Network.AppData' used to communicate with
      -- the other side of the connection
    , nmByteBuffer :: !ByteBuffer
      -- ^ This 'ByteBuffer' is used to buffer incoming data, until a
      -- complete message can be deserialized.
    } deriving (Typeable)

-- | Send a message to the other side of the connection.
nmWrite :: (MonadIO m, Store iSend) => NMAppData iSend youSend -> iSend -> m ()
nmWrite nm iSend = liftIO (appWrite (nmAppData nm) (encode iSend))

-- | Read a message from the other side of the connection.
nmRead  :: (MonadConnect m, Store youSend) => NMAppData iSend youSend -> m youSend
nmRead NMAppData{..} = liftIO (appGet "nmRead" nmByteBuffer nmAppData)

-- | Wait for incoming data from the other side.
--
-- The second return value can be used to cancel waiting for a
-- response.
nmWaitReadSTM :: (MonadConnect m, Store youSend) => NMAppData iSend youSend -> m (STM youSend, m ())
nmWaitReadSTM appData = do
    m <- liftIO $ newTVarIO Nothing
    _ <- fork $ do
        msg <- nmRead appData
        atomically $ writeTVar m (Just msg)
    let waitAction = readTVar m >>= \case
            Just msg -> return msg
            Nothing -> STM.retry
        killAction = return ()
    return (waitAction, killAction)

nmWaitReadByteString :: (MonadConnect m) => NMAppData iSend youSend -> m (STM ByteString, m ())
nmWaitReadByteString nm = do
    m <- liftIO $ newTVarIO Nothing
    _ <- fork $ do
        bs <- liftIO $ readByteString nm
        atomically $ writeTVar m (Just bs)
    let waitAction = readTVar m >>= \case
            Just bs -> return bs
            Nothing -> STM.retry
        killAction = return ()
    return (waitAction, killAction)

nmReadByteString :: (MonadConnect m) => NMAppData iSend youSend -> m ByteString
nmReadByteString nm = liftIO (readByteString nm)

readByteString :: NMAppData iSend youSend -> IO ByteString
readByteString NMAppData{..} =
    (S.peekMessageHeader nmByteBuffer >>= loop >>= getBS)
    `catch` (\ ex@(PeekException _ _) -> throwIO . NMDecodeFailure . ("nmWaitReadByteString " ++) . show $ ex)
  where
    loop (S.NeedMoreInput cont) = do
        bs <- appRead nmAppData
        if null bs
            then throwIO (NMDecodeFailure "nmWaitReadByteString Couldn't decode: no data")
            else cont bs >>= loop
    loop (S.Done (S.Message x)) = return x
    getBS (n :: S.SizeTag) =
        BB.consume nmByteBuffer n >>= \case
            Right bs -> return bs
            Left _ -> do
                chunk <- appRead nmAppData
                BB.copyByteString nmByteBuffer chunk
                getBS n

-- | Streaming decode function.  If the function to get more bytes
-- yields "", then it's assumed to be the end of the input, and
-- 'Nothing' is returned.
appGet :: (Store a)
       => String
       -> ByteBuffer  -- ^ 'ByteBuffer' for streaming
       -> AppData
       -> IO a  -- ^ result
appGet loc bb ad =
    (S.peekMessage bb >>= loop)
    `catch` (\ ex@(PeekException _ _) -> throwIO . NMDecodeFailure . ((loc ++ " ") ++) . show $ ex)
  where
    loop (S.NeedMoreInput cont) = do
        bs <- appRead ad
        if null bs
            then throwIO (NMDecodeFailure (loc ++ " Couldn't decode: no data"))
            else cont bs >>= loop
    loop (S.Done (S.Message x)) = return x

-- | Data compared between both parties during the initial handshake.
data Handshake = Handshake
    { hsISend     :: TypeHash
    , hsYouSend   :: TypeHash
    , hsExeHash   :: Maybe ByteString
    }
    deriving (Generic, Show, Eq, Typeable)
instance Store Handshake

-- | Construct data for the handshake from the 'TypeHash'es of the
-- message types, and the executable hash.
mkHandshake
    :: forall iSend youSend m a. (HasTypeHash iSend, HasTypeHash youSend)
    => NMApp iSend youSend m a -> Maybe ByteString -> Handshake
mkHandshake _ eh = Handshake
    { hsISend = typeHash (Proxy :: Proxy iSend)
    , hsYouSend = typeHash (Proxy :: Proxy youSend)
    , hsExeHash = eh
    }

-- | Convert an 'NMApp' into a "Data.Streaming.Network" application.
runNMApp :: forall iSend youSend m a.
       (MonadConnect m, Sendable iSend, Sendable youSend)
    => NMSettings
    -> NMApp iSend youSend m a
    -> AppData
    -> m a
runNMApp (NMSettings exeHash) nmApp ad = BB.with Nothing $ \buffer -> do
    -- Handshake
    let myHS = mkHandshake nmApp exeHash
    liftIO (appWrite ad (encode myHS))
    yourHS <- liftIO (appGet "handshake" buffer ad)
    when (hsISend myHS /= hsYouSend yourHS ||
          hsYouSend myHS /= hsISend yourHS ||
          hsExeHash myHS /= hsExeHash yourHS) $ do
        throwIO (NMMismatchedHandshakes myHS yourHS)
    nmApp NMAppData{nmAppData = ad, nmByteBuffer = buffer}

-- | Settings to be used by 'runNMApp'. Use 'defaultNMSettings' and modify with
-- setter functions.
data NMSettings = NMSettings
    { _nmExeHash :: !(Maybe ByteString)
    } deriving (Typeable)

-- | Default settings value.
--
-- This is in the IO monad because it reads / computes the
-- executable's hash.
defaultNMSettings :: (MonadIO m) => m NMSettings
defaultNMSettings = liftIO $ do
    exeHash <- $(executableHash)
    return NMSettings
        { _nmExeHash = exeHash
        }

-- | Creates a new 'ServerSettings', which will populate an 'MVar' with
-- the bound port. An action is returned which will unblock once there's
-- a result.
getPortAfterBind :: ServerSettings -> IO (ServerSettings, IO Int)
getPortAfterBind ss = do
    boundPortVar <- newEmptyMVar
    let ss' = setAfterBind
            (putMVar boundPortVar . fromIntegral <=< socketPort)
            ss
    return
        ( ss'
        , readMVar boundPortVar `catch` \BlockedIndefinitelyOnMVar ->
            error "Port will never get bound, thread got blocked indefinitely."
        )

-- | Utility for encoding a value after wrapping it in a 'S.Message'.
encode :: Store a => a -> ByteString
encode = S.encodeMessage . S.Message
