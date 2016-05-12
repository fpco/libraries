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
{-# LANGUAGE RankNTypes                 #-}
{-# LANGUAGE RecordWildCards #-}
-- | Pass well-typed messages across a network connection, including heartbeats.
--
-- Built on top of "Data.Streaming.Network". This module handles all heartbeat
-- checks and ping sending, and uses a handshake protocol to ensure that both
-- sides of the connection intend to send the same type of data.
--
-- Note that if the two sides of your connection are compiled against different
-- versions of libraries, it's entirely possible that 'Typeable' and 'Serialize'
-- instances may be incompatible, in which cases guarantees provided by the
-- handshake will not be accurate.
module Data.Streaming.NetworkMessage
    ( -- * Types
      NMApp
    , NMAppData
    , nmAppData
    , Sendable
    , NetworkMessageException(..)
      -- * Functions
    , runNMApp
    , nmWrite
    , nmRead
      -- * Settings
    , NMSettings
    , defaultNMSettings
      -- * Utils
    , getPortAfterBind
    ) where

import           ClassyPrelude
import qualified Data.Serialize as B
import           Control.Exception (BlockedIndefinitelyOnMVar(..))
import           Data.Streaming.Network (AppData, appRead, appWrite)
import           Data.Streaming.Network (ServerSettings, setAfterBind)
import           Data.TypeFingerprint
import           Data.Typeable (Proxy(..))
import           Network.Socket (socketPort)
import           System.Executable.Hash (executableHash)
import           FP.Redis (MonadConnect)
import           Control.Monad.Logger (logDebug)
import qualified Data.Text as T

data NetworkMessageException
    -- | This is thrown by 'runNMApp', during the initial handshake,
    -- when the two sides of the connection disagree about the
    -- datatypes being sent, or when there is a mismatch in executable
    -- hash.
    = NMMismatchedHandshakes Handshake Handshake
    -- | This is thrown by 'runNMApp' when there's an error decoding
    -- data sent by the other side of the connection.  This either
    -- indicates a bug in this library, or a misuse of 'nmAppData'.
    | NMDecodeFailure String
    deriving (Show, Typeable, Eq, Generic)
instance Exception NetworkMessageException
instance B.Serialize NetworkMessageException

-- | A network message application.
--
-- This type synonym has four type parameters: the type of messages our side of
-- the connection sends (@iSend@), the type of messages the other side sends
-- (@youSend@), the monad we live in (@m@), and the return value of the
-- application (@a@). Restrictions on these types:
--
-- * @iSend@ and @youSend@ must both be instances of 'Sendable'.  In
-- other words, they must both implement 'Typeable' and 'Serialize'.
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
type Sendable a = (B.Serialize a, HasTypeFingerprint a)

-- | Provides an 'NMApp' with a means of communicating with the other side of a
-- connection. See other functions provided by this module.
data NMAppData iSend youSend = NMAppData
    { nmAppData :: !AppData
    , nmLeftover :: !(IORef ByteString)
    } deriving (Typeable)

-- | Send a message to the other side of the connection.
nmWrite :: (MonadIO m, B.Serialize iSend) => NMAppData iSend youSend -> iSend -> m ()
nmWrite nm iSend = liftIO (appWrite (nmAppData nm) (B.encode iSend))

nmRead  :: (MonadConnect m, B.Serialize youSend) => NMAppData iSend youSend -> m youSend
nmRead NMAppData{..} = do
    (res, leftover) <- appGet "nmRead" nmAppData =<< readIORef nmLeftover
    writeIORef nmLeftover leftover
    return res

-- | Receive a message from the other side of the connection. Blocks until data
-- is available.
appGet :: (MonadConnect m, B.Serialize youSend) => String -> AppData -> ByteString -> m (youSend, ByteString)
appGet loc ad leftover0 = go (B.runGetPartial B.get leftover0)
  where
    go (B.Fail str _) =
        liftIO (throwIO (NMDecodeFailure (loc ++ " Couldn't decode: " ++ str)))
    go (B.Partial f) = do
        $logDebug (T.pack loc ++ " appGet reading")
        bs <- liftIO (appRead ad)
        if null bs
            then liftIO (throwIO (NMDecodeFailure (loc ++ " Couldn't decode: no data")))
            else go (f bs)
    go (B.Done res bs) = do
        return (res, bs)

data Handshake = Handshake
    { hsISend :: TypeFingerprint
    , hsYouSend :: TypeFingerprint
    , hsExeHash :: Maybe ByteString
    } deriving (Generic, Show, Eq, Typeable)
instance B.Serialize Handshake

mkHandshake
    :: forall iSend youSend m a. (HasTypeFingerprint iSend, HasTypeFingerprint youSend)
    => NMApp iSend youSend m a -> Maybe ByteString -> Handshake
mkHandshake _ eh = Handshake
    { hsISend = typeFingerprint (Proxy :: Proxy iSend)
    , hsYouSend = typeFingerprint (Proxy :: Proxy youSend)
    , hsExeHash = eh
    }

-- | Convert an 'NMApp' into a "Data.Streaming.Network" application.
runNMApp :: forall iSend youSend m a.
       (MonadConnect m, Sendable iSend, Sendable youSend)
    => NMSettings
    -> NMApp iSend youSend m a
    -> AppData
    -> m a
runNMApp (NMSettings exeHash) nmApp ad = do
    -- Handshake
    let myHS = mkHandshake nmApp exeHash
    forM_ (toChunks $ B.encodeLazy myHS) (liftIO . appWrite ad)
    (yourHS, leftover) <- appGet "handshake" ad ""
    when (hsISend myHS /= hsYouSend yourHS ||
          hsYouSend myHS /= hsISend yourHS ||
          hsExeHash myHS /= hsExeHash yourHS) $ do
        throwIO (NMMismatchedHandshakes myHS yourHS)

    -- Continue
    leftoverVar <- newIORef leftover
    nmApp NMAppData{nmAppData = ad, nmLeftover = leftoverVar}

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
