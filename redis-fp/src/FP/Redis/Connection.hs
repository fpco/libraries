{-# LANGUAGE NoImplicitPrelude, OverloadedStrings, ScopedTypeVariables, TypeFamilies,
             DeriveDataTypeable, FlexibleContexts, FlexibleInstances, RankNTypes, GADTs,
             ConstraintKinds, NamedFieldPuns, TupleSections, LambdaCase #-}

-- | Redis connection handling.

module FP.Redis.Connection
    ( ConnectInfo(..)
    , Connection
    , connectInfo
    , connectionInfo
    , connect
    , disconnect
    , disconnectNoQuit
    , withConnection
    , writeRequest
    , readResponse
    , sendCommand
    ) where

-- TODO OPTIONAL: Add a HasConnection class so clients don't need to pass Connections explicitly

import ClassyPrelude.Conduit hiding (Builder, connect, leftover)
import Blaze.ByteString.Builder (Builder)
import qualified Blaze.ByteString.Builder as Builder
import qualified Control.Concurrent.Async.Lifted.Safe as Async
import Control.Concurrent.STM (retry)
import Control.Exception (AsyncException(ThreadKilled))
import Control.Monad.Catch (Handler(..))
import Control.Monad.Extra
import Control.Monad.Logger
import Control.Monad.Trans.Unlift (UnliftBase(..), askUnliftBase)
import Control.Monad.Trans.Control (control)
import Data.Conduit.Blaze (unsafeBuilderToByteString, allocBuffer)
import qualified Data.Conduit.Network as CN
import qualified Data.DList as DList
import qualified Network.Socket as NS
import Control.Lens ((^.))
import qualified Data.Streaming.Network as CN
import qualified Data.Streaming.Network as CN

import FP.Redis.Command
import FP.Redis.Internal
import FP.Redis.Types.Internal
import Control.Concurrent.STM.TSQueue
import FP.ThreadFileLogger

connect :: (MonadCommand m) => ConnectInfo -> m Connection
connect cinfo = do
    let cs = CN.clientSettings (connectPort cinfo) (connectHost cinfo)
    (s, _address) <- liftIO (CN.getSocketFamilyTCP (CN.getHost cs) (cs ^. CN.portLens) (CN.getAddrFamily cs))
    leftoverRef <- newIORef ""
    return Connection
        { connectionInfo_ = cinfo
        , connectionSocket = s
        , connectionLeftover = leftoverRef
        }

disconnect :: (MonadCommand m) => Connection -> m ()
disconnect conn = do
    -- Ignore IO errors when disonnecting
    catch
        (sendCommand conn quit)
        (\(_ :: IOError) -> return ())
    liftIO (NS.sClose (connectionSocket conn))

disconnectNoQuit :: (MonadCommand m) => Connection -> m ()
disconnectNoQuit conn = liftIO (NS.sClose (connectionSocket conn))

-- | Connects to Redis server and runs the inner action.  When the inner action returns,
-- the connection is terminated.
withConnection :: forall a m. (MonadConnect m)
               => ConnectInfo -- ^ Connection information
               -> (Connection -> m a) -- ^ Inner action
               -> m a
withConnection cinfo = bracket (connect cinfo) disconnect

-- | Default Redis server connection info.
connectInfo :: ByteString -- ^ Server's hostname
            -> ConnectInfo
connectInfo host = ConnectInfo { connectHost = host
                               , connectPort = 6379
                               , connectLogSource = "REDIS" }

-- | Get original connect info from connection.
connectionInfo :: Connection -> ConnectInfo
connectionInfo = connectionInfo_
