{-# LANGUAGE NoImplicitPrelude, OverloadedStrings, ScopedTypeVariables, TypeFamilies,
             DeriveDataTypeable, FlexibleContexts, FlexibleInstances, RankNTypes, GADTs,
             ConstraintKinds, NamedFieldPuns, TupleSections, LambdaCase #-}

-- | Redis connection handling.

module FP.Redis.Connection
    ( ConnectInfo(..)
    , Connection
    , connectInfo
    , connectionInfo
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
import qualified Data.Streaming.Network as CN
import qualified Data.DList as DList
import Control.Lens ((^.))

import FP.Redis.Command
import FP.Redis.Internal
import FP.Redis.Types.Internal
import Control.Concurrent.STM.TSQueue
import FP.ThreadFileLogger

-- | Connects to Redis server and runs the inner action.  When the inner action returns,
-- the connection is terminated.
withConnection :: forall a m. (MonadConnect m)
               => ConnectInfo -- ^ Connection information
               -> (Connection -> m a) -- ^ Inner action
               -> m a
withConnection cinfo inner =
    control $ \run ->
        CN.runTCPClient
            (CN.clientSettings (connectPort cinfo) (connectHost cinfo))
            (\appData -> run $ do
                leftoverRef <- newIORef ""
                let conn = Connection
                        { connectionInfo_ = cinfo
                        , connectionAppData = appData
                        , connectionLeftover = leftoverRef
                        }
                finally (inner conn) (sendCommand conn quit))

-- | Default Redis server connection info.
connectInfo :: ByteString -- ^ Server's hostname
            -> ConnectInfo
connectInfo host = ConnectInfo { connectHost = host
                               , connectPort = 6379
                               , connectLogSource = "REDIS" }

-- | Get original connect info from connection.
connectionInfo :: Connection -> ConnectInfo
connectionInfo = connectionInfo_
