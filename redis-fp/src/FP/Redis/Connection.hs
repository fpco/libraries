{-# LANGUAGE NoImplicitPrelude, OverloadedStrings, ScopedTypeVariables, TypeFamilies,
             DeriveDataTypeable, FlexibleContexts, FlexibleInstances, RankNTypes, GADTs,
             ConstraintKinds, NamedFieldPuns, TupleSections, LambdaCase, TemplateHaskell #-}

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

import ClassyPrelude.Conduit hiding (Builder, connect, leftover, Handler)
import qualified Network.Socket as NS
import Control.Lens ((^.))
import qualified Data.Streaming.Network as CN
import qualified Data.Conduit.Network as CN
import Control.Retry (recovering)
import Control.Monad.Logger
import Control.Monad.Catch (Handler(..))

import FP.Redis.Command
import FP.Redis.Internal
import FP.Redis.Types.Internal

connect :: (MonadConnect m) => ConnectInfo -> m Connection
connect cinfo = do
    let cs = CN.clientSettings (connectPort cinfo) (connectHost cinfo)
    let retry m = case connectRetryPolicy cinfo of
            Nothing -> liftIO m
            Just policy -> do
                runBase <- askRunBase
                liftIO $ recovering
                   policy
                   [\_ -> Handler $ \(err :: IOError) -> runBase $ do
                       $logWarn ("Got IOError " ++ tshow err ++ " when trying to connect to redis, will retry")
                       return True]
                   (\_ -> m)
    (s, _address) <-
        retry (CN.getSocketFamilyTCP (CN.getHost cs) (cs ^. CN.portLens) (CN.getAddrFamily cs))
    leftoverRef <- newIORef ""
    return Connection
        { connectionInfo_ = cinfo
        , connectionSocket = s
        , connectionLeftover = leftoverRef
        }

disconnect :: (MonadCommand m) => Connection -> m ()
disconnect conn = do
    runBase <- askRunBase
    liftIO $ do
        -- Ignore IO errors when disonnecting
        catch
            (runBase (sendCommand conn quit))
            (\(_ :: IOError) -> return ())
        NS.sClose (connectionSocket conn)

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
                               , connectLogSource = "REDIS"
                               , connectRetryPolicy = Nothing
                               }

-- | Get original connect info from connection.
connectionInfo :: Connection -> ConnectInfo
connectionInfo = connectionInfo_
