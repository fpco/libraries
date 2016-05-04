{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TupleSections #-}
module FP.Redis.Managed
    ( ManagedConnection
    , newManagedConnection
    , destroyManagedConnection
    , withManagedConnection
    , useConnection
    , managedConnectInfo
    ) where

import ClassyPrelude
import Control.Monad.Logger
import qualified Control.Concurrent.STM as STM
import qualified Data.Text.Encoding as T

import FP.Redis.Connection
import FP.Redis.Types.Internal

-- REVIEW TODO add timestamp recording last time the connection was touched and "reaper"
-- to disconnect stale ones

data Connections = Connections
    { connectionsActive :: !Int
    , connectionsAvailable :: ![Connection]
    }

data ManagedConnection = ManagedConnection
    { mcConnections :: !(TVar Connections)
    , mcMaxConnections :: !Int
    , mcConnectInfo :: !ConnectInfo
    }

-- | We connect at the beginning (and not lazily) so that an eventual connection
-- error is immediately returned
newManagedConnection ::
       (MonadConnect m)
    => ConnectInfo -> Int -> m ManagedConnection
newManagedConnection cinfo maxConns = do
    when (maxConns < 1) $
        liftIO (fail ("newManagedConnection: maxConns must be >= 1, but got " ++ show maxConns))
    $logDebug ("ManagedConnection creating initial connection to " ++ showConnectInfo)
    conn <- connect cinfo
    conns <- liftIO (newTVarIO (Connections 1 [conn]))
    return ManagedConnection{mcConnections = conns, mcConnectInfo = cinfo, mcMaxConnections = maxConns}
  where
    showConnectInfo = T.decodeUtf8 (connectHost cinfo) ++ ":" ++ tshow (connectPort cinfo)

destroyManagedConnection :: (MonadCommand m) => ManagedConnection -> m ()
destroyManagedConnection conn = do
    mapM_ disconnect . connectionsAvailable =<< atomically (readTVar (mcConnections conn))

withManagedConnection :: (MonadConnect m) => ConnectInfo -> Int -> (ManagedConnection -> m a) -> m a
withManagedConnection cinfo maxConns = bracket (newManagedConnection cinfo maxConns) destroyManagedConnection

managedConnectInfo :: ManagedConnection -> ConnectInfo
managedConnectInfo = mcConnectInfo

useConnection :: forall m a.
       (MonadConnect m)
    => ManagedConnection -> (Connection -> m a) -> m a
useConnection mc cont =
    bracket tryTakeConnection putConnection $ \(_wasThere, mbConnRef) -> do
        let fillConnRef = do
                conn <- connect (mcConnectInfo mc)
                writeIORef mbConnRef (Just conn)
                return conn
        (conn, fresh) <- do
            mbConn <- readIORef mbConnRef
            case mbConn of
                Just conn -> return (conn, False)
                Nothing -> do
                    $logDebug ("ManagedConnection creating new connection to " ++ showConnectInfo)
                    conn <- fillConnRef
                    return (conn, True)
        mbErr :: Either IOError a <- try (cont conn)
        -- Retry once if the connection is not fresh.
        case (fresh, mbErr) of
            (False, Left err) -> do
                $logWarn ("Encountered IOError " ++ tshow err ++ " when trying to use a connection to " ++ showConnectInfo ++ ", trying to reconnect since the connection might be stale.")
                -- Disconnect just to be safe (IOErrors are ignored)
                disconnect conn
                -- Retry once
                conn' <- fillConnRef
                res <- cont conn'
                return res
            (True, Left err) -> throwM err
            (_, Right res) -> return res
  where
    showConnectInfo = T.decodeUtf8 (connectHost (mcConnectInfo mc)) ++ ":" ++ tshow (connectPort (mcConnectInfo mc))

    tryTakeConnection :: m (Bool, IORef (Maybe Connection))
    tryTakeConnection = do
        mbConn <- atomically $ do
            Connections{..} <- readTVar (mcConnections mc)
            case connectionsAvailable of
                conn : conns -> do
                    writeTVar (mcConnections mc) (Connections connectionsActive conns)
                    return (Just conn)
                [] -> if mcMaxConnections mc > connectionsActive
                    then return Nothing
                    else STM.retry
        (isJust mbConn, ) <$> newIORef mbConn

    putConnection :: (Bool, IORef (Maybe Connection)) -> m ()
    putConnection (wasThere, connRef) = do
        mbConn <- readIORef connRef
        atomically $ do
            Connections{..} <- readTVar (mcConnections mc)
            case mbConn of
                Just conn -> let
                    active = connectionsActive + if wasThere then 0 else 1
                    in if active > mcMaxConnections mc
                        then return ()
                        else writeTVar (mcConnections mc) (Connections active (conn : connectionsAvailable))
                Nothing -> return ()
