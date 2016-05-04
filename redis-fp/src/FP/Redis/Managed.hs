{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
module FP.Redis.Managed
    ( ManagedConnection
    , newManagedConnection
    , destroyManagedConnection
    , withManagedConnection
    , useConnection
    ) where

import ClassyPrelude
import Control.Monad.Logger

import FP.Redis.Connection
import FP.Redis.Types.Internal

data ManagedConnection = ManagedConnection
    { mcConnection :: !(MVar Connection)
    , mcConnectInfo :: !ConnectInfo
    }

-- | We connect at the beginning (and not lazily) so that an eventual connection
-- error is immediately returned
newManagedConnection :: (MonadCommand m) => ConnectInfo -> m ManagedConnection
newManagedConnection cinfo = do
    connVar <- newMVar =<< connect cinfo
    return ManagedConnection{mcConnection = connVar, mcConnectInfo = cinfo}

destroyManagedConnection :: (MonadCommand m) => ManagedConnection -> m ()
destroyManagedConnection conn = do
    disconnect =<< takeMVar (mcConnection conn)

withManagedConnection :: (MonadConnect m) => ConnectInfo -> (ManagedConnection -> m a) -> m a
withManagedConnection cinfo = bracket (newManagedConnection cinfo) destroyManagedConnection

useConnection ::
       (MonadConnect m)
    => ManagedConnection -> (Connection -> m a) -> m a
useConnection mc cont = do
    mbErr :: Either SomeException a <-
        modifyMVar (mcConnection mc) $ \conn -> do
            mbErr :: Either SomeException a <- try (cont conn)
            -- Retry once. The connection might be stale.
            case mbErr of
                Left err -> case fromException err of
                    Just (ioe :: IOError) -> do
                        $logWarn ("Encountered IOError " ++ tshow ioe ++ " when trying to use a connection to " ++ tshow (mcConnectInfo mc) ++ ", since this is the first attempt I will try to reconnect.")
                        -- Disconnect just to be safe (IOErrors are ignored)
                        disconnect conn
                        -- Retry once
                        conn' <- connect (mcConnectInfo mc)
                        res <- try (cont conn')
                        return (conn', res)
                    Nothing -> return (conn, Left err)
                Right res -> return (conn, Right res)
    either throwIO return mbErr