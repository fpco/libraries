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
import Control.Monad.Trans.Control
import Control.Monad.Trans.Unlift
import qualified Data.Pool as Pool

import FP.Redis.Connection
import FP.Redis.Types.Internal

data ManagedConnection = ManagedConnection
    { mcPool :: !(Pool.Pool Connection)
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
    unlift <- askUnliftBase
    pool <- liftIO $ Pool.createPool
        (unliftBase unlift (connect cinfo))
        (\conn -> unliftBase unlift (disconnect conn))
        1
        (fromIntegral (100 :: Int)) -- ^ Keep it for 100 secs
        maxConns
    return ManagedConnection{mcPool = pool, mcConnectInfo = cinfo}

destroyManagedConnection :: (MonadIO m) => ManagedConnection -> m ()
destroyManagedConnection = liftIO . Pool.destroyAllResources . mcPool

withManagedConnection :: (MonadConnect m) => ConnectInfo -> Int -> (ManagedConnection -> m a) -> m a
withManagedConnection cinfo maxConns =
    bracket (newManagedConnection cinfo maxConns) destroyManagedConnection

managedConnectInfo :: ManagedConnection -> ConnectInfo
managedConnectInfo = mcConnectInfo

useConnection :: forall m a.
       (MonadBaseControl IO m)
    => ManagedConnection -> (Connection -> m a) -> m a
useConnection mc = Pool.withResource (mcPool mc)
