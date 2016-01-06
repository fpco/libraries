{-# LANGUAGE FlexibleContexts #-}
module Data.SimpleSupply
    ( Supply
    , SupplyM
    , newSupply
    , askSupply
    , withSupplyM
    , askSupplyM
    ) where

import ClassyPrelude
import Control.Monad.RWS.Strict
import Control.Monad.Trans.Control (MonadBaseControl)

data Supply a = Supply (IORef a) (a -> a)

type SupplyM a = RWS (a -> a) () a

newSupply :: MonadBaseControl IO m => a -> (a -> a) -> m (Supply a)
newSupply x0 f = do
    ref <- newIORef x0
    return (Supply ref f)

askSupply :: MonadBaseControl IO m => Supply a -> m a
askSupply s = withSupplyM s askSupplyM

withSupplyM
  :: MonadBaseControl IO m
  => Supply a
  -> SupplyM a r
  -> m r
withSupplyM (Supply ref f) inner =
    atomicModifyIORef' ref $ \x ->
        let (result, x', ()) = runRWS inner f x in (x', result)

askSupplyM :: SupplyM a a
askSupplyM = rws (\f x -> (x, f x, ()))
