{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}
module Control.Concurrent.Mesosync.Lifted.Safe

    (
      -- * Asynchronous actions
      A.Async
           
    , ALS.Pure
    , ALS.Forall
      -- ** Spawning
    , ALS.async, ALS.asyncBound, ALS.asyncOn, ALS.asyncWithUnmask, ALS.asyncOnWithUnmask
                                                      
      -- ** Spawning with automatic 'cancel'ation
    , withAsync, withAsyncBound, withAsyncOn
    , withAsyncWithUnmask, withAsyncOnWithUnmask

      -- ** Quering 'Async's
    , ALS.wait, ALS.poll, ALS.waitCatch
    , cancel, cancelWith
    , A.asyncThreadId

      -- ** STM operations
    , A.waitSTM, A.pollSTM, A.waitCatchSTM

      -- ** Waiting for multiple 'Async's
    , ALS.waitAny, ALS.waitAnyCatch, waitAnyCancel, waitAnyCatchCancel
    , ALS.waitEither, ALS.waitEitherCatch, waitEitherCancel, ALS.waitEitherCatchCancel
    , ALS.waitEither_
    , ALS.waitBoth

    -- ** Linking
  , Unsafe.link, Unsafe.link2

    -- * Convenient utilities
  , race, race_, concurrently, mapConcurrently
  , Concurrently(..)
  ) where

import qualified Control.Concurrent.Async as A hiding
       ( waitEitherCancel, waitEitherCatchCancel
       , withAsync, withAsyncBound, withAsyncOn, withAsyncWithUnmask, withAsyncOnWithUnmask
       , waitAnyCancel, waitAnyCatchCancel
       , cancel, cancelWith
       , race, race_, concurrently, mapConcurrently
       , Concurrently)
import qualified Control.Concurrent.Async.Lifted.Safe as ALS hiding
       ( withAsync, withAsyncBound, withAsyncOn
       , withAsyncWithUnmask, withAsyncOnWithUnmask
       , cancel, cancelWith
       , waitAnyCancel, waitAnyCatchCancel
       , waitEitherCancel
       , race, race_, concurrently, mapConcurrently
       )
import qualified Control.Concurrent.Mesosync.Lifted as Unsafe
import Control.Applicative
import Control.Concurrent (threadDelay)
import Control.Monad
import Control.Concurrent.Async (Async)
import Control.Exception.Lifted (SomeException, Exception)
import Control.Monad.Base (MonadBase(..))
import Control.Monad.Trans.Control hiding (restoreM)
import Data.Constraint ((\\), (:-))
import Data.Constraint.Forall (Forall, inst)


-- | Generalized version of 'A.withAsync'.
withAsync
  :: forall m a b. (MonadBaseControl IO m, Forall (ALS.Pure m))
  => m a
  -> (Async a -> m b)
  -> m b
withAsync = Unsafe.withAsync
  \\ (inst :: Forall (ALS.Pure m) :- ALS.Pure m a)

-- | Generalized version of 'A.withAsyncBound'.
withAsyncBound
  :: forall m a b. (MonadBaseControl IO m, Forall (ALS.Pure m))
  => m a
  -> (Async a -> m b)
  -> m b
withAsyncBound = Unsafe.withAsyncBound
  \\ (inst :: Forall (ALS.Pure m) :- ALS.Pure m a)

-- | Generalized version of 'A.withAsyncOn'.
withAsyncOn
  :: forall m a b. (MonadBaseControl IO m, Forall (ALS.Pure m))
  => Int
  -> m a
  -> (Async a -> m b)
  -> m b
withAsyncOn = Unsafe.withAsyncOn
  \\ (inst :: Forall (ALS.Pure m) :- ALS.Pure m a)

-- | Generalized version of 'A.withAsyncWithUnmask'.
withAsyncWithUnmask
  :: forall m a b. (MonadBaseControl IO m, Forall (ALS.Pure m))
  => ((forall c. m c -> m c) -> m a)
  -> (Async a -> m b)
  -> m b
withAsyncWithUnmask restore = Unsafe.withAsyncWithUnmask restore
  \\ (inst :: Forall (ALS.Pure m) :- ALS.Pure m a)

-- | Generalized version of 'A.withAsyncOnWithUnmask'.
withAsyncOnWithUnmask
  :: forall m a b. (MonadBaseControl IO m, Forall (ALS.Pure m))
  => Int
  -> ((forall c. m c -> m c) -> m a)
  -> (Async a -> m b)
  -> m b
withAsyncOnWithUnmask cpu restore = Unsafe.withAsyncOnWithUnmask cpu restore
  \\ (inst :: Forall (ALS.Pure m) :- ALS.Pure m a)

-- | Generalized version of 'A.cancel'.
cancel :: MonadBase IO m => Async a -> m ()
cancel = Unsafe.cancel

-- | Generalized version of 'A.cancelWith'.
cancelWith :: (MonadBase IO m, Exception e) => Async a -> e -> m ()
cancelWith = Unsafe.cancelWith

-- | Generalized version of 'A.waitAnyCancel'.
waitAnyCancel
  :: forall m a. (MonadBaseControl IO m, Forall (ALS.Pure m))
  => [Async a]
  -> m (Async a, a)
waitAnyCancel = Unsafe.waitAnyCancel
  \\ (inst :: Forall (ALS.Pure m) :- ALS.Pure m a)

-- | Generalized version of 'A.waitAnyCatchCancel'.
waitAnyCatchCancel
  :: forall m a. (MonadBaseControl IO m, Forall (ALS.Pure m))
  => [Async a]
  -> m (Async a, Either SomeException a)
waitAnyCatchCancel = Unsafe.waitAnyCatchCancel
  \\ (inst :: Forall (ALS.Pure m) :- ALS.Pure m a)

-- | Generalized version of 'A.waitEitherCancel'.
waitEitherCancel
  :: forall m a b. (MonadBaseControl IO m, Forall (ALS.Pure m))
  => Async a
  -> Async b
  -> m (Either a b)
waitEitherCancel = Unsafe.waitEitherCancel
  \\ (inst :: Forall (ALS.Pure m) :- ALS.Pure m a)
  \\ (inst :: Forall (ALS.Pure m) :- ALS.Pure m b)

-- | Generalized version of 'A.race'.
race
  :: forall m a b. (MonadBaseControl IO m, Forall (ALS.Pure m))
  => m a -> m b -> m (Either a b)
race = Unsafe.race
  \\ (inst :: Forall (ALS.Pure m) :- ALS.Pure m a)
  \\ (inst :: Forall (ALS.Pure m) :- ALS.Pure m b)

-- | Generalized version of 'A.race_'.
race_
  :: forall m a b. (MonadBaseControl IO m, Forall (ALS.Pure m))
  => m a -> m b -> m ()
race_ = Unsafe.race_
  \\ (inst :: Forall (ALS.Pure m) :- ALS.Pure m a)
  \\ (inst :: Forall (ALS.Pure m) :- ALS.Pure m b)

-- | Generalized version of 'A.concurrently'.
concurrently
  :: forall m a b. (MonadBaseControl IO m, Forall (ALS.Pure m))
  => m a -> m b -> m (a, b)
concurrently = Unsafe.concurrently
  \\ (inst :: Forall (ALS.Pure m) :- ALS.Pure m a)
  \\ (inst :: Forall (ALS.Pure m) :- ALS.Pure m b)

-- | Generalized version of 'A.mapConcurrently'.
mapConcurrently
  :: (Traversable t, MonadBaseControl IO m, Forall (ALS.Pure m))
  => (a -> m b)
  -> t a
  -> m (t b)
mapConcurrently f = runConcurrently . traverse (Concurrently . f)

-- | Generalized version of 'A.Concurrently'.
--
-- A value of type @'Concurrently' m a@ is an IO-based operation that can be
-- composed with other 'Concurrently' values, using the 'Applicative' and
-- 'Alternative' instances.
--
-- Calling 'runConcurrently' on a value of type @'Concurrently' m a@ will
-- execute the IO-based lifted operations it contains concurrently, before
-- delivering the result of type 'a'.
--
-- For example
--
-- @
--   (page1, page2, page3) <- 'runConcurrently' $ (,,)
--     '<$>' 'Concurrently' (getURL "url1")
--     '<*>' 'Concurrently' (getURL "url2")
--     '<*>' 'Concurrently' (getURL "url3")
-- @
data Concurrently m a where
  Concurrently
    :: Forall (ALS.Pure m) => { runConcurrently :: m a } -> Concurrently m a

instance Functor m => Functor (Concurrently m) where
  fmap f (Concurrently a) = Concurrently $ f <$> a

instance (MonadBaseControl IO m, Forall (ALS.Pure m)) =>
  Applicative (Concurrently m) where
    pure = Concurrently . pure
    Concurrently (fs :: m (a -> b)) <*> Concurrently as =
      Concurrently (uncurry ($) <$> concurrently fs as)
        \\ (inst :: Forall (ALS.Pure m) :- ALS.Pure m a)
        \\ (inst :: Forall (ALS.Pure m) :- ALS.Pure m (a -> b))

instance (MonadBaseControl IO m, Forall (ALS.Pure m)) =>
  Alternative (Concurrently m) where
    empty = Concurrently $ liftBaseWith $ const (forever $ threadDelay maxBound)
    Concurrently (as :: m a) <|> Concurrently bs =
      Concurrently (either id id <$> race as bs)
        \\ (inst :: Forall (ALS.Pure m) :- ALS.Pure m a)
        \\ (inst :: Forall (ALS.Pure m) :- ALS.Pure m b)

instance (MonadBaseControl IO m, Forall (ALS.Pure m)) =>
  Monad (Concurrently m) where
    return = Concurrently . return
    Concurrently a >>= f = Concurrently $ a >>= runConcurrently . f
