{-# LANGUAGE CPP #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}
module Control.Concurrent.Mesosync.Lifted
    ( -- * Asynchronous actions
      A.Async
      -- ** Spawning
    , AL.async, AL.asyncBound, AL.asyncOn
    , AL.asyncWithUnmask, AL.asyncOnWithUnmask

    -- ** Spawning with automatic 'cancel'ation
    , withAsync, withAsyncBound, withAsyncOn
    , withAsyncWithUnmask, withAsyncOnWithUnmask

      -- ** Quering 'Async's
    , AL.wait, AL.poll, AL.waitCatch
    , cancel
    , cancelWith
    , AL.asyncThreadId

      -- ** STM operations
    , A.waitSTM, A.pollSTM, A.waitCatchSTM

      -- ** Waiting for multiple 'Async's
    , AL.waitAny, AL.waitAnyCatch
    , waitAnyCancel, waitAnyCatchCancel
    , AL.waitEither, AL.waitEitherCatch
    , waitEitherCancel
    , AL.waitEitherCatchCancel
    , AL.waitEither_
    , AL.waitBoth

      -- ** Linking
    , AL.link, AL.link2

      -- * Convenient utilities
    , race, race_, concurrently, mapConcurrently
    , Concurrently(..)
    ) where

import           Control.Applicative
import           Control.Concurrent.Async (Async)
import qualified Control.Concurrent.Async as A hiding
       ( cancel, cancelWith
       , withAsync, withAsyncBound, withAsyncOn, withAsyncWithUnmask, withAsyncOnWithUnmask
       , waitAnyCancel, waitAnyCatchCancel
       , waitEitherCancel, waitEitherCatchCancel
       , race, race_, concurrently, mapConcurrently
       , Concurrently)
import qualified Control.Concurrent.Async.Lifted as AL
import           Control.Concurrent.Lifted
import qualified Control.Concurrent.Mesosync as M
import           Control.Exception.Lifted (SomeException, Exception)
import qualified Control.Exception.Lifted as E
import           Control.Monad
import           Control.Monad.Base (MonadBase(..))
import           Control.Monad.Trans.Control
import           Data.IORef.Lifted
import           GHC.IO (unsafeUnmask)


-- | Generalized version of 'M.cancel'.
--
-- NOTE: This function discards the monadic effects besides IO in the forked
-- computation.
cancel :: MonadBase IO m => Async a -> m ()
cancel = liftBase . M.cancel

-- | Generalized version of 'M.cancelWith'.
--
-- NOTE: This function discards the monadic effects besides IO in the forked
-- computation.
cancelWith :: (MonadBase IO m, Exception e) => Async a -> e -> m ()
cancelWith = (liftBase .) . M.cancelWith

-- | Generalized version of 'A.withAsync'.
withAsync
  :: MonadBaseControl IO m
  => m a
  -> (Async (StM m a) -> m b)
  -> m b
withAsync = withAsyncUsing AL.async
{-# INLINABLE withAsync #-}

-- | Generalized version of 'A.withAsyncBound'.
withAsyncBound
  :: MonadBaseControl IO m
  => m a
  -> (Async (StM m a) -> m b)
  -> m b
withAsyncBound = withAsyncUsing AL.asyncBound
{-# INLINABLE withAsyncBound #-}

-- | Generalized version of 'A.withAsyncOn'.
withAsyncOn
  :: MonadBaseControl IO m
  => Int
  -> m a
  -> (Async (StM m a) -> m b)
  -> m b
withAsyncOn = withAsyncUsing . AL.asyncOn
{-# INLINABLE withAsyncOn #-}

-- | Generalized version of 'A.withAsyncWithUnmask'.
withAsyncWithUnmask
  :: MonadBaseControl IO m
  => ((forall c. m c -> m c) -> m a)
  -> (Async (StM m a) -> m b)
  -> m b
withAsyncWithUnmask actionWith =
  withAsyncUsing AL.async (actionWith (liftBaseOp_ unsafeUnmask))
{-# INLINABLE withAsyncWithUnmask #-}

-- | Generalized version of 'A.withAsyncOnWithUnmask'.
withAsyncOnWithUnmask
  :: MonadBaseControl IO m
  => Int
  -> ((forall c. m c -> m c) -> m a)
  -> (Async (StM m a) -> m b)
  -> m b
withAsyncOnWithUnmask cpu actionWith =
  withAsyncUsing (AL.asyncOn cpu) (actionWith (liftBaseOp_ unsafeUnmask))
{-# INLINABLE withAsyncOnWithUnmask #-}

withAsyncUsing
  :: MonadBaseControl IO m
  => (m a -> m (Async (StM m a)))
  -> m a
  -> (Async (StM m a) -> m b)
  -> m b
withAsyncUsing doFork action inner = E.mask $ \restore -> do
  a <- doFork $ restore action
  r <- restore (inner a) `E.catch` \e -> do
    cancel a
    E.throwIO (e :: SomeException)
  cancel a
  return r

-- | Generalized version of 'A.waitAnyCancel'.
waitAnyCancel
  :: MonadBaseControl IO m
  => [Async (StM m a)]
  -> m (Async (StM m a), a)
waitAnyCancel as = do
  (a, s) <- liftBase $ M.waitAnyCancel as
  r <- restoreM s
  return (a, r)


-- | Generalized version of 'A.waitAnyCatchCancel'.
waitAnyCatchCancel
  :: MonadBaseControl IO m
  => [Async (StM m a)]
  -> m (Async (StM m a), Either SomeException a)
waitAnyCatchCancel as = do
  (a, s) <- liftBase $ M.waitAnyCatchCancel as
  r <- sequenceEither s
  return (a, r)

sequenceEither :: MonadBaseControl IO m => Either e (StM m a) -> m (Either e a)
sequenceEither = either (return . Left) (liftM Right . restoreM)

-- | Generalized version of 'A.waitEitherCancel'.
waitEitherCancel
  :: MonadBaseControl IO m
  => Async (StM m a)
  -> Async (StM m b)
  -> m (Either a b)
waitEitherCancel a b =
  liftBase (M.waitEitherCancel a b) >>=
  either (liftM Left . restoreM) (liftM Right . restoreM)


race :: MonadBaseControl IO m => m a -> m b -> m (Either a b)
race left right = concurrently' left right collect
  where
    collect m = do
        e <- m
        case e of
            Left ex -> E.throw ex
            Right r -> return r

race_ :: MonadBaseControl IO m => m a -> m b -> m ()
race_ left right = void $ race left right

concurrently :: MonadBaseControl IO m => m a -> m b -> m (a,b)
concurrently left right = concurrently' left right (collect [])
  where
    collect [Left a, Right b] _ = return (a,b)
    collect [Right b, Left a] _ = return (a,b)
    collect xs m = do
        e <- m
        case e of
            Left ex -> E.throw ex
            Right r -> collect (r:xs) m


concurrently' :: MonadBaseControl IO m => m a -> m b
             -> (m (Either SomeException (Either a b)) -> m r)
             -> m r
concurrently' left right collect = do
    done <- newEmptyMVar
    E.mask $ \restore -> do
        lid <- fork $ restore (left >>= putMVar done . Right . Left)
                             `catchAll` (putMVar done . Left)
        rid <- fork $ restore (right >>= putMVar done . Right . Right)
                             `catchAll` (putMVar done . Left)

        count <- newIORef (2 :: Int)
        let takeDone = do
                -- Decrement the counter so we know how many takes are left.
                -- Since only the parent thread is calling this, we can
                -- use non-atomic modifications.
                modifyIORef count (subtract 1)

                takeMVar done

        let stop = do
                -- kill right before left, to match the semantics of
                -- the version using withAsync. (#27)
                E.uninterruptibleMask_ (killThread rid >> killThread lid)

                -- ensure the children are really dead
                count' <- readIORef count
                replicateM_ count' (takeMVar done)
        r <- restore (collect takeDone) `E.onException` stop
        stop
        return r

catchAll :: MonadBaseControl IO m => m a -> (SomeException -> m a) -> m a
catchAll = E.catch

-- | Generalized version of 'A.mapConcurrently'.
mapConcurrently
  :: (Traversable t, MonadBaseControl IO m)
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
newtype Concurrently m a = Concurrently { runConcurrently :: m a }

instance Functor m => Functor (Concurrently m) where
  fmap f (Concurrently a) = Concurrently $ f <$> a

instance MonadBaseControl IO m => Applicative (Concurrently m) where
  pure = Concurrently . pure
  Concurrently fs <*> Concurrently as =
    Concurrently $ uncurry ($) <$> concurrently fs as

instance MonadBaseControl IO m => Alternative (Concurrently m) where
  empty = Concurrently $ liftBaseWith $ const (forever $ threadDelay maxBound)
  Concurrently as <|> Concurrently bs =
    Concurrently $ either id id <$> race as bs

instance MonadBaseControl IO m => Monad (Concurrently m) where
  return = Concurrently . return
  Concurrently a >>= f = Concurrently $ a >>= runConcurrently . f
