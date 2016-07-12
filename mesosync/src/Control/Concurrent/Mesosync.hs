{-# LANGUAGE CPP, MagicHash, UnboxedTuples, RankNTypes #-}
#if __GLASGOW_HASKELL__ >= 701
{-# LANGUAGE Safe #-}
#endif
{-# OPTIONS -Wall #-}
module Control.Concurrent.Mesosync
       ( cancel, cancelWith
         -- , withAsync, withAsyncBound, withAsyncOn, withAsyncWithUnmask, withAsyncOnWithUnmask
       , waitAnyCancel, waitAnyCatchCancel
       , waitEitherCancel
         -- , waitEitherCatchCancel
       , race, race_, concurrently, mapConcurrently
       , Concurrently (..)
       ) where


#if !MIN_VERSION_base(4,6,0)
import           Prelude hiding (catch)
#endif

import           Control.Applicative
import           Control.Concurrent
import           Control.Concurrent.Async (Async)
import qualified Control.Concurrent.Async as A
import           Control.Exception
import           Control.Monad
import           Data.IORef

cancel :: Async a -> IO ()
cancel = uninterruptibleMask_ . interruptibleCancel

interruptibleCancel :: Async a -> IO ()
interruptibleCancel x = A.cancel x <* A.waitCatch x
{-# INLINABLE interruptibleCancel #-}

cancelWith :: Exception e => Async a -> e -> IO ()
cancelWith x e = A.cancelWith x e <* A.waitCatch x

-- | Like 'waitAnyCatch', but also cancels the other asynchronous
-- operations as soon as one has completed.
--
waitAnyCatchCancel :: [Async a] -> IO (Async a, Either SomeException a)
waitAnyCatchCancel asyncs =
  A.waitAnyCatch asyncs `finally` mapM_ cancel asyncs

-- | Like 'waitAny', but also cancels the other asynchronous
-- operations as soon as one has completed.
--
waitAnyCancel :: [Async a] -> IO (Async a, a)
waitAnyCancel asyncs =
  A.waitAny asyncs `finally` mapM_ cancel asyncs

-- | Like 'waitEither', but also 'cancel's both @Async@s before
-- returning.
--
waitEitherCancel :: Async a -> Async b -> IO (Either a b)
waitEitherCancel left right =
  A.waitEither left right `finally` (cancel left >> cancel right)

race :: IO a -> IO b -> IO (Either a b)

race left right = concurrently' left right collect
  where
    collect m = do
        e <- m
        case e of
            Left ex -> throwIO ex
            Right r -> return r

race_ :: IO a -> IO b -> IO ()
race_ left right = void $ race left right

concurrently :: IO a -> IO b -> IO (a,b)
concurrently left right = concurrently' left right (collect [])
  where
    collect [Left a, Right b] _ = return (a,b)
    collect [Right b, Left a] _ = return (a,b)
    collect xs m = do
        e <- m
        case e of
            Left ex -> throwIO ex
            Right r -> collect (r:xs) m

concurrently' :: IO a -> IO b
             -> (IO (Either SomeException (Either a b)) -> IO r)
             -> IO r
concurrently' left right collect = do
    done <- newEmptyMVar
    mask $ \restore -> do
        lid <- forkIO $ restore (left >>= putMVar done . Right . Left)
                             `catchAll` (putMVar done . Left)
        rid <- forkIO $ restore (right >>= putMVar done . Right . Right)
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
                uninterruptibleMask_ $ do
                    killThread rid >> killThread lid

                    -- ensure the children are really dead
                    count' <- readIORef count
                    replicateM_ count' (takeMVar done)
        r <- restore (collect takeDone) `onException` stop
        stop
        return r

catchAll :: IO a -> (SomeException -> IO a) -> IO a
catchAll = catch


-- | maps an @IO@-performing function over any @Traversable@ data
-- type, performing all the @IO@ actions concurrently, and returning
-- the original data structure with the arguments replaced by the
-- results.
--
-- For example, @mapConcurrently@ works with lists:
--
-- > pages <- mapConcurrently getURL ["url1", "url2", "url3"]
--
mapConcurrently :: Traversable t => (a -> IO b) -> t a -> IO (t b)
mapConcurrently f = runConcurrently . traverse (Concurrently . f)

-- -----------------------------------------------------------------------------

-- | A value of type @Concurrently a@ is an @IO@ operation that can be
-- composed with other @Concurrently@ values, using the @Applicative@
-- and @Alternative@ instances.
--
-- Calling @runConcurrently@ on a value of type @Concurrently a@ will
-- execute the @IO@ operations it contains concurrently, before
-- delivering the result of type @a@.
--
-- For example
--
-- > (page1, page2, page3)
-- >     <- runConcurrently $ (,,)
-- >     <$> Concurrently (getURL "url1")
-- >     <*> Concurrently (getURL "url2")
-- >     <*> Concurrently (getURL "url3")
--
newtype Concurrently a = Concurrently { runConcurrently :: IO a }

instance Functor Concurrently where
  fmap f (Concurrently a) = Concurrently $ f <$> a

instance Applicative Concurrently where
  pure = Concurrently . return
  Concurrently fs <*> Concurrently as =
    Concurrently $ (\(f, a) -> f a) <$> concurrently fs as

instance Alternative Concurrently where
  empty = Concurrently $ forever (threadDelay maxBound)
  Concurrently as <|> Concurrently bs =
    Concurrently $ either id id <$> race as bs

instance Monad Concurrently where
  return = pure
  Concurrently a >>= f =
    Concurrently $ a >>= runConcurrently . f
