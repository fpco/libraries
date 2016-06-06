{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TypeFamilies     #-}
-- | A simple thread pool based on "Data.WorkQueue".  It allows you to
-- run 'IO' actions on a fixed number of threads.
module Control.Concurrent.ThreadPool
    ( ThreadPool
    , withThreadPool
    , mapTP
    , mapTP_
    ) where

import Control.Concurrent.MVar
import Control.Concurrent.STM      (atomically)
import Control.Monad               (void, when)
import Control.Monad.IO.Class
import Control.Monad.Trans.Control
import Data.IORef
import Data.MonoTraversable
import Data.Traversable
import Data.WorkQueue
import Prelude                     hiding (sequence)

-- | This stores info required to enqueue 'IO' actions to a thread
-- pool.
newtype ThreadPool = ThreadPool (WorkQueue (IO ()) ())

-- | Create a 'ThreadPool', and pass it to the inner function.  This
-- uses the bracket pattern to ensure that the 'ThreadPool' resources
-- get cleaned up when the inner function exits (either due to
-- finishing and yielding a value, or due to an exception).
withThreadPool :: MonadBaseControl IO m
               => Int -- ^ number of threads
               -> (ThreadPool -> m a) -- ^ inner function
               -> m a
withThreadPool threads inner = withWorkQueue $ \queue ->
    withLocalSlaves queue threads id (inner $ ThreadPool queue)

-- NOTE: changes to 'mapTP' and 'mapTP_' should probably also be made
-- to 'mapQueue' and 'mapQueue_', since they have very similar code.

-- | This is like 'Data.Traversable.mapM', but it runs the 'IO'
-- actions concurrently on the 'ThreadPool'.
mapTP :: (Traversable t, MonadIO m)
      => ThreadPool
      -> (a -> IO b)
      -> t a
      -> m (t b)
mapTP (ThreadPool queue) f t = liftIO $ do
    t' <- forM t $ \a -> do
        var <- newEmptyMVar
        atomically $ queueItem queue (f a >>= putMVar var) return
        return $ takeMVar var
    sequence t'

-- | This is like 'Data.Traversable.mapM', but it runs the 'IO'
-- actions concurrently on the 'ThreadPool', and ignores the resulting
-- values.
mapTP_ :: (MonoFoldable mono, Element mono ~ a, MonadIO m)
       => ThreadPool
       -> (a -> IO b)
       -> mono
       -> m ()
mapTP_ (ThreadPool queue) f t = liftIO $ do
    itemsCount <- newIORef (olength t)
    done <- newEmptyMVar
    let decrement = do
            cnt <- atomicModifyIORef itemsCount (\x -> (x - 1, x - 1))
            when (cnt == 0) $ putMVar done ()
    oforM_ t $ \a -> atomically $ queueItem queue (void (f a)) (const decrement)
    takeMVar done