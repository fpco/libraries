{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TypeFamilies     #-}
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

newtype ThreadPool = ThreadPool (WorkQueue (IO ()) ())

withThreadPool :: MonadBaseControl IO m
               => Int -- ^ number of threads
               -> (ThreadPool -> m a)
               -> m a
withThreadPool threads inner = withWorkQueue $ \queue ->
    withLocalSlaves queue threads id (inner $ ThreadPool queue)

-- NOTE: changes to 'mapTP' and 'mapTP_' should probably also be
-- made to 'mapQueue' and 'mapQueue_'

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

mapTP_ :: (MonoFoldable mono, Element mono ~ a, MonadIO m)
       => ThreadPool
       -> (a -> IO b)
       -> mono
       -> m ()
mapTP_ (ThreadPool queue) f mono = liftIO $ do
    let xs = otoList mono
    itemsCount <- newIORef (length xs)
    done <- newEmptyMVar
    let decrement = do
            cnt <- atomicModifyIORef itemsCount (\x -> (x - 1, x - 1))
            when (cnt == 0) $ putMVar done ()
    atomically $ queueItems queue $ map (\a -> (void $ f a, const decrement)) xs
    takeMVar done
