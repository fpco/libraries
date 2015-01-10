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
import Control.Monad               (void)
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

mapTP :: (Traversable t, MonadIO m)
      => ThreadPool
      -> (a -> IO b)
      -> t a
      -> m (t b)
mapTP (ThreadPool queue) f t = liftIO $ do
    -- Stores a dlist of the items to enqueue.
    itemsRef <- newIORef id
    t' <- forM t $ \a -> do
        var <- newEmptyMVar
        modifyIORef itemsRef (((f a >>= putMVar var, return) :) .)
        return $ takeMVar var
    items <- readIORef itemsRef
    atomically $ modifyItems queue items
    sequence t'

mapTP_ :: (MonoFoldable mono, Element mono ~ a, MonadIO m)
       => ThreadPool
       -> (a -> IO b)
       -> mono
       -> m ()
mapTP_ (ThreadPool queue) f mono = liftIO $ do
    atomically $ queueItems queue $ map (\a -> (void $ f a, return)) $ otoList mono
    atomically $ checkEmptyWorkQueue queue
