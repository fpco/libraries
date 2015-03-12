{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE DeriveDataTypeable #-}
-- | A work queue, for distributed workloads among multiple local threads.
--
-- To distribute workloads to remote nodes, see "Distributed.WorkQueue", which
-- builds on top of this module.
module Data.WorkQueue
    ( WorkQueue
    , newWorkQueue
    , closeWorkQueue
    , withWorkQueue
    , queueItem
    , queueItems
    , checkEmptyWorkQueue
    , provideWorker
    , withLocalSlave
    , withLocalSlaves
    , mapQueue
    , mapQueue_
    ) where

import Control.Applicative         ((<$), (<$>), (<*>), (<|>))
import Control.Concurrent          (threadDelay)
import Control.Concurrent.Async    (race)
import Control.Concurrent.MVar
import Control.Concurrent.STM      (STM, STM, TMVar, TVar, atomically, check,
                                    modifyTVar', newEmptyTMVar, newTVar,
                                    readTMVar, readTVar, retry, tryPutTMVar,
                                    writeTVar)
import Control.Exception           (SomeException, catch, finally, mask,
                                    throwIO)
import Control.Exception.Lifted    (bracket)
import Control.Monad               (forever, join, void, when)
import Control.Monad.Base          (liftBase)
import Control.Monad.IO.Class
import Control.Monad.Trans.Control (MonadBaseControl, control)
import Data.IORef
import Data.MonoTraversable
import Data.Traversable
import Data.Typeable               (Typeable)
import Data.Void                   (absurd)
import Prelude                     hiding (sequence)

-- | A queue of work items to be performed, where each work item is of type
-- @payload@ and whose computation gives a result of type @result@.
data WorkQueue payload result = WorkQueue
    (TVar [(payload, result -> IO ())])
    (TVar Int) -- active workers
    (TMVar ()) -- filled when the work queue is closed
    deriving (Typeable)

-- | Create a new, empty, open work queue.
newWorkQueue :: STM (WorkQueue payload result)
newWorkQueue = WorkQueue <$> newTVar [] <*> newTVar 0 <*> newEmptyTMVar

-- | Close a work queue, so that all workers will exit.
closeWorkQueue :: WorkQueue payload result -> STM ()
closeWorkQueue (WorkQueue _ _ var) = void $ tryPutTMVar var ()

-- | Convenient wrapper for creating a work queue, bracketting, performing some
-- action, and closing it.
withWorkQueue :: MonadBaseControl IO m
              => (WorkQueue payload result -> m a)
              -> m a
withWorkQueue = bracket
    (liftBase $ atomically newWorkQueue)
    (liftBase . atomically . closeWorkQueue)

-- | Queue a single item in the work queue.  This prepends the item,
-- so it will be the next item taken by a worker.
queueItem :: WorkQueue payload result
          -> payload -- ^ payload to be computed
          -> (result -> IO ()) -- ^ action to be performed with its result
          -> STM ()
queueItem (WorkQueue var _ _) p f = modifyTVar' var ((p, f):)

-- | Queue multiple work items. This is only provided for convenience
-- and performance vs 'queueItem'; it gives identical atomicity
-- guarantees as calling 'queueItem' multiple times in the same STM
-- transaction.
--
-- Note that this will enqueue the items in the opposite order as
-- @mapM (\(x, f) -> queueItem queue x f)@.  This is because
-- 'queueItem' prepends each element, so that last item will end up on
-- the front of the queue.
queueItems :: WorkQueue payload result -> [(payload, result -> IO ())] -> STM ()
queueItems (WorkQueue var _ _) items = modifyTVar' var (items ++)

-- | Block until the work queue is empty. That is, until there are no work
-- items in the queue, and no work items checked out by a worker.
checkEmptyWorkQueue :: WorkQueue payload result -> STM ()
checkEmptyWorkQueue (WorkQueue work active _) = do
    readTVar work >>= (check . null)
    readTVar active >>= (check . (== 0))

-- | Provide a worker to perform computations. This function will repeatedly
-- request payloads from the @WorkQueue@, and for each payload perform the
-- given computation and provide the result back (using the function provided
-- in 'queueItem'). This function will block if the work queue is empty, and
-- will only exit after the queue is closed via 'closeWorkQueue'.
--
-- When the computation throws an exception, the exception is thrown
-- and the request gets re-enqueued on the queue.  This also applies
-- to exceptions thrown by the result handling functions passed to
-- 'queueItem' and 'queueItems', because this result handling function
-- is run by the worker.
--
-- While 'provideWorker' must perform actions on the local machine, it can be
-- leveraged to send payloads to remote machines. This is what the
-- "Distributed.WorkQueue" module provides.
provideWorker :: WorkQueue payload result -> (payload -> IO result) -> IO ()
provideWorker (WorkQueue work active final) onPayload =
    loop
  where
    loop = join $ mask $ \restore -> do
        mwork <- atomically $ (Just <$> getWork) <|> (Nothing <$ getFinal)
        case mwork of
            Nothing -> return $ return ()
            Just tuple@(payload, onResult) -> (`finally` decOpen) $ do
                restore (onPayload payload >>= onResult) `catch` \e -> do
                    atomically $ modifyTVar' work (tuple:)
                    throwIO (e :: SomeException)
                return loop

    decOpen = atomically $ modifyTVar' active (subtract 1)

    getWork = do
        work' <- readTVar work
        case work' of
            [] -> retry
            x:xs -> do
                writeTVar work xs
                modifyTVar' active (+ 1)
                return x
    getFinal = readTMVar final

-- | Same as 'withLocalSlaves', but with a thread count of 1.
withLocalSlave :: MonadBaseControl IO m
               => WorkQueue payload result
               -> (payload -> IO result)
               -> m a
               -> m a
withLocalSlave queue = withLocalSlaves queue 1

-- | Start local slaves against the given work queue.
--
-- Any exception thrown by a slave will be rethrown, and also cancel
-- all of the other workers. This call will not return until the work
-- queue is empty.
--
-- Note that you may run as many local slaves as desired, by nesting calls to
-- @withLocalSlave@. A convenience function for doing so is 'withLocalSlaves'.
--
-- Comparisons to the distributed @runSlave@:
--
-- This function is more efficient than @runSlave@ in that it can share data
-- structures in memory, avoiding network and serialization overhead.
--
-- Unlike @runSlave@, the calculation function does not take an explicit
-- @initialData@. Since this computation is run on a local machine, that data
-- can be provided to the function via currying.
withLocalSlaves :: MonadBaseControl IO m
                => WorkQueue payload result
                -> Int -- ^ slave count
                -> (payload -> IO result)
                -> m a
                -> m a
withLocalSlaves _ count _ inner | count <= 0 = inner
withLocalSlaves queue count0 calc inner =
    control $ \runInBase ->
        let loop 0 = do
                x <- runInBase inner
                atomically $ checkEmptyWorkQueue queue
                return x
            loop i = fmap getRight $ race slave $ loop $ i - 1
            slave = provideWorker queue calc >> forever (threadDelay maxBound)
            getRight (Left x) = absurd x
            getRight (Right x) = x
         in loop count0

-- NOTE: changes to 'mapQueue' and 'mapQueue_' should probably also be
-- made to 'mapTP' and 'mapTP_'

-- | This the items within the 'Traversable' and enqueues it on the
-- 'WorkQueue'.  Once all of this work is completed, the results are
-- yielded as a 'Traversable' with the same structure, where the
-- payloads have been replaced with their results.
mapQueue :: (MonadIO m, Traversable t)
         => WorkQueue payload result
         -> t payload
         -> m (t result)
mapQueue queue t = liftIO $ do
    t' <- forM t $ \a -> do
        var <- newEmptyMVar
        atomically $ queueItem queue a $ putMVar var
        return $ takeMVar var
    sequence t'

-- | This is similar to 'mapQueue', except it ignores the results.
mapQueue_ :: (MonadIO m, MonoFoldable mono, Element mono ~ payload)
          => WorkQueue payload result
          -> mono
          -> m ()
mapQueue_ queue t = liftIO $ do
    itemsCount <- newIORef (olength t)
    done <- newEmptyMVar
    let decrement = do
            cnt <- atomicModifyIORef itemsCount (\x -> (x - 1, x - 1))
            when (cnt == 0) $ putMVar done ()
    oforM_ t $ \a -> atomically $ queueItem queue a (const decrement)
    takeMVar done
