{-# LANGUAGE NoImplicitPrelude, RankNTypes #-}

-- | TQueue that tracks its size.
module Control.Concurrent.STM.TSQueue where

import ClassyPrelude

-- | See 'newTQueue'
newTSQueue :: forall a. STM (TSQueue a)
newTSQueue = do
    lengthTVar <- newTVar 0
    tqueue <- newTQueue
    return (TSQueue lengthTVar tqueue)

-- | Get size of the queue
lengthTSQueue :: forall t. TSQueue t -> STM Int
lengthTSQueue (TSQueue lengthTVar _) =
    readTVar lengthTVar

-- | See 'writeTQueue'
writeTSQueue :: forall a. TSQueue a -> a -> STM ()
writeTSQueue (TSQueue lengthTVar tqueue) val = do
    writeTQueue tqueue val
    modifyTVar' lengthTVar (+ 1)

-- | See 'unGetTQueue'
unGetTSQueue :: forall a. TSQueue a -> a -> STM ()
unGetTSQueue (TSQueue lengthTVar tqueue) val = do
    unGetTQueue tqueue val
    modifyTVar' lengthTVar (+ 1)

-- | See 'readTQueue'
readTSQueue :: forall b. TSQueue b -> STM b
readTSQueue (TSQueue lengthTVar tqueue) = do
    val <- readTQueue tqueue
    modifyTVar' lengthTVar (subtract 1)
    return val

-- | See 'tryReadTQueue'
tryReadTSQueue :: forall a. TSQueue a -> STM (Maybe a)
tryReadTSQueue (TSQueue lengthTVar tqueue) = do
    mval <- tryReadTQueue tqueue
    when (isJust mval) (modifyTVar' lengthTVar (subtract 1))
    return mval

-- | Read the whole queue and return as a REVERSED list.
readAllReversedTSQueue :: forall a. TSQueue a -> STM [a]
readAllReversedTSQueue q = loop []
  where
    loop vals = do
        mval <- tryReadTSQueue q
        case mval of
            Nothing -> return vals
            Just val -> loop (val:vals)

-- | TQueue that tracks its size
data TSQueue a = TSQueue (TVar Int) (TQueue a)
