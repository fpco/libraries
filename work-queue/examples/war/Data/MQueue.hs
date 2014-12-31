{-# LANGUAGE KindSignatures #-}
-- | A mutable queue implementation.
module Data.MQueue
    ( MQueue
    , newMQueue
    , pushMQueue
    , popMQueue
    ) where

import           Control.Monad               (liftM)
import           Control.Monad.Primitive     (PrimMonad, PrimState)
import           Data.Primitive.MutVar
import           Data.Sequence               (Seq)
import qualified Data.Sequence               as Seq
import qualified Data.Vector.Generic         as V
import qualified Data.Vector.Unboxed.Mutable as VMU

-- | A mutable queue, allowing items to be popped from the front and pushed to
-- the end.
--
-- NOTE: Currently implemented with just Seq under the surface. This is
-- purposely non-optimal.
newtype MQueue (v :: * -> * -> *) s a = MQueue (MutVar s (Seq a))

-- | Create a new, empty MQueue from the given @Vector@.
newMQueue :: (PrimMonad m, V.Vector v a)
          => v a -- ^ initial values in queue
          -> m (MQueue VMU.MVector (PrimState m) a)
newMQueue = liftM MQueue . newMutVar . Seq.fromList . V.toList

-- | Push an item to the end of the queue.
pushMQueue :: PrimMonad m => MQueue v (PrimState m) a -> a -> m ()
pushMQueue (MQueue var) a = modifyMutVar' var (Seq.|> a)

-- | Pop an item from the beginning of the queue.
popMQueue :: PrimMonad m => MQueue v (PrimState m) a -> m (Maybe a)
popMQueue (MQueue var) = do
    s <- readMutVar var
    case Seq.viewl s of
        Seq.EmptyL -> return Nothing
        x Seq.:< s' -> do
            writeMutVar var s'
            return $ Just x
