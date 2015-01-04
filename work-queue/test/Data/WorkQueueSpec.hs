module Data.WorkQueueSpec (spec) where

import Test.Hspec
import Data.WorkQueue
import qualified Data.Vector.Unboxed.Mutable as V
import Control.Monad (forM_)
import Control.Monad.STM (atomically)
import Data.IORef
import Control.Concurrent

spec :: Spec
spec = do
    it "fill a vector" $ do
        let count = 20
        ref <- newIORef 0
        v <- V.new count
        forM_ [0..count - 1] $ \i -> V.write v i (- 1)
        withWorkQueue $ \queue -> do
            withLocalSlaves queue 2 (\i -> V.write v i i) $ do
                atomically $ queueItems queue $ flip map [0..count - 1] $ \i ->
                    (i, \j -> do
                        threadDelay 100000
                        atomicModifyIORef' ref $ \t -> (t + 1, ())
                        return j)
        cnt <- readIORef ref
        cnt `shouldBe` count
        forM_ [0..count - 1] $ \i -> do
            x <- V.read v i
            x `shouldBe` i
    -- FIXME add tests of exceptions being thrown
