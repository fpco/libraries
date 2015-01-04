module Data.WorkQueueSpec (spec) where

import Test.Hspec
import Data.WorkQueue
import qualified Data.Vector.Unboxed.Mutable as V
import Control.Monad (forM_)
import Control.Monad.STM (atomically)

spec :: Spec
spec = do
    it "fill a vector" $ do
        let count = 20
        v <- V.new count
        withWorkQueue $ \queue -> do
            withLocalSlaves queue 2 (\i -> V.write v i i) $ do
                atomically $ queueItems queue $ flip map [0..count - 1] $ \i ->
                    (i, return)
        forM_ [0..count - 1] $ \i -> do
            x <- V.read v i
            x `shouldBe` i
