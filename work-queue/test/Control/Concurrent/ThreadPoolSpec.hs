module Control.Concurrent.ThreadPoolSpec (spec) where

import Test.Hspec
import Control.Concurrent.ThreadPool
import Data.IORef

spec :: Spec
spec = do
    it "mapTP" $ withThreadPool 4 $ \tp -> do
        res <- mapTP tp (\x -> return $ x + 1) [1..10]
        res `shouldBe` [2..11]
    it "mapTP_" $ withThreadPool 4 $ \tp -> do
        ref <- newIORef 0
        let go i = atomicModifyIORef' ref $ \x -> (x + i, ())
        mapTP_ tp go [1..10]
        res <- readIORef ref
        res `shouldBe` sum [1..10]
