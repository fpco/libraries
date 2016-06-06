module Control.Concurrent.ThreadPoolSpec (spec) where

import Control.Concurrent.ThreadPool
import Data.IORef
import Test.Hspec

spec :: Spec
spec = do
    it "mapTP" $ withThreadPool 4 $ \tp -> do
        res <- mapTP tp (\x -> return $ x + 1) ([1..10] :: [Int])
        res `shouldBe` [2..11]
    it "mapTP_" $ withThreadPool 4 $ \tp -> do
        ref <- newIORef 0
        let go i = atomicModifyIORef' ref $ \x -> (x + i, ())
        mapTP_ tp go ([1..10] :: [Int])
        res <- readIORef ref
        res `shouldBe` sum [1..10]