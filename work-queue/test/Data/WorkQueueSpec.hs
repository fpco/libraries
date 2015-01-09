{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Data.WorkQueueSpec (spec) where

import           Control.Concurrent
import           Control.Exception (try, SomeException)
import           Control.Monad (forM_)
import           Control.Monad.STM (atomically)
import           Data.IORef
import           Data.List (sort)
import qualified Data.Vector.Unboxed.Mutable as V
import           Data.WorkQueue
import           System.Random (randomRIO)
import           System.Timeout (timeout)
import           Test.Hspec

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
                    (i, \() -> do
                        threadDelay 100000
                        atomicModifyIORef' ref $ \t -> (t + 1, ()))
        cnt <- readIORef ref
        cnt `shouldBe` count
        forM_ [0..count - 1] $ \i -> do
            x <- V.read v i
            x `shouldBe` i
    it "mapQueue" $ do
        withWorkQueue $ \queue ->
            withLocalSlaves queue 2 return $ do
                let xs = [1..100]
                results <- mapQueue queue xs
                results `shouldBe` xs
    it "mapQueue_" $ do
        ref <- newIORef []
        -- By throwing in a random delay, the results get appended to
        -- the list nondeterministically.  This forces sorting the
        -- results.  While not strictly necessary, it's good to do
        -- this as without the delay the results end up accumulating
        -- in order (but not necessarily always!).
        let calc x = do
                randomTinyDelay
                atomicModifyIORef ref (\xs -> (x:xs, ()))
            xs = [1..100]
        withWorkQueue $ \queue -> withLocalSlaves queue 2 calc $ do
            mapQueue_ queue xs
            results <- readIORef ref
            sort results `shouldBe` xs
    it "withLocalSlave" $ do
        -- Since there is only one slave, these actions will be run
        -- sequentially.
        ref <- newIORef []
        let calc x = randomTinyDelay >> modifyIORef ref (x:)
            xs = [1..100]
        results <- withWorkQueue $ \queue -> withLocalSlave queue calc $ do
            mapQueue_ queue xs
            readIORef ref
        results `shouldBe` xs
    it "provideWorker unblocks on close" $ do
        done <- newEmptyMVar
        withWorkQueue $ \queue -> do
            _ <- forkIO $ do
                provideWorker queue (\() -> return ())
                putMVar done ()
            mapQueue_ queue [()]
        result <- timeout (20 * 1000) $ takeMVar done
        result `shouldBe` Just ()
    it "no processing with 0 slaves" $ do
        result <- timeout (20 * 1000) $
            withWorkQueue $ \queue ->
                withLocalSlaves queue 0 (\() -> return ()) $
                    mapQueue_ queue [()]
        result `shouldBe` Nothing
    it "exceptions halt all slaves" $ do
        ref <- newIORef 0
        let calc True = fail "boom!"
            calc False = atomicModifyIORef ref (\x -> (x + 1, ()))
        (result :: Either SomeException ()) <- try $
            withWorkQueue $ \queue ->
                withLocalSlaves queue 2 calc $ do
                    -- The two initial 'True' values are provided to
                    -- the two workers.  Since this causes exceptions
                    -- to be thrown, it should be impossible for any
                    -- worker to be provided a 'False' value.
                    atomically $ queueItems queue $ map (, \() -> return ()) [True, True, False, False]
                    atomically $ checkEmptyWorkQueue queue
        show result `shouldBe` "Left user error (boom!)"
        incCount <- readIORef ref
        incCount `shouldBe` 0

randomTinyDelay :: IO ()
randomTinyDelay = threadDelay =<< randomRIO (0, 2000)
