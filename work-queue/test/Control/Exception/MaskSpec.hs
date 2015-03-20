{-# OPTIONS_GHC -fno-warn-name-shadowing #-}

module Control.Exception.MaskSpec (spec) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async
import Control.Concurrent.STM
import Control.Exception.Mask
import Test.Hspec

spec :: Spec
spec = do
    -- Demonstrates the need for this.
    it "STM can be masked" $ do
        ready <- newTVarIO False
        var <- newTVarIO False
        thread <- async $ uninterruptibleMask $ \_ -> do
            atomically $ writeTVar ready True
            atomically $ check =<< readTVar var
        -- Wait for the thread to enter the masked block.
        atomically $ check =<< readTVar ready
        -- Check that it is indeed blocked.
        threadDelay (10 * 1000)
        result <- poll thread
        show result `shouldBe` "Nothing"
        -- This fails to cancel the thread, because it's masked.
        _ <- async $ cancel thread
        threadDelay (10 * 1000)
        result <- poll thread
        show result `shouldBe` "Nothing"
        -- Setting the var to True causes it to exit.
        atomically $ writeTVar var True
        threadDelay (10 * 1000)
        result <- poll thread
        show result `shouldBe` "Just (Left thread killed)"
    -- Demonstrates the need for this.
    it "STM can now be unmasked inside an 'atomically' block!" $ do
        ready <- newTVarIO False
        var <- newTVarIO False
        thread <- async $ uninterruptibleMask $ \ra -> do
            atomically $ writeTVar ready True
            atomically $ restoreSTM ra $ check =<< readTVar var
        -- Wait for the thread to enter the masked block.
        atomically $ check =<< readTVar ready
        -- Check that it is indeed blocked.
        threadDelay (10 * 1000)
        result <- poll thread
        show result `shouldBe` "Nothing"
        -- This succeeds cancelling the thread, because it's unmasked
        -- within the transaction.
        threadDelay (10 * 1000)
        _ <- async $ cancel thread
        threadDelay (10 * 1000)
        result <- poll thread
        show result `shouldBe` "Just (Left thread killed)"
        -- Reference to var, so that STM isn't blocked indefinitely.
        atomically $ writeTVar var True
