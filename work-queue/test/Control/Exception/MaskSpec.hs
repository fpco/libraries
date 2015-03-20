{-# OPTIONS_GHC -fno-warn-name-shadowing #-}

module Control.Exception.MaskSpec (spec) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async
import Control.Concurrent.STM
import Control.Exception (mask, uninterruptibleMask)
-- import Control.Exception.Mask
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
        threadDelay (10 * 1000)
        cancel thread
        threadDelay (10 * 1000)
        result <- poll thread
        -- Oddly enough, this is actually
        -- Just (Left thread blocked indefinitely in an STM transaction)
        show result `shouldBe` "Nothing"
        -- Setting the var to True causes it to exit.
        atomically $ writeTVar var True
        threadDelay (10 * 1000)
        result <- poll thread
        show result `shouldBe` "Just (Right ())"
