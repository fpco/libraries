{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE FlexibleContexts #-}

import ClassyPrelude
import Control.Concurrent.Async (Async, async, waitCatch)
import Control.Concurrent.Lifted (threadDelay, fork)
import Control.Concurrent.STM (check)
import Control.Monad.Logger (runStdoutLoggingT, LoggingT)
import Data.List.NonEmpty (NonEmpty((:|)))
import FP.Redis
import System.Timeout (timeout)
import Test.Hspec (Spec, it, hspec, shouldBe, shouldThrow)
import Control.Monad.Trans.Control (MonadBaseControl, liftBaseWith)

main :: IO ()
main = hspec spec

spec :: Spec
spec = do
    -- Ideally, this could be done in a cleaner fashion.  See #34
    it "can cancel subscriptions" $ do
        let chan = "test-chan-1"
        connRef <- newIORef (error "connRef uninitialized")
        messageSeenRef <- newIORef False
        (ready, _) <- asyncSubscribe chan $ \conn _ _ -> do
            writeIORef connRef conn
            writeIORef messageSeenRef True
        -- Check that messages are received once we've subscribed.
        -- This also causes connRef to be initialized.
        atomically $ check =<< readTVar ready
        withRedis $ \redis -> runCommand_ redis $ publish chan "message"
        liftIO $ threadDelay (1000 * 100)
        wasSeen <- readIORef messageSeenRef
        wasSeen `shouldBe` True
        disconnectSub =<< readIORef connRef
        -- Check that messages aren't received once we've disconnected.
        writeIORef messageSeenRef False
        withRedis $ \redis -> runCommand_ redis $ publish chan "message"
        liftIO $ threadDelay (1000 * 100)
        wasSeen' <- readIORef messageSeenRef
        wasSeen' `shouldBe` False
    it "throws exceptions from within withSubscriptionsEx" $ do
        let chan = "test-chan-2"
        (ready, thread) <- asyncSubscribe chan $ \_ _ _ ->
            throwM RedisTestException
        atomically $ check =<< readTVar ready
        withRedis $ \redis -> runCommand_ redis $ publish chan "message"
        eres <- waitCatch thread
        case eres of
            Right (Left (fromException -> Just RedisTestException)) -> return ()
            _ -> fail $ "Expected RedisTestException. Instead got " ++ show eres
    -- http://code.google.com/p/redis/issues/detail?id=199
    skip $ it "should block when there is no data available" $ do
        res <- timeout (1000 * 1000) $ withRedis $ \redis ->
                   runCommand redis $ brpop ("foobar_list" :| []) (Seconds 0)
        res `shouldBe` Nothing
    it "should return data when it's available" $ do
        withRedis $ \redis -> runCommand_ redis $ lpush "foobar_list" ("my data" :| [])
        res <- timeout (1000 * 1000) $ withRedis $ \redis ->
            runCommand redis $ brpop ("foobar_list" :| []) (Seconds 0)
        res `shouldBe` (Just (Just ("foobar_list", "my data")))
    skip $ it "can interrupt subscriptions" $ do
        let chan = "test-chan-3"
            sub = subscribe (chan :| [])
        messageSeenRef <- newIORef False
        timeoutSucceededRef <- newIORef False
        _ <- fork $
            (void $ timeout (1000 * 10) $ runStdoutLoggingT $ withSubscriptionsWrapped localhost (sub :| []) $ \msg ->
                case msg of
                    Message {} -> liftIO $ writeIORef messageSeenRef True
                    _ -> return ()
            ) `finally` writeIORef timeoutSucceededRef True
        threadDelay (1000 * 50)
        withRedis $ \r -> runCommand_ r $ publish chan ""
        threadDelay (1000 * 50)
        messageSeen <- readIORef messageSeenRef
        messageSeen `shouldBe` False
        timeoutSucceded <- readIORef timeoutSucceededRef
        timeoutSucceded `shouldBe` True
    it "throws DisconnectedException when running a command on a disconnected connection" $ do
        withRedis $ \r -> do
            let cmd = set "test-key" "test" []
            runCommand_ r cmd
            disconnect r
            runCommand_ r cmd `shouldThrowLifted` \ex ->
                case ex of
                    DisconnectedException -> True
                    _ -> False

skip :: Monad m => m () -> m ()
skip _ = return ()

asyncSubscribe :: Channel
               -> (SubscriptionConnection -> Channel -> ByteString -> LoggingT IO ())
               -> IO (TVar Bool, Async (Either SomeException ()))
asyncSubscribe chan f = do
    ready <- newTVarIO False
    thread <- async $ tryAny $ void $ runStdoutLoggingT $
        withSubscriptionsExConn localhost (subscribe (chan :| []) :| []) $
            \conn -> return $ trackSubscriptionStatus ready (f conn)
    return (ready, thread)

withRedis :: (Connection -> LoggingT IO a) -> IO a
withRedis = runStdoutLoggingT . withConnection localhost

localhost :: ConnectInfo
localhost = connectInfo "localhost"

shouldThrowLifted :: (MonadBaseControl IO m, Exception e) => m () -> (e -> Bool) -> m ()
shouldThrowLifted f s = liftBaseWith $ \restore -> restore f `shouldThrow` s

data RedisTestException = RedisTestException
    deriving (Show, Typeable)

instance Exception RedisTestException
