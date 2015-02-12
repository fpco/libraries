{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE NoImplicitPrelude #-}

import ClassyPrelude
import Control.Concurrent.Async (Async, async, waitCatch)
import Control.Concurrent.Lifted (threadDelay, fork)
import Control.Concurrent.STM (check)
import Control.Monad.Logger (runStdoutLoggingT, LoggingT)
import Data.List.NonEmpty (NonEmpty((:|)))
import FP.Redis
import System.Timeout (timeout)
import Test.Hspec (Spec, it, hspec, shouldBe)

main :: IO ()
main = hspec spec

spec :: Spec
spec = do
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
    it "should block when there is no data available" $ do
        res <- timeout (1000 * 1000) $ withRedis $ \redis ->
            runCommand redis $ brpop ("foobar_list" :| []) (Seconds 0)
        res `shouldBe` Nothing
    it "should return data when it's available" $ do
        withRedis $ \redis -> runCommand_ redis $ lpush "foobar_list" ("my data" :| [])
        res <- timeout (1000 * 1000) $ withRedis $ \redis ->
            runCommand redis $ brpop ("foobar_list" :| []) (Seconds 0)
        res `shouldBe` (Just (Just ("foobar_list", "my data")))
    it "can interrupt subscriptions" $ do
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

asyncSubscribe :: Channel
               -> (Connection -> Channel -> ByteString -> LoggingT IO ())
               -> IO (TVar Bool, Async (Either SomeException ()))
asyncSubscribe chan f = do
    ready <- newTVarIO False
    thread <- async $ tryAny $ void $ runStdoutLoggingT $
        withSubscriptionsEx' localhost (subscribe (chan :| []) :| []) $
            \conn -> trackSubscriptionStatus ready (f conn)
    return (ready, thread)

withRedis :: (Connection -> LoggingT IO a) -> IO a
withRedis = runStdoutLoggingT . withConnection localhost

localhost :: ConnectInfo
localhost = connectInfo "localhost"

data RedisTestException = RedisTestException
    deriving (Show, Typeable)

instance Exception RedisTestException
