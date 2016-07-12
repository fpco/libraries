{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}

import ClassyPrelude
import qualified Control.Concurrent.Mesosync.Lifted.Safe as Async
import Control.Concurrent.Lifted (threadDelay, fork, killThread)
import Control.Concurrent.STM (check)
import Control.Exception (throw)
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
    return ()
    {-
    -- Ideally, this could be done in a cleaner fashion.  See #34
    it "can cancel subscriptions" $ do
        let chan = "test-chan-1"
        connRef <- newIORef (error "connRef uninitialized")
        messageSeenRef <- newIORef False
        (ready, thread) <- asyncSubscribe chan $ \conn _ _ -> do
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
        -- Kill subscriber
        cancel thread
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
    it "can interrupt subscriptions" $ do
        let chan = "test-chan-3"
            sub = subscribe (chan :| [])
        messageSeenRef <- newIORef False
        timeoutSucceededRef <- newIORef False
        tid <- fork $
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
        killThread tid
    it "throws DisconnectedException when running a command on a disconnected connection" $ do
        withRedis $ \r -> do
            let cmd = set "test-key" "test" []
            runCommand_ r cmd
            disconnect r
            runCommand_ r cmd `shouldThrowLifted` \ex ->
                case ex of
                    DisconnectedException -> True
                    _ -> False
    it "publish succeeds in a timely manner" $ do
        let chan = "test-chan-2"
            msg = "test message"
            cnt = 10 :: Int
        countRef <- newIORef 0
        -- Fork some listeners
        listeners <- forM [1..cnt] $ \_ -> asyncSubscribe chan $ \_ _ msg' -> do
            liftIO $ msg' `shouldBe` msg
            atomicModifyIORef' countRef ((,()) . (+1))
        -- Wait for them to be ready
        mapM_ (\(ready, _) -> atomically $ check =<< readTVar ready) listeners
        -- Publish some messages
        forM_ [1..cnt] $ \_ -> do
          mres <- timeout (1000 * 100) $ withRedis $ \r ->
              runCommand_ r $ publish chan msg
          when (isNothing mres) $ fail "Publish took too long"
        -- Wait a little bit
        threadDelay (1000 * 100)
        -- Check the listeners for exceptions
        mapM_ (mapM_ (either throwIO (either throwIO (\_ -> return ()))) <=< poll . snd) listeners
        -- Expect that every listener got the message
        count <- readIORef countRef
        count `shouldBe` (cnt * cnt)
        -- Kill all the listeners
        mapM_ (cancel . snd) listeners
    it "evaluates exceptions when enqueing" $ do
        (eres :: Either RedisTestException (IO Bool)) <-
            try $ withRedis $ \redis ->
                sendCommand redis (set "testkey" (throw RedisTestException) [])
        case eres of
            Left RedisTestException -> return ()
            _ -> fail $ "Expected RedisTestException"
    -}

{-
asyncSubscribe :: Channel
               -> (SubscriptionConnection -> Channel -> ByteString -> LoggingT IO ())
               -> IO (TVar Bool, Async (Either SomeException ()))
asyncSubscribe chan f = do
    ready <- newTVarIO False
    thread <- async $ tryAny $ void $ runStdoutLoggingT $
        withSubscriptionsExConn localhost (subscribe (chan :| []) :| []) $
            \conn -> return $ \msg ->
                case msg of
                    Subscribe {} -> liftIO $ atomically $ writeTVar ready True
                    Unsubscribe {} -> return ()
                    Message k x -> f conn k x
    return (ready, thread)
-}

redisIt :: String -> IO () -> Spec
redisIt name f = it name (clearRedis >> f)

clearRedis :: IO ()
clearRedis = withRedis (\conn -> runCommand conn flushall)

withRedis :: (Connection -> LoggingT IO a) -> IO a
withRedis = runStdoutLoggingT . withConnection localhost

localhost :: ConnectInfo
localhost = connectInfo "localhost"

{-
shouldThrowLifted :: (MonadBaseControl IO m, Exception e) => m () -> (e -> Bool) -> m ()
shouldThrowLifted f s = liftBaseWith $ \restore -> restore f `shouldThrow` s
-}

data RedisTestException = RedisTestException
    deriving (Show, Typeable)

instance Exception RedisTestException
