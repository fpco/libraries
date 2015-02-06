{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE OverloadedStrings #-}

import Control.Concurrent.Async (async, waitCatch)
import Control.Exception (Exception, fromException, throwIO)
import Control.Monad (void)
import Control.Monad.IO.Class
import Control.Monad.Logger (runStdoutLoggingT, LoggingT)
import Data.List.NonEmpty (NonEmpty((:|)))
import Data.Typeable (Typeable)
import FP.Redis
import System.Timeout (timeout)
import Test.Hspec (Spec, it, hspec, shouldBe)

main :: IO ()
main = hspec spec

spec :: Spec
spec = do
    it "throws exceptions from within withSubscriptionsEx" $ do
        let sub = subscribe ("test-channel" :| [])
        subThread <- async $ void $ runStdoutLoggingT $
            withSubscriptionsEx localhost (sub :| []) $ \_msg ->
                liftIO $ throwIO RedisTestException
        withRedis $ \redis ->
            runCommand_ redis $ publish "test-channel" "message"
        eres <- waitCatch subThread
        case eres of
            Left (fromException -> Just RedisTestException) -> return ()
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

withRedis :: (Connection -> LoggingT IO a) -> IO a
withRedis = runStdoutLoggingT . withConnection localhost

localhost :: ConnectInfo
localhost = connectInfo "localhost"

data RedisTestException = RedisTestException
    deriving (Show, Typeable)

instance Exception RedisTestException
