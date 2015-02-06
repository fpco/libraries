{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE OverloadedStrings #-}

import Control.Concurrent.Async (async, waitCatch)
import Control.Exception (Exception, fromException, throwIO)
import Control.Monad (void)
import Control.Monad.IO.Class
import Control.Monad.Logger (runStdoutLoggingT)
import Data.List.NonEmpty (NonEmpty((:|)))
import Data.Typeable (Typeable)
import FP.Redis
import Test.Hspec (Spec, it, hspec)

main :: IO ()
main = hspec spec

spec :: Spec
spec = do
    it "throws exceptions from within withSubscriptionsEx" $ do
        let sub = subscribe ("test-channel" :| [])
        subThread <- async $ void $ runStdoutLoggingT $
            withSubscriptionsEx localhost (sub :| []) $ \_msg ->
                liftIO $ throwIO RedisTestException
        runStdoutLoggingT $ withConnection localhost $ \redis -> do
            runCommand_ redis $ publish "test-channel" "message"
        eres <- waitCatch subThread
        case eres of
            Left (fromException -> Just RedisTestException) -> return ()
            _ -> fail $ "Expected RedisTestException. Instead got " ++ show eres

localhost :: ConnectInfo
localhost = connectInfo "localhost"

data RedisTestException = RedisTestException
    deriving (Show, Typeable)

instance Exception RedisTestException
