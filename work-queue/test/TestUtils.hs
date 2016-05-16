{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE RecordWildCards #-}
module TestUtils
    ( testRedisConfig
    , redisIt
    , redisIt_
    , loggingIt
    , loggingProperty
    , randomThreadDelay
    , KillRandomly(..)
    , killRandomly
    ) where

import           ClassyPrelude hiding (keys)
import           Test.Hspec (Spec, it)
import           FP.Redis
import qualified Data.List.NonEmpty as NE
import           Control.Monad.Logger
import qualified Data.Text as T
import qualified Control.Concurrent.Async.Lifted.Safe as Async
import           System.Random (randomRIO)
import           Control.Concurrent (threadDelay)
import qualified Test.QuickCheck as QC

import           Distributed.Redis

testRedisConfig :: RedisConfig
testRedisConfig = defaultRedisConfig
    { rcKeyPrefix = "test:" }

clearRedisKeys :: (MonadConnect m) => Redis -> m ()
clearRedisKeys redis = do
    matches <- run redis (keys "test:*")
    mapM_ (run_ redis . del) (NE.nonEmpty matches)

minimumLogLevel ll = ll >= LevelError

loggingIt :: String -> (forall m. (MonadConnect m) => m ()) -> Spec
loggingIt msg cont = it msg x
  where
    x :: IO ()
    x = runStdoutLoggingT $ filterLogger (\_ -> minimumLogLevel) $ do
        $logInfo (T.pack msg)
        cont

loggingProperty :: forall prop.
       (QC.Testable prop)
    => (LoggingT IO prop) -> QC.Property
loggingProperty m = QC.ioProperty
    (runStdoutLoggingT (filterLogger (\_ -> minimumLogLevel) m) :: IO prop)

redisIt_ :: String -> (forall m. (MonadConnect m) => m ()) -> Spec
redisIt_ msg cont = redisIt msg (\_r -> cont)

redisIt :: String -> (forall m. (MonadConnect m) => Redis -> m ()) -> Spec
redisIt msg cont = loggingIt msg $ do
    withRedis testRedisConfig (\redis -> clearRedisKeys redis >> cont redis)

data KillRandomly = KillRandomly
    { krMaxPause :: !Int -- ^ Max pause between executions, in milliseconds
    , krRetries :: !Int
    , krMaxTimeout :: !Int -- ^ Max timout, in milliseconds
    }

randomThreadDelay :: (MonadIO m) => Int -> m ()
randomThreadDelay maxN = liftIO $ do
    n <- randomRIO (0, maxN)
    threadDelay n

killRandomly :: (MonadConnect m) => KillRandomly -> m a -> m a
killRandomly KillRandomly{..} action = if krRetries < 1
    then liftIO (fail "killRandomly: krRetries < 1")
    else go krRetries
  where
    go n = if n == 0
        then action
        else do
            randomThreadDelay (krMaxPause * 1000)
            mbRes <- Async.race
                (do randomThreadDelay (krMaxTimeout * 1000)
                    $logInfo "Killing action")
                action
            case mbRes of
                Left () -> go (n - 1)
                Right x -> return x

