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
    , testHeartbeatCheckIvl
    , testJobQueueConfig
    , redisIt
    , redisIt_
    , loggingIt
    , loggingProperty
    , randomThreadDelay
    , KillRandomly(..)
    , killRandomly
    , stressfulTest
    , waitFor
    , upToNSeconds
    , upToTenSeconds
    , raceAgainstVoids
    ) where

import           ClassyPrelude hiding (keys, (<>))
import           Control.Concurrent (threadDelay)
import qualified Control.Concurrent.Async.Lifted.Safe as Async
import           Control.Monad.Catch (Handler (..))
import           Control.Monad.Logger
import           Control.Retry
import           Data.Foldable (foldl)
import qualified Data.Text as T
import           Data.Void (absurd, Void)
import           Distributed.Heartbeat
import           Distributed.JobQueue.Worker
import           FP.Redis
import           System.Environment (lookupEnv)
import           System.Random (randomRIO)
import           Test.HUnit.Lang (HUnitFailure (..))
import           Test.Hspec (Spec, it)
import           Test.Hspec.Core.Spec (SpecM, runIO)
import qualified Test.QuickCheck as QC

import           Distributed.Redis

testRedisConfig :: RedisConfig
testRedisConfig = defaultRedisConfig "test:"

testHeartbeatCheckIvl :: Seconds
testHeartbeatCheckIvl = Seconds 2

testJobQueueConfig :: JobQueueConfig
testJobQueueConfig = (defaultJobQueueConfig "test:")
    { jqcRequestNotificationFailsafeTimeout = Milliseconds 1000
    , jqcSlaveRequestsNotificationFailsafeTimeout = Milliseconds 1000
    , jqcWaitForResponseNotificationFailsafeTimeout = Milliseconds 100
    , jqcCancelCheckIvl = Seconds 1
    , jqcHeartbeatConfig = HeartbeatConfig
        { hcSenderIvl = Seconds 1
        , hcCheckerIvl = testHeartbeatCheckIvl
        }
    }

clearRedisKeys :: (MonadConnect m) => Redis -> m ()
clearRedisKeys redis = do
    run redis flushall

minimumLogLevel :: LogLevel -> Bool
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

-- | Does not run the action if we have STRESSFUL=1 in the env
stressfulTest :: SpecM a () -> SpecM a ()
stressfulTest m = do
    mbS <- runIO (lookupEnv "STRESSFUL")
    case mbS of
        Just "1" -> m
        _ -> return ()

-- | This function allows us to make assertions that should become true, after some time.
--
-- If the assertion fails, it will retry, until it passes.  The
-- 'RetryPolicy' can be chosen to limit the number of retries.
--
-- This allows us, for example, to test that heartbeat failures are
-- detected, without waiting a fixed amount of time.
waitFor :: forall m a. (MonadIO m, MonadMask m) => RetryPolicy -> m a -> m a
waitFor policy expectation =
    recovering policy [handler] expectation
  where
    handler :: Int -> Handler m Bool
    handler _ = Handler $ \(HUnitFailure _) -> return True

-- | Wiat for up to @n@ seconds, in steps of 1/10th of a second.
upToNSeconds :: Int -> RetryPolicy
upToNSeconds n = constantDelay 100000 <> limitRetries (n * 10)

upToTenSeconds :: RetryPolicy
upToTenSeconds = upToNSeconds 10

-- | Perform an action concurrently with some non-terminating actions
-- that will be killed when the action finishes.
--
-- This can be used to spawn workers for a test, making sure they are
-- all killed at the end of the test.
raceAgainstVoids :: MonadConnect m
                    => m a
                    -- ^ Action to perform.
                    -> [m Void]
                    -- ^ Non-terminating actions that will run
                    -- concurrently to the first action, and will all
                    -- be killed when the first action terminates.
                    -> m a
raceAgainstVoids = foldl (\x v -> either id absurd <$> Async.race x v)
