{-# OPTIONS_GHC -fno-warn-orphans #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE DataKinds #-}
module Distributed.StaleKeySpec (spec) where

import ClassyPrelude hiding (keys, (<>))
import Data.Void
import Control.Concurrent.Lifted (threadDelay)
import Control.Concurrent.Async.Lifted.Safe
import Control.Monad.Base
import Control.Monad.Catch (Handler (..))
import Control.Monad.Logger
import Control.Retry
import Data.Store.TypeHash (mkHasTypeHash)
import Data.Void
import Distributed.Heartbeat
import Distributed.JobQueue.Client
import Distributed.JobQueue.Status
import Distributed.JobQueue.Worker
import Distributed.Redis
import Distributed.Types
import FP.Redis (Seconds(..), MonadConnect)
import Test.HUnit.Lang (HUnitFailure (..))
import Test.Hspec (Spec)
import Test.Hspec.Expectations.Lifted
import TestUtils

$(mkHasTypeHash =<< [t| ByteString |])

type Request = ByteString
type Response = ByteString

-- Configs/init
-----------------------------------------------------------------------

heartbeatConfig :: HeartbeatConfig
heartbeatConfig = HeartbeatConfig
    { hcSenderIvl = Seconds 3600 -- We don't want the second heartbeat to arrive, ever.
    , hcCheckerIvl = Seconds 1
    }

jqc :: JobQueueConfig
jqc = testJobQueueConfig { jqcHeartbeatConfig = heartbeatConfig
                         , jqcCheckStaleKeysInterval = Seconds 2
                         }

workerFunc :: MonadConnect m => Redis -> RequestId -> Request -> m (Reenqueue Response)
workerFunc _ _ _ = do
    _ <- forever $ threadDelay maxBound
    return $ DontReenqueue "done"

myWorker :: MonadConnect m => m void
myWorker = jobWorker jqc workerFunc

requestId :: RequestId
requestId = RequestId "myRequest"

myClient :: MonadConnect m => m ()
myClient = withJobClient jqc $ \jq -> do
    let request = "some request" :: Request
    submitRequest jq requestId request
    mResponse <- waitForResponse_ jq requestId
    mResponse `shouldBe` Just ("done" :: Response)

waitFor :: forall m . (MonadIO m, MonadMask m) => RetryPolicy -> m () -> m ()
waitFor policy expectation =
    recovering policy [handler] expectation
  where
    handler :: Int -> Handler m Bool
    handler _ = Handler $ \(HUnitFailure _) -> return True

upToFiveSeconds :: RetryPolicy
upToFiveSeconds = constantDelay 100000 <> limitRetries 50

waitForHeartbeatFailure :: MonadConnect m => Redis -> m ()
waitForHeartbeatFailure redis = waitFor upToFiveSeconds $ do
    liveWorkers <- activeOrUnhandledWorkers redis
    liveWorkers `shouldBe` [] -- no active workers
    failedWorkers <- deadWorkers redis
    length failedWorkers `shouldBe` 1 -- 1 heartbeat failure

waitForJobStarted :: MonadConnect m => Redis -> m ()
waitForJobStarted redis = waitFor upToFiveSeconds $ do
    allRequests <- getAllRequests redis
    allRequests `shouldBe` [requestId]
    jqs <- getJobQueueStatus redis
    jqsPending jqs `shouldBe` [] -- there should be no job enqueued

waitForJobReenqueued :: MonadConnect m => Redis -> m ()
waitForJobReenqueued redis = waitFor upToFiveSeconds $ do
    allRequestStats <- getAllRequestStats redis
    map fst allRequestStats `shouldBe` [requestId]
    jqs <- getJobQueueStatus redis
    jqsPending jqs `shouldBe` [requestId] -- the job should be enqueued again

staleKeyTest :: MonadConnect m => Redis -> m ()
staleKeyTest redis =
    -- We'll need an explicit 'heartbeatChecker', since the client
    -- should pick a job _after_ failing its heartbeat.  If its
    -- heartbeat was checked by the client, it would already have
    -- taken the job.
    fmap (either id id) $ race
        (either absurd absurd <$> race myWorker
         (checkHeartbeats (jqcHeartbeatConfig jqc) redis $ \_inactive cleanup -> cleanup)
        ) $ do
            waitForHeartbeatFailure redis -- worker fails heartbeat, but is actually still alive.
            fmap (either id id) $ race myClient $ do
                -- we expect the worker to take the job
                waitForJobStarted redis
                -- and the client to subsequently re-enqueue it, since
                -- the worker is not in the list of active workers.
                waitForJobReenqueued redis

spec :: Spec
spec = redisIt "Re-enqueues jobs from dead workers that had already failed their heartbeat before working on the job." staleKeyTest
