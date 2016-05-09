{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Distributed.HeartbeatSpec (spec) where

import           ClassyPrelude hiding (keys)
import           Test.Hspec (Spec, SpecWith, it, runIO, hspec)
import           FP.Redis (Seconds(..))
import           Data.Time.Clock.POSIX
import qualified Data.HashMap.Strict as HMS
import           Control.Concurrent.Async.Lifted.Safe (concurrently)
import           FP.Redis (MonadConnect, del, keys)
import           FP.ThreadFileLogger
import qualified Data.List.NonEmpty as NE
import           Control.Monad.Trans.Control (MonadBaseControl)
import qualified Control.Concurrent.STM as STM

import           Distributed.Heartbeat
import           Distributed.Types
import           Distributed.Redis
import           TestUtils

-- Bookkeeping
-----------------------------------------------------------------------

data WorkerStatus = WorkerStatus
    { wsFinished :: !(MVar POSIXTime)
    , wsCollectedFailures :: !(IORef Int)
    }

type Workers =
    IORef (HMS.HashMap WorkerId WorkerStatus)

newWorkers :: (MonadIO m, MonadBaseControl IO m) => [WorkerId] -> m Workers
newWorkers ids = do
    hms <- fmap HMS.fromList $ forM ids $ \id_ -> do
        ws <- WorkerStatus <$> newEmptyMVar <*> newIORef 0
        return (id_, ws)
    newIORef hms

markFinished :: (MonadIO m, MonadBaseControl IO m) => Workers -> WorkerId -> m ()
markFinished workersRef wid = do
    workers <- readIORef workersRef
    Just ws <- return (HMS.lookup wid workers)
    finishTime <- liftIO getPOSIXTime
    put <- tryPutMVar (wsFinished ws) finishTime
    unless put $
        fail ("Could not put finished time for worker " ++ show wid)

bumpCollectedFailures :: (MonadIO m, MonadBaseControl IO m) => Workers -> WorkerId -> m ()
bumpCollectedFailures workersRef wid = do
    workers <- readIORef workersRef
    Just ws <- return (HMS.lookup wid workers)
    atomicModifyIORef' (wsCollectedFailures ws) (\c -> (c + 1, ()))

-- Configs/init
-----------------------------------------------------------------------

heartbeatConfig :: HeartbeatConfig
heartbeatConfig = HeartbeatConfig
    { hcSenderIvl = Seconds 2
    , hcCheckerIvl = Seconds 1
    }

withHeartbeats_ :: (MonadConnect m) => Redis -> WorkerId -> m a -> m a
withHeartbeats_ = withHeartbeats heartbeatConfig

checkHeartbeats_ :: (MonadConnect m) => Redis -> ([WorkerId] -> m () -> m ()) -> m void
checkHeartbeats_ = checkHeartbeats heartbeatConfig

withCheckHeartbeats_ :: (MonadConnect m) => Redis -> ([WorkerId] -> m () -> m ()) -> m a -> m a
withCheckHeartbeats_ = withCheckHeartbeats heartbeatConfig

-- Utilities
-----------------------------------------------------------------------

expectWorkers :: (Monad m) => [WorkerId] -> [WorkerId] -> m ()
expectWorkers expected got = do
    unless (expected == got) $
        fail ("Expected workers " ++ show (map unWorkerId expected) ++ ", got " ++ show (map unWorkerId got))

checkNoWorkers :: (MonadConnect m) => Redis -> m ()
checkNoWorkers r = do
    wids <- activeOrUnhandledWorkers r
    unless (null wids) $
        fail ("Expected no workers, but got " ++ show (map unWorkerId wids))

-- Tests
-----------------------------------------------------------------------

spec :: Spec
spec = do
    redisIt "Detects dead worker" $ \redis -> do
        let wid = WorkerId "0"
        workers <- newWorkers [wid]
        stopChecking :: MVar () <- newEmptyMVar
        void $ concurrently
            (withHeartbeats_ redis wid (markFinished workers wid))
            (withCheckHeartbeats_ redis
                (\wids markHandled -> do
                    expectWorkers [wid] wids
                    bumpCollectedFailures workers wid
                    markHandled
                    putMVar stopChecking ())
                (takeMVar stopChecking))
        checkNoWorkers redis
    redisIt "Keeps detecting dead workers if they're not handled" $ \redis -> do
        let wid = WorkerId "0"
        workers <- newWorkers [wid]
        handleCalls :: TVar Int <- liftIO (newTVarIO 0)
        void $ concurrently
            (withHeartbeats_ redis wid (markFinished workers wid))
            (withCheckHeartbeats_ redis
                (\wids markHandled -> do
                    expectWorkers [wid] wids
                    bumpCollectedFailures workers wid
                    calls <- atomically $ do
                        modifyTVar handleCalls (+1)
                        readTVar handleCalls
                    when (calls == 3) markHandled)
                (atomically $ do
                    calls <- readTVar handleCalls
                    unless (calls == 3) STM.retry))
        checkNoWorkers redis
