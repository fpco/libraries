{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE DataKinds #-}
module Distributed.RequestSlavesSpec (spec) where

import           ClassyPrelude
import           Test.Hspec
import           Data.Store.TypeHash
import           FP.Redis
import           Data.Store (Store)
import qualified Control.Concurrent.Async.Lifted.Safe as Async
import           Data.Void (absurd)
import qualified Data.Map.Strict as Map
import qualified Control.Concurrent.STM as STM
import qualified Data.List.NonEmpty as NE
import           Control.Concurrent (threadDelay)
import           Control.Monad.Logger
import qualified Data.Conduit.Network as CN
import qualified Data.ByteString.Char8 as BSC8

import           Distributed.Redis
import           Distributed.Types
import           Data.Streaming.NetworkMessage
import           Distributed.RequestSlaves

import           TestUtils

-- * Utils
-----------------------------------------------------------------------

newtype MasterId = MasterId {_unMasterId :: Int}
    deriving (Eq, Ord, Typeable, Store)

instance Show MasterId where
    show (MasterId mid) = "S" ++ show mid

newtype MasterSends = MasterSends MasterId
    deriving (Eq, Show, Typeable, Store)

newtype SlaveId = SlaveId {_unSlaveId :: Int}
    deriving (Eq, Ord, Typeable, Store)

instance Show SlaveId where
    show (SlaveId sid) = "S" ++ show sid

data SlaveSends = SlaveSends
    { _slaveId :: !SlaveId
    , _slaveMasterId :: !MasterId
    } deriving (Eq, Show, Typeable, Generic)
instance Store SlaveSends

mkManyHasTypeHash [[t|MasterSends|], [t|SlaveSends|]]

data WorkerIds = WorkerIds
    { wisCount :: !Int
    , wisWorkerIds :: ![WorkerId]
    }

newWorkerIdsVar :: (MonadConnect m) => m (MVar WorkerIds)
newWorkerIdsVar = newMVar WorkerIds{wisCount = 0, wisWorkerIds = []}

masterLog :: MasterId -> Text -> Text
masterLog (MasterId mid) msg = "(M" ++ tshow mid ++ ") " ++ msg

slaveLog :: SlaveId -> Text -> Text
slaveLog (SlaveId mid) msg = "(S" ++ tshow mid ++ ") " ++ msg

runMaster :: forall m a.
       (MonadConnect m)
    => Redis -> MVar WorkerIds -> NMApp MasterSends SlaveSends m () -> m a -> m a
runMaster r wids nm action = do
    wid <- modifyMVar wids $ \WorkerIds{..} -> do
        let wid = WorkerId (BSC8.pack (show wisCount))
        return (WorkerIds{wisCount = wisCount + 1, wisWorkerIds = wid : wisWorkerIds}, wid)
    acceptSlaveConnections r wid (CN.serverSettings 0 "*") "127.0.0.1" Nothing nm action

runSlave :: forall m void.
       (MonadConnect m)
    => Redis -> MVar WorkerIds -> NMApp SlaveSends MasterSends m () -> m void
runSlave r wids cont = connectToMaster r (Milliseconds 100) (wisWorkerIds <$> readMVar wids) cont

runMasterCollectResults :: (MonadConnect m) => Redis -> MVar WorkerIds -> MasterId -> Int -> m ()
runMasterCollectResults r wids mid numSlaves = do
    resultsVar :: TVar (Map.Map SlaveId MasterId) <- liftIO (newTVarIO mempty)
    let whenSlaveConnects nm = do
            nmWrite nm (MasterSends mid)
            SlaveSends slaveN n <- nmRead nm
            $logInfo (masterLog mid ("Got echo from " ++ tshow slaveN))
            atomically (modifyTVar resultsVar (Map.insert slaveN n))
    let master = do
        $logInfo (masterLog mid "Waiting for all slaves to be done")
        res <- atomically $ do
            results <- readTVar resultsVar
            unless (Map.size results == numSlaves) STM.retry
            return results
        $logInfo (masterLog mid "Slaves done")
        return res
    results <- runMaster r wids whenSlaveConnects master
    unless (results == Map.fromList [(SlaveId x, mid) | x <- [1..numSlaves]]) $
        fail "Unexpected results"

runEchoSlave :: (MonadConnect m) => Redis -> MVar WorkerIds -> SlaveId -> Int -> m void
runEchoSlave r wids slaveId delay = do
    let slave nm = do
            MasterSends n <- nmRead nm
            liftIO (threadDelay (delay * 1000))
            $logInfo (slaveLog slaveId ("Echoing to " ++ tshow n))
            nmWrite nm (SlaveSends slaveId n)
            $logInfo (slaveLog slaveId ("Slave done, quitting"))
    runSlave r wids slave

mapConcurrently_ :: (MonadConnect m, Traversable t) => (a -> m ()) -> t a -> m ()
mapConcurrently_ f x = void (Async.mapConcurrently f x)

-- * Spec
-----------------------------------------------------------------------

spec :: Spec
spec = do
    redisIt "Simple echo" $ \r -> do
        slaveDataOnMaster :: MVar SlaveSends <- newEmptyMVar
        let mid = MasterId 42
        let whenSlaveConnects nm = do
                nmWrite nm (MasterSends mid)
                putMVar slaveDataOnMaster =<< nmRead nm
        let master = takeMVar slaveDataOnMaster
        wids <- newWorkerIdsVar
        result <- fmap (either id absurd) $ Async.race
            (runMaster r wids whenSlaveConnects master) (runEchoSlave r wids (SlaveId 0) 0)
        unless (result == SlaveSends (SlaveId 0) mid) $
            fail ("Expecting 42 in SlaveSends, got " ++ show result)
    redisIt "Echo with many slaves" $ \r -> do
        let numSlaves = 10
        wids <- newWorkerIdsVar
        fmap (either (absurd . NE.head) id) $ Async.race
            (Async.mapConcurrently (\x -> runEchoSlave r wids (SlaveId x) 0) (NE.fromList [1..numSlaves]))
            (runMasterCollectResults r wids (MasterId 0) numSlaves)
    redisIt "Echo with many masters and many slaves (short)" $ \r -> do
        let numSlaves :: Int = 10
        let numMasters :: Int = 5
        wids <- newWorkerIdsVar
        fmap (either (absurd . NE.head) id) $ Async.race
            (Async.mapConcurrently (\x -> runEchoSlave r wids (SlaveId x) 0) (NE.fromList [1..numSlaves]))
            (mapConcurrently_ (\mid -> runMasterCollectResults r wids (MasterId mid) numSlaves) [1..numMasters])
    stressfulTest $ redisIt "Echo with many masters and many slaves (long)" $ \r -> do
        let numSlaves :: Int = 10
        let numMasters :: Int = 5
        let killRandomly_ = killRandomly KillRandomly
                { krMaxPause = 100
                , krRetries = 20
                , krMaxTimeout = 1000
                }
        wids <- newWorkerIdsVar
        fmap (either (absurd . NE.head) id) $ Async.race
            (Async.mapConcurrently (\x -> killRandomly_ (runEchoSlave r wids (SlaveId x) 500)) (NE.fromList [1..numSlaves]))
            (mapConcurrently_ (\mid -> killRandomly_ (runMasterCollectResults r wids (MasterId mid) numSlaves)) [1..numMasters])
    redisIt "Candidate slaves notice and remove inactive masters, while master is alive" $ \r -> do
        let whenSlaveConnects _nm = fail "Got an unexpected connection on master"
        hasMasterStarted :: MVar () <- newEmptyMVar
        let master = do
                putMVar hasMasterStarted ()
                -- Keep the master alive so that it doesn't remove itself from the list.
                liftIO $ forever (threadDelay maxBound)
        wids <- newWorkerIdsVar
        fmap (either absurd id) $ Async.race
            (runMaster r wids whenSlaveConnects master)
            (do -- Wait for the master to be alive
                takeMVar hasMasterStarted
                -- Remove the master from the list of worker ids, which will make it inactive
                -- as far as the slaves are concerned
                modifyMVar_ wids (\wids' -> return wids'{wisWorkerIds = []})
                fmap (either absurd id) $ Async.race
                    -- Start the slave
                    (runEchoSlave r wids (SlaveId 0) 0)
                    (do -- Short delay to make sure that the slave has a chance to see the connection
                        -- request and delete it
                        liftIO $ threadDelay (1 * 1000 * 1000)
                        reqs <- getWorkerRequests r
                        liftIO $ reqs `shouldBe` []))
