{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE MultiWayIf #-}
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

import           Distributed.Redis
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

masterLog :: MasterId -> Text -> Text
masterLog (MasterId mid) msg = "(M" ++ tshow mid ++ ") " ++ msg

slaveLog :: SlaveId -> Text -> Text
slaveLog (SlaveId mid) msg = "(S" ++ tshow mid ++ ") " ++ msg

runMaster :: forall m a.
       (MonadConnect m)
    => Redis -> WorkerId -> NMApp MasterSends SlaveSends m () -> m a -> m a
runMaster r wid =
    acceptSlaveConnections r wid (CN.serverSettings 0 "*") "127.0.0.1" Nothing

runSlave :: forall m void.
       (MonadConnect m)
    => Redis -> NMApp SlaveSends MasterSends m () -> m void
runSlave r cont = connectToMaster r (Milliseconds 100) cont

runMasterCollectResults :: (MonadConnect m) => Redis -> WorkerId -> MasterId -> Int -> m ()
runMasterCollectResults r wid mid numSlaves = do
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
    results <- runMaster r wid whenSlaveConnects master
    unless (results == Map.fromList [(SlaveId x, mid) | x <- [1..numSlaves]]) $
        fail "Unexpected results"

runEchoSlave :: (MonadConnect m) => Redis -> SlaveId -> Int -> m void
runEchoSlave r slaveId delay = do
    let slave nm = do
            MasterSends n <- nmRead nm
            liftIO (threadDelay (delay * 1000))
            $logInfo (slaveLog slaveId ("Echoing to " ++ tshow n))
            nmWrite nm (SlaveSends slaveId n)
            $logInfo (slaveLog slaveId ("Slave done, quitting"))
    runSlave r slave

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
        result <- fmap (either id absurd) $ Async.race
            (runMaster r whenSlaveConnects master) (runEchoSlave r (SlaveId 0) 0)
        unless (result == SlaveSends (SlaveId 0) mid) $
            fail ("Expecting 42 in SlaveSends, got " ++ show result)
    redisIt "Echo with many slaves" $ \r -> do
        let numSlaves = 10
        fmap (either (absurd . NE.head) id) $ Async.race
            (Async.mapConcurrently (\x -> runEchoSlave r (SlaveId x) 0) (NE.fromList [1..numSlaves]))
            (runMasterCollectResults r (MasterId 0) numSlaves)
    redisIt "Echo with many masters and many slaves (short)" $ \r -> do
        let numSlaves :: Int = 10
        let numMasters :: Int = 5
        fmap (either (absurd . NE.head) id) $ Async.race
            (Async.mapConcurrently (\x -> runEchoSlave r (SlaveId x) 0) (NE.fromList [1..numSlaves]))
            (mapConcurrently_ (\mid -> runMasterCollectResults r (MasterId mid) numSlaves) [1..numMasters])
    stressfulTest $ redisIt "Echo with many masters and many slaves (long)" $ \r -> do
        let numSlaves :: Int = 10
        let numMasters :: Int = 5
        let killRandomly_ = killRandomly KillRandomly
                { krMaxPause = 100
                , krRetries = 20
                , krMaxTimeout = 1000
                }
        fmap (either (absurd . NE.head) id) $ Async.race
            (Async.mapConcurrently (\x -> killRandomly_ (runEchoSlave r (SlaveId x) 500)) (NE.fromList [1..numSlaves]))
            (mapConcurrently_ (\mid -> killRandomly_ (runMasterCollectResults r (MasterId mid) numSlaves)) [1..numMasters])
