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
module Distributed.RequestSlavesSpec (spec) where

import           ClassyPrelude
import           Test.Hspec
import           Data.TypeFingerprint
import           FP.Redis
import           Data.Serialize (Serialize)
import qualified Data.Serialize as C
import qualified Data.UUID as UUID
import qualified Data.UUID.V4 as UUID
import qualified Control.Concurrent.Async.Lifted.Safe as Async
import           Data.Void (absurd)
import           GHC.Generics (Generic)
import qualified Data.Map.Strict as Map
import qualified Control.Concurrent.STM as STM
import qualified Data.List.NonEmpty as NE
import           Control.Concurrent (threadDelay)
import           Control.Monad.Logger
import qualified Data.Conduit.Network as CN
import qualified Data.Streaming.Network as CN
import qualified Data.Text as T
import           Control.Exception.Lifted (catches, Handler(..))

import           Distributed.Redis
import           Data.Streaming.NetworkMessage
import           Distributed.RequestSlaves
import           Distributed.Types

import           TestUtils

-- * Utils
-----------------------------------------------------------------------

newtype MasterId = MasterId {unMasterId :: Int}
    deriving (Eq, Ord, Typeable, Serialize)

instance Show MasterId where
    show (MasterId mid) = "S" ++ show mid

newtype MasterSends = MasterSends MasterId
    deriving (Eq, Show, Typeable, Serialize)

newtype SlaveId = SlaveId {unSlaveId :: Int}
    deriving (Eq, Ord, Typeable, Serialize)

instance Show SlaveId where
    show (SlaveId sid) = "S" ++ show sid

data SlaveSends = SlaveSends
    { slaveId :: !SlaveId
    , slaveMasterId :: !MasterId
    } deriving (Eq, Show, Typeable, Generic)
instance Serialize SlaveSends

mkManyHasTypeFingerprint [[t|MasterSends|], [t|SlaveSends|]]

getWorkerId :: (MonadIO m) => m WorkerId
getWorkerId = do
    liftIO (WorkerId . UUID.toASCIIBytes <$> UUID.nextRandom)

masterLog :: MasterId -> Text -> Text
masterLog (MasterId mid) msg = "(M" ++ tshow mid ++ ") " ++ msg

slaveLog :: SlaveId -> Text -> Text
slaveLog (SlaveId mid) msg = "(S" ++ tshow mid ++ ") " ++ msg

-- Since we run tests where things randomly die, we ignore the exceptions
-- simply to make the test output prettier
ignoreNetworkExceptions :: (MonadConnect m) => m () -> m ()
ignoreNetworkExceptions m = do
    catches m
        [ Handler $ \(err :: NetworkMessageException) -> do
            $logInfo ("ignoreNetworkExceptions: Got NetworkMessageException " ++ tshow err)
        , Handler $ \(err :: IOError) -> do
            $logInfo ("ignoreNetworkExceptions: Got IOError " ++ tshow err)
        ]

runMaster :: forall m a.
       (MonadConnect m)
    => Redis -> MasterId -> NMApp MasterSends SlaveSends m () -> m a -> m a
runMaster r mid contSlave cont = do
    nmSettings <- defaultNMSettings
    whenSlaveConnects :: MVar (m ()) <- newEmptyMVar
    let contSlave' nm = do
            $logInfo (masterLog mid "Taking slave connects action")
            join (readMVar whenSlaveConnects)
            $logInfo (masterLog mid "Got slave, continuing")
            contSlave nm
    acceptSlaveConnections nmSettings "127.0.0.1" contSlave' $ \wci -> do
        wid <- getWorkerId
        requestSlaves r wid wci $ \wsc -> do
            putMVar whenSlaveConnects wsc
            $logInfo (masterLog mid "Running master function")
            cont

runSlave :: forall m void.
       (MonadConnect m)
    => Redis -> NMApp SlaveSends MasterSends m () -> m void
runSlave r cont = do
    nmSettings <- defaultNMSettings
    withSlaveRequests r (\wcis -> void (connectToMaster nmSettings wcis cont))

runMasterCollectResults :: (MonadConnect m) => Redis -> MasterId -> Int -> m ()
runMasterCollectResults r mid numSlaves = do
    resultsVar :: TVar (Map.Map SlaveId MasterId) <- liftIO (newTVarIO mempty)
    let whenSlaveConnects nm = ignoreNetworkExceptions $ do
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
    results <- runMaster r mid whenSlaveConnects master
    unless (results == Map.fromList [(SlaveId x, mid) | x <- [1..numSlaves]]) $
        fail "Unexpected results"

runEchoSlave :: (MonadConnect m) => Redis -> SlaveId -> Int -> m void
runEchoSlave r slaveId delay = do
    let slave nm = ignoreNetworkExceptions $ do
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
            (runMaster r mid whenSlaveConnects master) (runEchoSlave r (SlaveId 0) 0)
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
    redisIt "Echo with many masters and many slaves (long)" $ \r -> do
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



