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
module Distributed.RequestSlavesSpec (spec) where

import           ClassyPrelude
import           Test.Hspec
import           Data.TypeFingerprint
import           FP.Redis
import           Data.Serialize (Serialize)
import qualified Data.UUID as UUID
import qualified Data.UUID.V4 as UUID
import qualified Control.Concurrent.Async.Lifted.Safe as Async
import           Data.Void (absurd)

import           Distributed.Redis
import           Data.Streaming.NetworkMessage
import           Distributed.RequestSlaves
import           Distributed.Types

import           TestUtils

-- * Utils
-----------------------------------------------------------------------

newtype MasterSends = MasterSends Int
    deriving (Eq, Show, Typeable, Serialize)
newtype SlaveSends = SlaveSends Int
    deriving (Eq, Show, Typeable, Serialize)

mkManyHasTypeFingerprint [[t|MasterSends|], [t|SlaveSends|]]

getWorkerId :: (MonadIO m) => m WorkerId
getWorkerId = do
    liftIO (WorkerId . UUID.toASCIIBytes <$> UUID.nextRandom)

runMaster :: forall m a.
       (MonadConnect m)
    => Redis -> (NMApp MasterSends SlaveSends m ()) -> m a -> m a
runMaster r contSlave cont = do
    nmSettings <- defaultNMSettings
    whenSlaveConnects :: MVar (m ()) <- newEmptyMVar
    let contSlave' nm = do
            join (readMVar whenSlaveConnects)
            contSlave nm
    acceptSlaveConnections nmSettings "127.0.0.1" contSlave' $ \wci -> do
        wid <- getWorkerId
        requestSlaves r wid wci $ \wsc -> do
            putMVar whenSlaveConnects wsc
            cont

runSlave :: forall m void.
       (MonadConnect m)
    => Redis -> NMApp SlaveSends MasterSends m () -> m void
runSlave r cont = do
    nmSettings <- defaultNMSettings
    withSlaveRequests r (\wcis -> void (connectToMaster nmSettings wcis cont))

-- * Spec
-----------------------------------------------------------------------

spec :: Spec
spec = do
    redisIt "simple echo" $ \r -> do
        slaveDataOnMaster :: MVar SlaveSends <- newEmptyMVar
        let whenSlaveConnects nm = do
                nmWrite nm (MasterSends 42)
                putMVar slaveDataOnMaster =<< nmRead nm
        let master = takeMVar slaveDataOnMaster
        let slave nm = do
                MasterSends n <- nmRead nm
                nmWrite nm (SlaveSends n)
        result <- fmap (either id absurd) $ Async.race
            (runMaster r whenSlaveConnects master) (runSlave r slave)
        unless (result == SlaveSends 42) $
            fail ("Expecting 42 in SlaveSends, got " ++ show result)
