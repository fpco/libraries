{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TemplateHaskell #-}
module Distributed.Stateful2
    ( -- * Pure implementation
      runSlavePure
    , runSimplePure
    ) where

import           ClassyPrelude
import           Control.DeepSeq (NFData)
import           Data.Serialize.Orphans ()
import qualified Control.Concurrent.Async.Lifted.Safe as Async
import           Distributed.Stateful.Slave
import           Distributed.Stateful.Internal
import           Distributed.Stateful.Master
import           Data.Void (absurd)
import           FP.Redis (MonadConnect)
import           Data.Serialize (Serialize)
import           Control.Concurrent.STM.TMChan

withTMChan :: (MonadConnect m) => (TMChan a -> m b) -> m b
withTMChan = bracket (liftIO newTMChanIO) (atomically . closeTMChan)

runSlavePure :: forall m context input state output a.
       (MonadConnect m, NFData state, NFData output, Serialize state)
    => (context -> input -> state -> m (state, output))
    -> (SlaveConn m state context input output -> m a)
    -> m a
runSlavePure update_ cont =
    withTMChan $ \(reqChan :: TMChan (SlaveReq state context input)) ->
    withTMChan $ \(respChan :: TMChan (SlaveResp state output)) -> do
        let slaveConn = chanStatefulConn respChan reqChan
        let masterConn = chanStatefulConn reqChan respChan
        fmap (either absurd id) $ Async.race
            (runSlave (SlaveArgs update_ slaveConn))
            (cont masterConn)
  where
    chanStatefulConn :: forall req resp.
        TMChan req -> TMChan resp -> StatefulConn m req resp
    chanStatefulConn reqChan respChan = StatefulConn
        { scWrite = \x -> atomically (writeTMChan reqChan x)
        , scRead = do
            mbX <- atomically (readTMChan respChan)
            case mbX of
                Nothing -> fail "runSlavePure: trying to read on closed chan"
                Just x -> return x
        }

runSimplePure :: forall m context input state output a.
       (MonadConnect m, NFData state, NFData output, Serialize state)
    => MasterArgs
    -> Int -- ^ Desired slaves
    -> (context -> input -> state -> m (state, output))
    -> (MasterHandle m state context input output -> m a)
    -> m a
runSimplePure ma slavesNum0 update_ cont = if slavesNum0 < 1
    then fail "runSimplePure: slavesNum0 < 1"
    else do
        mh <- mkMasterHandle ma
        go mh slavesNum0
  where
    go mh slavesNum = if slavesNum == 0
        then cont mh
        else runSlavePure update_ $ \conn -> do
            addSlaveConnection mh conn
            go mh (slavesNum - 1)