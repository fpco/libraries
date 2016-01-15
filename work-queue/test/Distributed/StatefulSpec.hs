{-# LANGUAGE TupleSections #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ParallelListComp #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE FlexibleContexts #-}
module Distributed.StatefulSpec (spec) where

import           Control.Concurrent.Async
import           Control.DeepSeq (NFData)
import           Control.Monad.Trans.Control (MonadBaseControl)
import           Data.Binary (Binary)
import           Data.Conduit.Network (serverSettings, clientSettings)
import qualified Data.Conduit.Network as CN
import           Data.Foldable
import qualified Data.HashMap.Strict as HMS
import           Data.Streaming.NetworkMessage
import qualified Data.Streaming.NetworkMessage as NM
import qualified Data.Vector as V
import           Distributed.Stateful
import           Test.Hspec (shouldBe)
import qualified Test.Hspec as Hspec

-- FIXME: better fix than "master inner function should get a Vector
-- StateId"

spec :: Hspec.Spec
spec = do
    it "Sends data around properly" $ do
        let slaveUpdate input state =
                return (input,  Right input : state)
        let initialStates :: [[Either Int String]]
            initialStates = map ((:[]) . Left) [1..4]
        runMasterAndSlaves 7000 4 slaveUpdate initialStates $ \mh -> do
            states0 <- getStates mh
            let inputs = HMS.fromList
                    [ (sid, ("input 1" :: String, i :: Int))
                    | i <- [1..4]
                    | (sid, _) <- HMS.toList states0
                    ]
            outputs <- update mh ("step 1" :: String)
            states <- getStates mh
            states `shouldBe` (HMS.map (\(_input, initial) -> [Right "step 1", Left initial]) inputs)

it :: String -> IO () -> Hspec.Spec
it name f = Hspec.it name f

runMasterAndSlaves
    :: forall state updatePayload updateOutput a.
       (NFData state, Binary state, Binary updatePayload, Binary updateOutput)
    => Int
    -> Int
    -> (updatePayload -> state -> IO (updateOutput, state))
    -> [state]
    -> (MasterHandle state updatePayload updateOutput () () () -> IO a)
    -> IO a
runMasterAndSlaves basePort slaveCnt slaveUpdate initialStates inner = do
    nms <- nmsSettings
    let ports = [basePort..basePort + slaveCnt]
    let settings = map (\port -> serverSettings port "localhost") ports
    let masterArgs = MasterArgs
            { maMinBatchSize = Nothing
            , maMaxBatchSize = Just 5
            , maNMSettings = nms
            }
    let slaveArgs = SlaveArgs
            { saUpdate = slaveUpdate
            , saResample = \() () state -> return ((), state)
            , saNMSettings = nms
            , saClientSettings = clientSettings "" "localhost"
            }
    withAsync (mapConcurrently (runSlave . slaveArgs) ports) $ \_ ->
      connectToSlaves (zip settings (repeat nms)) $ \slaves -> do
        mh <- mkMasterHandle masterArgs
        forM_ slaves (addSlaveConnection mh)
        resetStates mh initialStates
        inner mh

-- connectToSlaves
--   :: (MonadBaseControl IO m, Binary state, Binary updatePayload, Binary updateOutput, Binary resampleContext, Binary resamplePayload, Binary resampleOutput)
--   => [(CN.ServerSettings, NM.NMSettings)]
--   -> ([SlaveNMAppData state updatePayload updateOutput resampleContext resamplePayload resampleOutput] -> m a)
--   -> m a
-- connectToSlaves settings0 cont = case settings0 of
--   [] -> cont []
--   (ss, nms) : settings ->
--     CN.runGeneralTCPServer ss $ NM.generalRunNMApp nms (const "") (const "") $ \nm ->
--       connectToSlaves settings (\nodes -> cont (nm : nodes))


nmsSettings :: IO NMSettings
nmsSettings = do
  nms <- defaultNMSettings
  return $ setNMHeartbeat 5000000 nms -- We use 5 seconds
