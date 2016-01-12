{-# LANGUAGE TupleSections #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ParallelListComp #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Distributed.StatefulSpec (spec) where

import           Control.Concurrent.Async
import           Control.DeepSeq (NFData)
import           Data.Binary (Binary)
import           Data.Conduit.Network (serverSettings, clientSettings)
import qualified Data.HashMap.Strict as HMS
import           Data.Streaming.NetworkMessage
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
    :: forall state broadcastPayload broadcastOutput a.
       (NFData state, Binary state, Binary broadcastPayload, Binary broadcastOutput)
    => Int
    -> Int
    -> (broadcastPayload -> state -> IO (broadcastOutput, state))
    -> [state]
    -> (MasterHandle state broadcastPayload broadcastOutput () () () -> IO a)
    -> IO a
runMasterAndSlaves basePort slaveCnt slaveUpdate initialStates inner = do
    nms <- nmsSettings
    let ports = [basePort..basePort + slaveCnt]
    let settings = map (\port -> clientSettings port "localhost") ports
    let masterArgs = MasterArgs
            { maInitialStates = V.fromList initialStates
            , maSlaves = V.fromList (zip settings (repeat nms))
            , maMinBatchSize = Nothing
            , maMaxBatchSize = Just 5
            }
    let slaveArgs port = SlaveArgs
            { saUpdate = slaveUpdate
            , saResample = \() () state -> return ((), state)
            , saNMSettings = nms
            , saServerSettings = serverSettings port "*"
            }
    withAsync (mapConcurrently (runSlave . slaveArgs) ports) $ \_ ->
        runMaster masterArgs inner

nmsSettings :: IO NMSettings
nmsSettings = do
  nms <- defaultNMSettings
  return $ setNMHeartbeat 5000000 nms -- We use 5 seconds
