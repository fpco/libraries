{-# LANGUAGE TupleSections #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ParallelListComp #-}
module Distributed.StatefulSpec (spec) where

import           Control.Concurrent.Async
import           Data.Binary (Binary)
import           Data.Conduit.Network (serverSettings, clientSettings)
import qualified Data.HashMap.Strict as HMS
import qualified Data.HashSet as HS
import           Data.Streaming.NetworkMessage
import qualified Data.Vector as V
import           Distributed.Stateful
import           Test.Hspec (shouldBe)
import qualified Test.Hspec as Hspec
import           Control.DeepSeq (NFData)

-- FIXME: better fix than "master inner function should get a Vector
-- StateId"

spec :: Hspec.Spec
spec = do
    it "Sends data around properly" $ do
        nms <- nmsSettings
        let slaveUpdate context input stateId state =
                let val = (context, input)
                in (val, HMS.singleton stateId (Right val : state))
        let initialStates :: [[Either Int (String, String)]]
            initialStates = map ((:[]) . Left) [1..4]
        runMasterAndSlaves 7000 4 slaveUpdate initialStates $ \sids mh -> do
            let inputs = HMS.fromList
                    [ (sid, ("input 1" :: String, i :: Int))
                    | i <- [1..4]
                    | sid <- sids
                    ]
            outputs <- update mh ("step 1" :: String) (HMS.map fst inputs)
            states <- getStates mh
            states `shouldBe` (HMS.map (\(input, initial) -> [Right ("step 1", input), Left initial]) inputs)

it :: String -> IO () -> Hspec.Spec
it name f = Hspec.it name f

runMasterAndSlaves
    :: (NFData state, Binary state, Binary context, Binary input, Binary output)
    => Int
    -> Int
    -> (context -> input -> StateId -> state -> (output, HMS.HashMap StateId state))
    -> [state]
    -> ([StateId] -> MasterHandle state context input output -> IO a)
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
            , saNMSettings = nms
            , saServerSettings = serverSettings port "*"
            }
    withAsync (mapConcurrently (slave . slaveArgs) ports) $ \_ ->
        master masterArgs (inner . V.toList)

nmsSettings :: IO NMSettings
nmsSettings = do
  nms <- defaultNMSettings
  return $ setNMHeartbeat 5000000 nms -- We use 5 seconds
