module Main where

import           Test.Hspec

import qualified Distributed.HeartbeatSpec
import qualified Distributed.RequestSlavesSpec
import qualified Data.Streaming.NetworkMessageSpec

main :: IO ()
main = hspec $ do
    describe "Distributed.Heartbeat" Distributed.HeartbeatSpec.spec
    describe "Distributed.RequestSlaves" Distributed.RequestSlavesSpec.spec
    describe "Data.Streaming.NetworkMessage" Data.Streaming.NetworkMessageSpec.spec
