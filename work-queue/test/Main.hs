module Main where

import           Test.Hspec

import qualified Distributed.HeartbeatSpec
import qualified Distributed.RequestSlavesSpec
import qualified Distributed.JobQueueSpec2
import qualified Data.Streaming.NetworkMessageSpec

main :: IO ()
main = hspec $ do
    describe "Distributed.Heartbeat" Distributed.HeartbeatSpec.spec
    describe "Distributed.RequestSlaves" Distributed.RequestSlavesSpec.spec
    describe "Distributed.JobQueue" Distributed.JobQueueSpec2.spec
    describe "Data.Streaming.NetworkMessage" Data.Streaming.NetworkMessageSpec.spec
