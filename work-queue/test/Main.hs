module Main where

import           Test.Hspec

import qualified Distributed.HeartbeatSpec
import qualified Distributed.RequestSlavesSpec
import qualified Distributed.JobQueueSpec
import qualified Data.Streaming.NetworkMessageSpec
import qualified Distributed.StatefulSpec2

main :: IO ()
main = hspec $ do
    describe "Distributed.Heartbeat" Distributed.HeartbeatSpec.spec
    describe "Distributed.RequestSlaves" Distributed.RequestSlavesSpec.spec
    describe "Distributed.JobQueue" Distributed.JobQueueSpec.spec
    describe "Distributed.StatefulSpec" Distributed.StatefulSpec2.spec
    describe "Data.Streaming.NetworkMessage" Data.Streaming.NetworkMessageSpec.spec
