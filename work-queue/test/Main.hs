module Main where

import           Test.Hspec
import           System.Posix.Resource

import qualified Distributed.HeartbeatSpec
import qualified Distributed.RequestSlavesSpec
import qualified Distributed.StaleKeySpec
import qualified Distributed.JobQueueSpec
import qualified Data.Streaming.NetworkMessageSpec
import qualified Distributed.StatefulSpec
import qualified Distributed.StatusSpec
import qualified Data.WorkQueueSpec

main :: IO ()
main = do
  return ()
  -- Some tests open _a lot_ of connections to redis
  rl <- getResourceLimit ResourceOpenFiles
  setResourceLimit ResourceOpenFiles rl{softLimit = ResourceLimit 50000}
  hspec $ do
    describe "Distributed.Heartbeat" Distributed.HeartbeatSpec.spec
    describe "Distributed.RequestSlaves" Distributed.RequestSlavesSpec.spec
    describe "Distributed.JobQueue" Distributed.JobQueueSpec.spec
    describe "Distributed.StatefulSpec" Distributed.StatefulSpec.spec
    describe "Distributed.StatusSpec" Distributed.StatusSpec.spec
    describe "Data.Streaming.NetworkMessage" Data.Streaming.NetworkMessageSpec.spec
    describe "Data.WorkQueue" Data.WorkQueueSpec.spec
    describe "Distributed.StaleKey" Distributed.StaleKeySpec.spec
