module Main where

import           Test.Hspec

import qualified Distributed.HeartbeatSpec
import qualified Distributed.RequestSlavesSpec

main :: IO ()
main = hspec $ do
    -- describe "Distributed.Heartbeat" Distributed.HeartbeatSpec.spec
    describe "Distributed.RequestSlaves" Distributed.RequestSlavesSpec.spec
