module Main where

import           Test.Hspec

import qualified Distributed.HeartbeatSpec

main :: IO ()
main = hspec $ do
    describe "Distributed.Heartbeat" Distributed.HeartbeatSpec.spec
