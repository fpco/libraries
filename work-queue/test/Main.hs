module Main where

import Distributed.WorkQueueSpec (runWorkQueue)
import Spec (spec)
import System.Environment
import Test.Hspec (hspec)

main :: IO ()
main = do
    args <- getArgs
    case args of
        ("work-queue":_) -> runWorkQueue
        _ -> hspec spec
