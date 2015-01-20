{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Criterion.Main
import Distributed.WorkQueueSpec (forkMasterSlave)

main :: IO ()
main = defaultMain
    [ bgroup "2^12 integers xored together (and initial 100ms delay)"
        [ bench "no slaves" $ nfIO $ forkMasterSlave "bench0"
        , bench "1 slave"   $ nfIO $ forkMasterSlave "bench1"
        , bench "2 slaves"  $ nfIO $ forkMasterSlave "bench2"
        , bench "10 slaves" $ nfIO $ forkMasterSlave "bench10"
        ]
    ]
