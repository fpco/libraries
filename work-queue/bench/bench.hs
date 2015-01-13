module Main (main) where

import Control.Exception.Enclosed (tryAny)
import Control.Monad (replicateM_, void)
import Criterion.Main hiding (defaultConfig)
import Distributed.WorkQueueSpec (runXor, defaultConfig, Config (..))
import System.IO (stdout, stderr)
import System.IO.Silently (hSilence)

main :: IO ()
main = defaultMain
    [ bgroup "2^12 integers xored together (and initial 100ms delay)"
        [ bench "no slaves" $ nfIO $ runXor 0 12 10 (config 0) { checkSlaveRan = False }
        , bench "1 slave"   $ nfIO $ runXor 0 12 10 (config 1)
        , bench "2 slaves"  $ nfIO $ runXor 0 12 10 (config 2)
        , bench "10 slaves" $ nfIO $ runXor 0 12 10 (config 10)
        ]
    ]

config :: Int -> Config
config n = defaultConfig
    { checkAllSlavesRan = False
    , wrapProcess = void . tryAny . hSilence [stdout, stderr]
    , whileRunning = \_ startSlave -> replicateM_ n startSlave
    }
