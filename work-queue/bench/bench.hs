{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Control.Monad.IO.Class (liftIO)
import Control.Monad.Logger (runNoLoggingT)
import Criterion.Main
import Data.Proxy (Proxy(..))
import Distributed.JobQueue.Client
import Distributed.RedisQueue
import Distributed.RedisQueue.Internal
import Distributed.WorkQueueSpec (forkMasterSlave, redisTestPrefix)
import FP.Redis

main :: IO ()
main =
  -- FIXME: Ideally there would be a way to get redis connections and free for
  -- each benchmark (but not measure this time). This is not yet possible - see
  -- https://github.com/bos/criterion/issues/79
  runNoLoggingT $
  withRedis redisTestPrefix (connectInfo "localhost") $
  \r -> liftIO $ defaultMain
      [ bgroup "2^12 integers xored together (and initial 100ms delay)"
          [ bench "no slaves" $ nfIO $ forkMasterSlave "bench0"
          , bench "1 slave"   $ nfIO $ forkMasterSlave "bench1"
          , bench "2 slaves"  $ nfIO $ forkMasterSlave "bench2"
          , bench "10 slaves" $ nfIO $ forkMasterSlave "bench10"
          ]
      , bgroup "JobQueue"
          [ bench "sendJobRequest" $ nfIO $ runNoLoggingT $ do
              let encoded = encodeRequest ("" :: String) (Proxy :: Proxy Int)
                  k = getRequestId encoded
              sendRequestInternal' defaultClientConfig r k encoded
          ]
      ]
