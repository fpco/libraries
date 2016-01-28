{-# LANGUAGE OverloadedStrings #-}

import Control.Concurrent.STM
import Control.Monad.Logger
import Data.ByteString (ByteString)
import Distributed.JobQueue.Client.NewApi
import Distributed.RedisQueue

type Request = ByteString
type Response = ByteString

main :: IO ()
main = do
  logFunc <- runStdoutLoggingT askLoggerIO
  jc <- newJobClient logFunc defaultJobClientConfig { jccRedisPrefix = "stateful-demo:" }
  -- Use the current time as the request id, to avoid cached results.
  now <- getCurrentTime
  let rid = RequestId (show now)
  let request = ", request input " :: Request
  stmResponse <- submitRequest jc rid request
  eres <- atomically $ do
    meres <- stmResponse
    case meres of
      Nothing -> retry
      Just eres -> return eres
  print (eres :: Either DistributedJobQueueException Response)
