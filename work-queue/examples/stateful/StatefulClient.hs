{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE OverloadedStrings #-}

import Control.Concurrent.STM
import Control.Monad
import Control.Monad.Logger
import Data.ByteString (ByteString)
import Data.ByteString.Char8 (pack)
import Data.Maybe
import Data.Time
import Data.TypeFingerprint (mkHasTypeFingerprint)
import Distributed.JobQueue.Client.NewApi

$(mkHasTypeFingerprint =<< [t| ByteString |])

type Request = ByteString
type Response = ByteString

main :: IO ()
main = do
  logFunc <- runStdoutLoggingT askLoggerIO
  jc <- newJobClient logFunc defaultJobClientConfig { jccRedisPrefix = "stateful-demo:" }
  -- Use the current time as the request id, to avoid cached results.
  now <- getCurrentTime
  let rid = RequestId (pack (show now))
  let request = ", request input " :: Request
  mresp <- submitRequest jc rid request
  when (isJust mresp) $ fail "Didn't expect cached result"
  stmResponse <- waitForResponse jc rid
  eres <- atomically $ do
    meres <- stmResponse
    case meres of
      Nothing -> retry
      Just eres -> return eres
  print (eres :: Either DistributedJobQueueException Response)
