{-# OPTIONS_GHC -fno-warn-orphans #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DataKinds #-}

import Control.Monad.Logger
import Control.Monad.IO.Class
import Data.ByteString (ByteString)
import Data.ByteString.Char8 (pack)
import Data.Time
import Data.Store.TypeHash (mkHasTypeHash)
import Distributed.JobQueue.Client
import Distributed.Types
import Distributed.Redis
import Distributed.JobQueue.Worker
import           System.Environment (getArgs)
import Control.Concurrent (threadDelay)
import Data.Monoid

$(mkHasTypeHash =<< [t| ByteString |])

type Request = ByteString
type Response = ByteString

mainClient :: IO ()
mainClient = runStdoutLoggingT $
  withJobClient jqc $ \jc -> do
    -- Use the current time as the request id, to avoid cached results.
    now <- liftIO getCurrentTime
    let rid = RequestId (pack (show now))
        request = ", request input " :: Request
    submitRequest jc rid request
    mResponse <- waitForResponse_ jc rid
    liftIO $ print (mResponse :: Maybe Response)

mainWorker :: IO ()
mainWorker = runStdoutLoggingT $ jobWorker jqc workerFunc

workerFunc :: Redis -> RequestId -> Request -> (LoggingT IO) (Reenqueue Response)
workerFunc _ _ request = do
    liftIO $ threadDelay (60 * 1000 * 1000) -- wait a minute
    return (DontReenqueue $ "Done with " <> request)

jqc :: JobQueueConfig
jqc = defaultJobQueueConfig "stateful-demo:"

main :: IO ()
main = do
  [mode] <- getArgs
  case mode of
    "client" -> mainClient
    "worker" -> mainWorker
    _ -> fail "bad usage"