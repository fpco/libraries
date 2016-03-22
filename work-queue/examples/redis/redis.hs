{-# OPTIONS_GHC -fno-warn-orphans #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE FlexibleInstances #-}

-- Example of combining job-queue with work-queue
module Main where

import ClassyPrelude
import Control.Concurrent (threadDelay)
import Control.Monad.Logger (askLoggerIO)
import Data.Bits (xor, zeroBits)
import Data.List.NonEmpty (nonEmpty)
import Data.List.Split (chunksOf)
import Data.TypeFingerprint (mkManyHasTypeFingerprint)
import Distributed.JobQueue.Client
import Distributed.JobQueue.Worker
import Distributed.Types
import Distributed.WorkQueue (mapQueue)
import FP.Redis
import FP.ThreadFileLogger

$(mkManyHasTypeFingerprint
    [ [t| Int |]
    , [t| [Int] |]
    , [t| Vector [Int] |]
    ])

main :: IO ()
main = do
    args <- getArgs
    case args of
        ["dispatcher"] -> dispatcher
        _ -> masterOrSlave

dispatcher :: IO ()
dispatcher = do
    clearData
    logFunc <- runThreadFileLoggingT askLoggerIO
    jc <- newJobClient logFunc
    let workItems = fromList (chunksOf 100 [1..(2^(8 :: Int))-1]) :: Vector [Int]
    respStm <- submitRequestAndWaitForResponse jc (RequestId "0") workItems
    resp <- atomicallyReturnOrThrow respStm
    putStrLn "================"
    putStrLn $ "Received result: " ++ tshow (resp :: Int)
    putStrLn "================"

masterOrSlave :: IO ()
masterOrSlave =
    runThreadFileLoggingT $
        jobWorker config $ \redis _rid request -> do
        undefined
  where
    calc input = do
        threadDelay (1000 * 1000 * 2)
        return $ foldl' xor zeroBits (input :: [Int])
    inner redis mci _requestId request queue = do
        requestSlave redis mci
        subresults <- mapQueue queue request
        response <- calc (otoList (subresults :: Vector Int))
        liftIO $ do
            putStrLn "================"
            putStrLn $ "Sending result: " ++ tshow (response :: Int)
            putStrLn "================"
        return response

config :: JobQueueConfig
config = defaultJobQueueConfig
    { jqcRedisConfig = defaultRedisConfig
        { rcKeyPrefix = "fpco:hpc-example-redis:" }
    }

localhost :: ConnectInfo
localhost = connectInfo "localhost"

prefix :: ByteString
prefix = "fpco:work-queue:"

clearData :: IO ()
clearData =
    runThreadFileLoggingT $ withConnection localhost $ \redis -> do
        matches <- runCommand redis $ FP.Redis.keys (prefix <> "*")
        mapM_ (runCommand_ redis . del) (nonEmpty matches)
