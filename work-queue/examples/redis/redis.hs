{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE NoImplicitPrelude #-}

module Main where

import ClassyPrelude
import Control.Monad.Logger (runStdoutLoggingT)
import Data.Bits (xor, zeroBits)
import Data.List.NonEmpty (nonEmpty)
import Data.List.Split (chunksOf)
import Distributed.JobQueue
import Distributed.RedisQueue (withRedis)
import Distributed.WorkQueue (mapQueue)
import FP.Redis

main :: IO ()
main = do
    args <- getArgs
    case args of
        ["dispatcher"] -> dispatcher
        _ -> masterOrSlave

dispatcher :: IO ()
dispatcher = do
    clearData
    let config = defaultClientConfig
    runStdoutLoggingT $ withRedis prefix localhost $ \redis ->
        -- Run the client heartbeat checker and back channel
        -- subscription.
        withJobQueueClient config redis $ \cvs -> do
             -- Push a single set of work requests.
             let workItems = fromList (chunksOf 100 [1..(2^(8 :: Int))-1]) :: Vector [Int]
             response <- jobQueueRequest config cvs redis workItems
             liftIO $ do
                 putStrLn "================"
                 putStrLn $ "Received result: " ++ tshow (response :: Int)
                 putStrLn "================"

masterOrSlave :: IO ()
masterOrSlave =
    runStdoutLoggingT $ jobQueueWorker config calc inner
  where
    config = defaultWorkerConfig prefix localhost "localhost"
    calc input = return $ foldl' xor zeroBits (input :: [Int])
    inner redis mci request queue = do
        requestSlave redis mci
        subresults <- mapQueue queue request
        response <- calc (otoList (subresults :: Vector Int))
        liftIO $ do
            putStrLn "================"
            putStrLn $ "Sending result: " ++ tshow (response :: Int)
            putStrLn "================"
        return response

localhost :: ConnectInfo
localhost = connectInfo "localhost"

prefix :: ByteString
prefix = "fpco:work-queue:"

clearData :: IO ()
clearData =
    runStdoutLoggingT $ withConnection localhost $ \redis -> do
        matches <- runCommand redis $ FP.Redis.keys (prefix <> "*")
        mapM_ (runCommand_ redis . del) (nonEmpty matches)
