{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE NoImplicitPrelude #-}

module Main where

import ClassyPrelude
import Control.Concurrent.Lifted (fork)
import Control.Monad.Logger (runStdoutLoggingT)
import Data.Binary (decode)
import Data.Bits (xor, zeroBits)
import Data.List.Split (chunksOf)
import Distributed.JobQueue
import Distributed.RedisQueue (withRedis)
import Distributed.WorkQueue (mapQueue)
import FP.Redis
import Data.List.NonEmpty (nonEmpty)

main :: IO ()
main = do
    args <- getArgs
    case args of
        ["dispatcher"] -> dispatcher
        _ -> masterOrSlave

dispatcher :: IO ()
dispatcher = do
    clearData
    runStdoutLoggingT $ withRedis prefix localhost $ \redis -> do
        -- Client configuration, where heartbeats are checked every 20
        -- seconds, and requests expire after an hour.
        client <- liftIO (newClientVars (Seconds 20) (Seconds 3600))
        -- Run the client heartbeat checker and back channel
        -- subscription.
        _ <- fork $ jobQueueClient client redis
        -- Push a single set of work requests.
        let workItems = fromList (chunksOf 100 [1..(2^(8 :: Int))-1]) :: Vector [Int]
        response <- jobQueueRequest client redis workItems
        let result = decode (fromStrict response) :: Int
        liftIO $ do
            putStrLn "================"
            putStrLn $ "Received result: " ++ tshow result
            putStrLn "================"

masterOrSlave :: IO ()
masterOrSlave =
    runStdoutLoggingT $ jobQueueWorker config initialData calc inner
  where
    config = defaultWorkerConfig prefix localhost
    initialData = return ()
    calc () input = return $ foldl' xor zeroBits (input :: [Int])
    inner () redis request queue = do
        requestSlave config redis
        subresults <- mapQueue queue request
        response <- calc () (otoList (subresults :: Vector Int))
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
        matches <- runCommand redis $ keys (prefix <> "*")
        mapM_ (runCommand_ redis . del) (nonEmpty matches)
