{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE NoImplicitPrelude #-}

module Main where

import ClassyPrelude
import Control.Concurrent.Lifted (fork)
import Control.Monad.Logger (runStdoutLoggingT)
import Data.Binary (encode, decode)
import Data.Bits (xor, zeroBits)
import Data.List.Split (chunksOf)
import Distributed.JobQueue
import Distributed.RedisQueue
import Distributed.WorkQueue
import FP.Redis (Seconds(..), ConnectInfo, connectInfo)

main :: IO ()
main = do
    args <- getArgs
    case args of
        ["dispatcher"] -> dispatcher
        _ -> masterOrSlave

dispatcher :: IO ()
dispatcher =
    runStdoutLoggingT $ withRedisInfo prefix localhost $ \redis -> do
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
masterOrSlave = runArgs initialData calc inner
  where
    initialData = return ()
    calc () input = return $ foldl' xor zeroBits (input :: [Int])
    inner () queue = do
        runStdoutLoggingT $ withRedisInfo prefix localhost $ \redis -> do
            void $ jobQueueWorker config redis queue $ \subresults -> do
                result <- calc () (otoList subresults)
                liftIO $ do
                    putStrLn "================"
                    putStrLn $ "Sending result: " ++ tshow (result :: Int)
                    putStrLn "================"
                return (toStrict (encode result))
    -- Send heartbeat every 10 seconds, and response data expires
    -- every hour.
    config = WorkerConfig (10 * 1000 * 1000) (Seconds 3600)

localhost :: ConnectInfo
localhost = connectInfo "localhost"

prefix :: ByteString
prefix = "fpco:work-queue:"
