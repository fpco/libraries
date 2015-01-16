{-# LANGUAGE OverloadedStrings #-}

module Main where

import ClassyPrelude (toStrict, fromStrict)
import Control.Concurrent.Lifted (fork, threadDelay)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import Control.Concurrent.STM (atomically, newTVarIO, readTVar, check)
import Control.Monad (forever, void)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Logger (runStdoutLoggingT)
import Data.Binary (encode, decode)
import Data.Bits (xor, zeroBits)
import Data.ByteString.Char8 (ByteString, pack)
import Data.List (foldl')
import Data.List.Split (chunksOf)
import Distributed.RedisQueue
import Distributed.WorkQueue
import FP.Redis.Connection (ConnectInfo, connectInfo)
import System.Environment (getArgs)
import System.Posix.Process (getProcessID)

main :: IO ()
main = do
    args <- getArgs
    case args of
        ["dispatcher"] -> dispatcher
        _ -> masterOrSlave

dispatcher :: IO ()
dispatcher =
    runStdoutLoggingT $ withRedisInfo prefix localhost $ \redis -> do
        let client = ClientInfo redis "dispatcher"
        subscribed <- liftIO $ newTVarIO False
        done <- liftIO $ newEmptyMVar
        _ <- fork $ subscribeToResponses client subscribed $ \requestId ->
            withResponse client requestId $ \response -> do
                let result = decode (fromStrict response) :: Int
                liftIO $ do
                    putStrLn "================"
                    putStrLn $ "Received result: " ++ show result
                    putStrLn "================"
                    putMVar done ()
        _ <- fork $ forever $ do
            let micros = 2 * 1000 * 1000
            checkHeartbeats redis micros
            threadDelay micros
        -- Block until we're subscribed before pushing requests.
        liftIO $ atomically $ check =<< readTVar subscribed
        -- Push a single work request.
        _ <- pushRequest client (toStrict (encode ([1..(2^(8 :: Int))-1] :: [Int])))
        liftIO $ takeMVar done

masterOrSlave :: IO ()
masterOrSlave = runArgs initialData calc inner
  where
    initialData = return ()
    calc () input = return $ foldl' xor zeroBits input
    inner () queue = do
        runStdoutLoggingT $ withRedisInfo prefix localhost $ \redis -> do
            processId <- liftIO getProcessID
            let wid = WorkerId $ pack $ show (fromIntegral processId :: Int)
                worker = WorkerInfo redis wid
            _ <- fork $ forever $ do
                sendHeartbeat worker
                void $ threadDelay (1000 * 1000)
            forever $ do
                (requestId, request) <- popRequest worker 0
                let xs = decode (fromStrict request)
                subresults <- mapQueue queue (chunksOf 100 xs)
                result <- calc () subresults
                -- threadDelay (10 * 1000 * 1000)
                sendResponse worker requestId (toStrict (encode result))
                liftIO $ do
                    putStrLn "================"
                    putStrLn $ "Sent result: " ++ show (result :: Int)
                    putStrLn "================"

localhost :: ConnectInfo
localhost = connectInfo "localhost"

prefix :: ByteString
prefix = "fpco.work-queue."
