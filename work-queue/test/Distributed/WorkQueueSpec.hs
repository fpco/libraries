{-# LANGUAGE NoImplicitPrelude  #-}
{-# LANGUAGE OverloadedStrings #-}
module Distributed.WorkQueueSpec where

import           ClassyPrelude hiding (intersect)
import           Control.Concurrent (threadDelay, forkIO, killThread)
import           Control.Concurrent.Async (race)
import           Data.Binary (Binary)
import           Data.Bits (xor, zeroBits)
import           Data.List (intersect, delete)
import           Data.List.Split (chunksOf)
import           Data.Streaming.Network (getSocketFamilyTCP)
import           Distributed.WorkQueue
import           Filesystem (isFile, removeFile)
import qualified Network.Socket as NS
import           Prelude (appendFile, read)
import           System.Environment (withArgs)
import           System.Exit (ExitCode(ExitSuccess))
import           System.Posix.Process (forkProcess, getProcessStatus, getProcessID, ProcessStatus(Exited))
import           System.Posix.Signals (signalProcess, killProcess)
import           System.Posix.Types (ProcessID)
import           System.Random (randomRIO)
import           Test.Hspec (Spec, it, shouldBe)

data Config = Config
    { masterJobs :: Int
    , checkMasterRan :: Bool
    , checkSlaveRan :: Bool
    , checkAllSlavesRan :: Bool
    , wrapProcess :: IO () -> IO ()
    , whileRunning :: ProcessID -> IO ProcessID -> IO ()
    }

defaultConfig :: Config
defaultConfig = Config 1 True True True id (\_ startSlave -> startSlave >> return ())

spec :: Spec
spec = do
    it "can run tasks on master and slave server" $
        runXor 0 12 100 defaultConfig
    it "can run tasks on just the master server" $
        runXor 1 12 100 defaultConfig
            { checkSlaveRan = False
            , checkAllSlavesRan = False
            , whileRunning = \_ _ -> return ()
            }
    it "can run tasks on master and two slave servers" $
        runXor 2 12 100 defaultConfig
            { whileRunning = \_ startSlave -> startSlave >> startSlave >> return () }
    it "can run tasks only on slaves" $
        runXor 3 12 100 defaultConfig
            { masterJobs = 0
            , checkMasterRan = False
            , whileRunning = \_ startSlave -> startSlave >> startSlave >> return ()
            }
    it "preserves data despite slaves being started and killed periodically" $
        runXor 4 12 100 defaultConfig
            { masterJobs = 0
            , checkMasterRan = False
            , checkAllSlavesRan = False
            , whileRunning = \_ startSlave -> do
                let randomSlaveSpawner = forever $ do
                        pid <- startSlave
                        ms <- randomRIO (0, 300)
                        threadDelay (1000 * ms)
                        signalProcess killProcess pid
                void $ randomSlaveSpawner `race` randomSlaveSpawner
            }

runXor :: Int -> Int -> Int -> Config -> IO ()
runXor expected bits chunkSize config = do
    firstRunRef <- newIORef True
    let xs = expected : [1..(2^bits)-1]
        initialData = return ()
        calc () input = do
            -- This thread delay is necessary so that the master
            -- doesn't start up so fast that the client never gets a
            -- chance to start.  Ideally this would only run for the
            -- master.
            firstRun <- readIORef firstRunRef
            when firstRun $ do
                threadDelay (100 * 1000)
                writeIORef firstRunRef False
            return $ foldl' xor zeroBits input
        inner () queue = do
            subresults <- mapQueue queue (chunksOf chunkSize xs)
            calc () subresults
    result <- forkMasterSlave config initialData calc inner
    result `shouldBe` expected

-- Run the master and slave in separate processes, using files to
-- communicate with the test process.
forkMasterSlave
    :: ( Typeable initialData
       , Typeable payload
       , Typeable result
       , Binary initialData
       , Binary payload
       , Binary result
       , Show output
       , Read output
       )
    => Config
    -> IO initialData
    -> (initialData -> payload -> IO result)
    -> (initialData -> WorkQueue payload result -> IO output)
    -> IO output
forkMasterSlave config initialData calc' inner' = do
    slavesRef <- newIORef []
    let cleanup = do
            removeFileIfExists seenPidsPath
            removeFileIfExists resultsPath
        seenPidsPath = "work_queue_spec_seen_pids"
        resultsPath = "work_queue_spec_results"
        calc initial input = do
            pid <- getProcessID
            appendFile (fpToString seenPidsPath) (show pid ++ "\n")
            calc' initial input
        inner initial queue = do
            result <- inner' initial queue
            appendFile (fpToString resultsPath) (show result)
        startSlave = do
            pid <- forkProcess $ withArgs ["slave", "localhost", "2015"] run
            modifyIORef slavesRef (pid:)
            return pid
        run = wrapProcess config $ runArgs initialData calc inner
        go = do
            cleanup
            masterPid <- forkProcess $ withArgs ["master", show (masterJobs config), "2015"] run
            waitForSocket
            tid <- forkIO $ whileRunning config masterPid startSlave
            status <- getProcessStatus True False masterPid
            status `shouldBe` Just (Exited ExitSuccess)
            -- Ensure that both the master and slave processes have
            -- performed calculation.
            seenPids <- ordNub . map read . lines <$> readFile seenPidsPath
            slavePids <- readIORef slavesRef
            when (checkMasterRan config && masterPid `notElem` seenPids) $
                fail "Master never ran"
            when (checkAllSlavesRan config) $
                sort (delete masterPid seenPids) `shouldBe` sort slavePids
            when (checkSlaveRan config && null (intersect seenPids slavePids)) $
                fail "No slave ran"
            -- Exit the 'whileRunning' thread.
            killThread tid
            -- Read out the results file.
            result <- readFile resultsPath
            return (read result)
    cleanup
    go `finally` cleanup

-- Wait for a connection to the test socket to succeed, indicating
-- that the master is accepting connections.
waitForSocket :: IO ()
waitForSocket = loop (100 :: Int)
  where
    loop 0 = fail "Ran out of waitForSocket retries."
    loop n = do
        eres <- tryAny $ getSocketFamilyTCP "localhost" 2015 NS.AF_UNSPEC
        case eres of
            Left _ -> do
                threadDelay (20 * 1000)
                loop (n - 1)
            Right (socket, _) ->
                NS.close socket

-- Remove the specified file, if it exists.
removeFileIfExists :: FilePath -> IO ()
removeFileIfExists fp = do
    exists <- isFile fp
    when exists $ removeFile fp
