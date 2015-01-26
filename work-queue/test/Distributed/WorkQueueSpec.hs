{-# LANGUAGE NoImplicitPrelude  #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE RecordWildCards #-}
module Distributed.WorkQueueSpec where

import           ClassyPrelude hiding (intersect)
import           Control.Concurrent (threadDelay)
import           Control.Concurrent.Async (race, async, wait, cancel, Async)
import           Control.Concurrent.MVar (tryReadMVar)
import           Control.Monad.Logger (runStdoutLoggingT)
import           Data.Binary (Binary, encode)
import           Data.Bits (xor, zeroBits)
import           Data.List (intersect, delete)
import           Data.List.Split (chunksOf)
import           Data.Streaming.Network (getSocketFamilyTCP)
import           Distributed.JobQueue
import           Distributed.RedisQueue
import           Distributed.WorkQueue
import           FP.Redis (connectInfo, Seconds(..))
import           Filesystem (isFile, removeFile)
import qualified Network.Socket as NS
import           Prelude (appendFile, read)
import           System.Environment (withArgs)
import           System.Environment.Executable (getExecutablePath)
import           System.Exit (ExitCode(ExitSuccess))
import           System.IO.Silently (hSilence)
import           System.Posix.Process (getProcessID)
import           System.Posix.Types (CPid(..))
import           System.Process (runProcess, waitForProcess, terminateProcess, ProcessHandle)
import           System.Process.Internals (ProcessHandle(..), ProcessHandle__(..))
import           System.Random (randomRIO)
import           Test.Hspec (Spec, it, shouldBe)

data Config
    = XorConfig
        { xorExtra :: Int
        , xorBits :: Int
        , xorChunkSize :: Int
        , sharedConfig :: SharedConfig
        }
    | RedisConfig
        { redisPrefix :: ByteString
        , sharedConfig :: SharedConfig
        }

data SharedConfig = SharedConfig
    { masterJobs :: Int
    , checkMasterRan :: Bool
    , checkSlaveRan :: Bool
    , checkAllSlavesRan :: Bool
    , wrapProcess :: IO () -> IO ()
    , whileRunning :: Process -> IO Process -> IO ()
    , expectedOutput :: Maybe String
    }

spec :: Spec
spec = do
    it "can run tasks on master and slave server" $
        forkMasterSlave "xor0"
    it "can run tasks on just the master server" $
        forkMasterSlave "xor1"
    it "can run tasks on master and two slave servers" $
        forkMasterSlave "xor2"
    it "can run tasks only on slaves" $
        forkMasterSlave "xor3"
    it "preserves data despite slaves being started and killed periodically" $
        forkMasterSlave "xor4"

defaultXorConfig :: Int -> SharedConfig -> Config
defaultXorConfig n c = XorConfig n 12 100 c { expectedOutput = Just (show n) }

defaultRedisConfig :: SharedConfig -> Config
defaultRedisConfig = RedisConfig redisTestPrefix

redisTestPrefix :: ByteString
redisTestPrefix = "fpco:job-queue-test:"

defaultSharedConfig :: SharedConfig
defaultSharedConfig = SharedConfig 1 True True True id f Nothing
  where
    f _ startSlave = void startSlave

-- TODO: Use TH to generate this dispatch so that definitions can go
-- in their respective Spec modules?

getConfig :: Text -> Config
getConfig "xor0" = defaultXorConfig 0 defaultSharedConfig
getConfig "xor1" = defaultXorConfig 1 defaultSharedConfig
    { checkSlaveRan = False
    , checkAllSlavesRan = False
    , whileRunning = \_ _ -> return ()
    }
getConfig "xor2" = defaultXorConfig 2 defaultSharedConfig
    { whileRunning = \_ startSlave -> startSlave >> startSlave >> return () }
getConfig "xor3" = defaultXorConfig 3 defaultSharedConfig
    { masterJobs = 0
    , checkMasterRan = False
    , whileRunning = \_ startSlave -> startSlave >> startSlave >> return ()
    }
getConfig "xor4" = defaultXorConfig 4 defaultSharedConfig
    { masterJobs = 0
    , checkMasterRan = False
    , checkAllSlavesRan = False
    , whileRunning = \_ startSlave -> do
        let randomSlaveSpawner = forever $ do
                _pid <- startSlave
                ms <- randomRIO (150, 300)
                threadDelay (1000 * ms)

        void $ randomSlaveSpawner `race` randomSlaveSpawner
    }
getConfig "bench0" = defaultXorConfig 0 (benchConfig 0) { checkSlaveRan = False}
getConfig "bench1" = defaultXorConfig 0 (benchConfig 1)
getConfig "bench2" = defaultXorConfig 0 (benchConfig 2)
getConfig "bench10" = defaultXorConfig 0 (benchConfig 10)
getConfig "redis0" = defaultRedisConfig defaultSharedConfig { masterJobs = 0, whileRunning = \_ _ -> return () }
getConfig "redis1" = defaultRedisConfig defaultSharedConfig

getConfig which = error $ "No such config: " ++ unpack which

benchConfig :: Int -> SharedConfig
benchConfig n = defaultSharedConfig
    { checkAllSlavesRan = False
    , wrapProcess = void . tryAny . hSilence [stdout, stderr]
    , whileRunning = \_ startSlave -> replicateM_ n startSlave
    }

forkMasterSlaveNoBlock :: Text -> IO (IO ())
forkMasterSlaveNoBlock which = do
    let config = sharedConfig (getConfig which)
        startSlave = execProcessOrFork ["work-queue", unpack which, "slave", "localhost", "2015"]
    removeFileIfExists seenPidsPath
    removeFileIfExists resultsPath
    master <- execProcessOrFork ["work-queue", unpack which, "master", show (masterJobs config), "2015"]
    waitForSocket
    whileRunningThread <- async $ whileRunning config master startSlave
    return $ do
        cancel whileRunningThread
        cancelProcess master

-- Run the master and slave in separate processes or threads, using
-- files to communicate with the test process.
forkMasterSlave :: Text -> IO ()
forkMasterSlave which = do
    slavesRef <- newIORef []
    let config = sharedConfig (getConfig which)
        cleanup = do
            removeFileIfExists seenPidsPath
            removeFileIfExists resultsPath
        startSlave = do
            pid <- execProcessOrFork ["work-queue", unpack which, "slave", "localhost", "2015"]
            mpid' <- processPid pid
            forM_ mpid' $ \pid' -> modifyIORef slavesRef (pid':)
            return pid
        go = do
            master <- execProcessOrFork ["work-queue", unpack which, "master", show (masterJobs config), "2015"]
            mmasterPid <- processPid master
            waitForSocket
            whileRunningThread <- async $ whileRunning config master startSlave
            waitForExit master
            -- Ensure that both the master and slave processes have
            -- performed calculation.
            seenPids <- ordNub . map read . lines <$> readFile seenPidsPath
            slavePids <- readIORef slavesRef
            case (useForkIO, mmasterPid) of
                (False, Just masterPid) -> do
                    when (checkMasterRan config && masterPid `notElem` seenPids) $
                        fail "Master never ran"
                    when (checkAllSlavesRan config) $
                        sort (delete masterPid seenPids) `shouldBe` sort slavePids
                    when (checkSlaveRan config && null (intersect seenPids slavePids)) $
                        fail "No slave ran"
                _ -> return ()
            cancel whileRunningThread
            -- Read out the results file.
            result <- readFile resultsPath
            return result
    cleanup
    output <- go `finally` cleanup
    forM_ (expectedOutput config) $ \expected -> output `shouldBe` expected

seenPidsPath, resultsPath :: FilePath
seenPidsPath = "work_queue_spec_seen_pids"
resultsPath = "work_queue_spec_results"

execProcessOrFork :: [String] -> IO Process
execProcessOrFork args =
    if useForkIO
        then fmap Right $ async $ withArgs args runWorkQueue
        else do
            path <- getExecutablePath
            fmap Left $ runProcess path args Nothing Nothing Nothing Nothing Nothing

runWorkQueue :: IO ()
runWorkQueue = do
    args <- getArgs
    case args of
        ("work-queue" : which : xs) -> withArgs (map unpack xs) $ runMasterOrSlave (getConfig which)
        _ -> fail "Expected test invocation like './test work-queue xor0 ...'"

runMasterOrSlave :: Config -> IO ()
runMasterOrSlave XorConfig {..} = do
    firstRunRef <- newIORef True
    let xs = xorExtra : [1..(2^xorBits)-1]
        initialData = return ()
        calc () input = do
            -- This thread delay is necessary so that the master
            -- doesn't start up so fast that the client never gets a
            -- chance to start.  Ideally this would only run for the
            -- master.
            firstRun <- readIORef firstRunRef
            when firstRun $ do
                threadDelay (200 * 1000)
                writeIORef firstRunRef False
            return $ foldl' xor zeroBits input
        inner () queue = do
            subresults <- mapQueue queue (chunksOf xorChunkSize xs)
            calc () subresults
    runArgs' sharedConfig initialData calc inner
runMasterOrSlave RedisConfig {..} = do
    firstRunRef <- newIORef True
    let initialData = return ()
        calc :: () -> [Int] -> IO Int
        calc () input = do
            -- See similar comment above.
            firstRun <- readIORef firstRunRef
            when firstRun $ do
                threadDelay (200 * 1000)
                writeIORef firstRunRef False
            return $ foldl' xor zeroBits input
        inner () queue = do
            runStdoutLoggingT $ withRedisInfo redisPrefix (connectInfo "localhost") $ \redis -> do
                void $ jobQueueWorker config redis queue $ \subresults -> do
                    result <- liftIO $ calc () (otoList subresults)
                    return (toStrict (encode result))
        -- Send heartbeat every second, and response data expires
        -- every hour.
        config = WorkerConfig (1000 * 1000) (Seconds 3600)
    runArgs' sharedConfig initialData calc inner

runArgs'
    :: ( Typeable initialData
       , Typeable payload
       , Typeable result
       , Binary initialData
       , Binary payload
       , Binary result
       , Show output
       )
    => SharedConfig
    -> IO initialData
    -> (initialData -> payload -> IO result)
    -> (initialData -> WorkQueue payload result -> IO output)
    -> IO ()
runArgs' config initialData calc' inner' = do
    let calc initial input = do
            pid <- getProcessID
            appendFile (fpToString seenPidsPath) (show pid ++ "\n")
            calc' initial input
        inner initial queue = do
            result <- inner' initial queue
            appendFile (fpToString resultsPath) (show result)
    wrapProcess config $ runArgs initialData calc inner

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

type Process = Either ProcessHandle (Async ())

waitForExit :: Process -> IO ()
waitForExit (Left pid) = do
    status <- waitForProcess pid
    status `shouldBe` ExitSuccess
waitForExit (Right a) = wait a

cancelProcess :: Process -> IO ()
cancelProcess (Left pid) = terminateProcess pid
cancelProcess (Right a) = cancel a

useForkIO :: Bool
useForkIO =
#if COVERAGE
    True
#else
    False
#endif

processPid :: Process -> IO (Maybe Int32)
processPid (Left ph) = processHandlePid ph
processPid _ = return Nothing

processHandlePid :: ProcessHandle -> IO (Maybe Int32)
processHandlePid (ProcessHandle var _) = do
    mpid <- tryReadMVar var
    return $ case mpid of
        Just (OpenHandle (CPid pid)) -> Just pid
        _ -> Nothing
