{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE NoImplicitPrelude #-}
module PerformanceUtils ( CSVInfo (..)
                        , runWithNM
                        , runWithoutNM
                        , logSourceBench
                        , logErrorsOrBench
                        ) where

import           ClassyPrelude
import           Control.Concurrent.Lifted (threadDelay)
import           Control.Concurrent.Mesosync.Lifted.Safe
import           Control.DeepSeq
import           Control.Lens
import           Control.Monad.Logger
import qualified Control.Retry as R
import           Criterion.Measurement
import qualified Data.Conduit.Network as CN
import           Data.Store (Store)
import           Data.Store.TypeHash
import           Data.Streaming.Process (Inherited(..), ClosedStream(..), streamingProcess, waitForStreamingProcess, ProcessExitedUnsuccessfully(..), streamingProcessHandleRaw)
import           Data.Void
import           Distributed.JobQueue.Client
import           Distributed.JobQueue.Status
import           Distributed.JobQueue.Worker
import           Distributed.Redis (Redis)
import           Distributed.Stateful
import           Distributed.Stateful.Internal (slaveProfilingToCsv)
import           Distributed.Stateful.Master
import           FP.Redis (MonadConnect)
import           GHC.Environment (getFullArgs)
import           System.Directory
import           System.Environment (getExecutablePath)
import           System.Exit (ExitCode(..))
import           System.IO (withFile, IOMode (..))
import           System.Process
import           TypeHash.Orphans ()


-- | Key value pairs to be written to a csv file.
data CSVInfo = CSVInfo [(Text, Text)] deriving Show
instance Monoid CSVInfo where
    mempty = CSVInfo []
    CSVInfo xs `mappend` CSVInfo xs' = CSVInfo $ xs `mappend` xs'
instance Semigroup CSVInfo

withWorker :: MonadConnect m
    => Int -- ^ number of slaves the worker should accept
    -> m a -- ^ action to perform while the slave is running
    -> m a
withWorker nSlaves cont = do
    program <- liftIO getExecutablePath
    args <- liftIO getFullArgs
    let cp = proc program (("--spawn-worker=" ++ show nSlaves):map unpack args)
    (x, y, z, sph) <- streamingProcess cp
    let cont' = \ClosedStream Inherited Inherited -> do
            res <- cont
            liftIO $ terminateProcess (streamingProcessHandleRaw sph)
            return res
    either absurd id <$> race
        (do waitForStreamingProcess sph >>= \case
                ExitSuccess -> forever (threadDelay maxBound)
                ec -> liftIO . throwIO $ ProcessExitedUnsuccessfully cp ec
        )
        (cont' x y z `onException` (liftIO $ terminateProcess (streamingProcessHandleRaw sph)))

-- | Will spawn n+1 workers, that live as long as the given action
-- takes. The 'NMStatefulMasterArgs' will be set to accept exactly n
-- slaves.
withNSlaves :: MonadConnect m
    => Int -- ^ @n@, the number of slaves
    -> m a
    -> m a
withNSlaves nSlaves action = go 0 action
  where
    go n cont | n > nSlaves = cont
    go n cont = go (n+1) (withWorker nSlaves cont)

-- | Wait until exactly @n@ workers are live.
--
-- We want to do this before starting to measure the time that the
-- request takes, in order not to include the workers' startup time in
-- the benchmark.  In particular, we don't want to increase the time
-- that the master waits for slaves.
waitForNWorkers :: MonadConnect m => Redis -> Int -> m ()
waitForNWorkers r n =
    void $ R.retrying policy retryWhen getNWorkers
  where
    policy = R.constantDelay 10000 R.<> R.limitRetries 1000 -- wait for up to ten seconds, in steps of ten milliseconds
    retryWhen _ n' = return $ n' /= n
    getNWorkers = length . jqsWorkers <$> getJobQueueStatus r

logSourceBench :: LogSource
logSourceBench = "benchmark"

-- | Only log messages that either come from the benchmark directly,
-- and errors.
logErrorsOrBench :: MonadIO m => LoggingT m a -> m a
logErrorsOrBench = runStdoutLoggingT . filterLogger (\ls ll -> ll >= LevelError || ls == logSourceBench)

-- | Measure the time between submitting a request and receiving the
-- corresponding response.
measureRequestTime :: forall m response request.
    ( MonadConnect m
    , Store request, Store response
    , HasTypeHash request, HasTypeHash response
    , Show response)
    => m request
    -> JobClient (response, SlaveProfiling)
    -> m (Double, SlaveProfiling) -- ^ Wall time between submitting the request and
               -- receiving the response.
measureRequestTime generateRequest jc = do
    rid <- uniqueRequestId
    req <- generateRequest
    t0 <- liftIO getTime
    submitRequest (jc :: JobClient (response, SlaveProfiling)) rid req
    waitForResponse_ jc rid >>= \case
        Just (resp :: response, sp) -> do
            $logInfoS logSourceBench $ unwords [ "result:", tshow resp]
            t1 <- liftIO getTime
            $logInfoS logSourceBench $ "The request took " ++ pack (show (t1 - t0)) ++ " seconds."
            return (t1 - t0, sp)
        Nothing -> error "no response"

runWithNM :: forall m a context input output state request response.
    ( MonadConnect m
    , NFData state, NFData output, NFData input
    , Store state, Store context, Store input, Store output, Store request, Store response
    , HasTypeHash state, HasTypeHash context, HasTypeHash input, HasTypeHash output, HasTypeHash request, HasTypeHash response
    , Show response)
    => FilePath
    -> CSVInfo
    -> JobQueueConfig
    -> Maybe a
    -- ^ If @Nothing@, perform the request.
    -- ^ Otherwise, spawn a worker that acceps @nSlaves@ slaves.
    -> MasterArgs m state context input output
    -> Int
    -- ^ @nSlaves@
    -> (request
      -> MasterHandle m state context input output
      -> m (response, SlaveProfiling))
    -> m request
    -> m (Int, Double)
    -- ^ (@nSlaves@, walltime needed to perform the request)
runWithNM fp csvInfo jqc spawnWorker masterArgs nSlaves workerFunc generateRequest =
    if isJust spawnWorker
       then do
           $logDebugS logSourceBench "spawning worker"
           let ss = CN.serverSettings 3333 "*"
           runJobQueueStatefulWorker
               jqc
               ss
               "127.0.0.1"
               Nothing
               masterArgs
               (NMStatefulMasterArgs (Just nSlaves) (Just nSlaves) (1000 * 1000))
               (\mh _rid req -> DontReenqueue <$> workerFunc req mh)
       else do
           (time, sp) <- withNSlaves
               nSlaves
               (withJobClient jqc $ \(jc :: JobClient (response, SlaveProfiling)) -> do
                       waitForNWorkers (jcRedis jc) (nSlaves + 1) -- +1 for the worker
                       measureRequestTime generateRequest jc)
           liftIO $ writeToCsv fp (CSVInfo [("time", pack $ show time)] <> csvInfo <> slaveProfilingCsv sp)
           return (nSlaves, time)

runWithoutNM ::
    ( MonadConnect m
    , Show response, NFData state, NFData input
    , NFData output, Store state)
    => FilePath
    -> CSVInfo
    -> MasterArgs m state context input output
    -> Int
    -> (request
       -> MasterHandle m state context input output
       -> m (response, SlaveProfiling))
    -> m request
    -> m (Int, Double)
runWithoutNM fp csvInfo masterArgs nSlaves masterFunc generateRequest = do
    req <- generateRequest
    liftIO initializeTime
    t0 <- liftIO getTime
    (res, sp) <- runSimplePureStateful masterArgs nSlaves (masterFunc req)
    $logInfoS logSourceBench $ unwords [ "result:", tshow res]
    t1 <- liftIO getTime
    $logInfoS logSourceBench $ "The request took " ++ pack (show (t1 - t0)) ++ " seconds."
    liftIO . writeToCsv fp $ CSVInfo [("time", pack $ show (t1 - t0))] <> csvInfo <> slaveProfilingCsv sp
    return (nSlaves, (t1 - t0))

slaveProfilingCsv :: SlaveProfiling -> CSVInfo
slaveProfilingCsv = CSVInfo . slaveProfilingToCsv

-- | Write key value pairs to a csv file.
--
-- If the file exists, it is assumed that the header also exists, and
-- the values are appended.  If the file does not exist yet, it is
-- created, and the header and data is written.
writeToCsv :: FilePath -> CSVInfo -> IO ()
writeToCsv fp (CSVInfo vals) = do
    exists <- doesFileExist fp
    withFile fp (if exists then AppendMode else WriteMode) $ \h -> do
        unless exists (hPutStrLn h $ intercalate "," $ map fst vals)
        hPutStrLn h $ intercalate "," $ map snd vals
