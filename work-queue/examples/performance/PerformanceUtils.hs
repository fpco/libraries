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
{-# LANGUAGE NoImplicitPrelude #-}
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
import           Control.Monad.Logger
import           Criterion.Measurement
import qualified Data.Conduit.Network as CN
import           Data.Store (Store)
import           Data.Store.TypeHash
import           Data.Streaming.Process (Inherited(..), ClosedStream(..), streamingProcess, waitForStreamingProcess, ProcessExitedUnsuccessfully(..), streamingProcessHandleRaw)
import           Data.Void
import           Distributed.JobQueue.Client
import           Distributed.JobQueue.Worker
import           Distributed.Stateful
import           Distributed.Stateful.Master
import           FP.Redis (MonadConnect)
import           GHC.Environment (getFullArgs)
import           System.Directory
import           System.Environment (getExecutablePath)
import           System.Exit (ExitCode(..))
import           System.IO (withFile, IOMode (..))
import           System.Process


-- | Key value pairs to be written to a csv file.
data CSVInfo = CSVInfo [(Text, Text)]
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
    -> JobClient response
    -> m Double -- ^ Wall time between submitting the request and
               -- receiving the response.
measureRequestTime generateRequest jc = do
    rid <- uniqueRequestId
    req <- generateRequest
    t0 <- liftIO getTime
    submitRequest (jc :: JobClient response) rid req
    mRes <- waitForResponse_ jc rid
    $logInfoS logSourceBench $ unwords [ "result:", tshow (mRes :: Maybe response)]
    t1 <- liftIO getTime
    $logInfoS logSourceBench $ "The request took " ++ pack (show (t1 - t0)) ++ " seconds."
    return (t1 - t0)

runWithNM :: forall m a context input output state request response.
    ( MonadConnect m
    , NFData state, NFData output
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
      -> m response)
    -> m request
    -> m ()
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
           time <- withNSlaves
               nSlaves
               (withJobClient jqc $ \(jc :: JobClient response) -> measureRequestTime generateRequest jc)
           liftIO $ writeToCsv fp (CSVInfo [("time", pack $ show time)] <> csvInfo)

runWithoutNM ::
    ( MonadConnect m
    , Show response, NFData state
    , NFData output, Store state)
    => FilePath
    -> CSVInfo
    -> MasterArgs m state context input output
    -> Int
    -> (request
       -> MasterHandle m state context input output
       -> m response)
    -> m request
    -> m ()
runWithoutNM fp csvInfo masterArgs nSlaves masterFunc generateRequest = do
    req <- generateRequest
    liftIO initializeTime
    t0 <- liftIO getTime
    res <- runSimplePureStateful masterArgs nSlaves (masterFunc req)
    $logInfoS logSourceBench $ unwords [ "result:", tshow res]
    t1 <- liftIO getTime
    $logInfoS logSourceBench $ "The request took " ++ pack (show (t1 - t0)) ++ " seconds."
    liftIO . writeToCsv fp $ CSVInfo [("time", pack $ show (t1 - t0))] <> csvInfo

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
