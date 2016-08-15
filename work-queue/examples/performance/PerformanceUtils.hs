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
module PerformanceUtils where

import           ClassyPrelude
import           System.Process
import           System.Environment (getExecutablePath)
import           Control.Monad.Logger
import           Criterion.Measurement
import           Distributed.JobQueue.Client
import           Distributed.JobQueue.Worker
import           Data.Store (Store)
import           FP.Redis (MonadConnect)
import           Data.Store.TypeHash
import           Distributed.Stateful
import           Distributed.Stateful.Master
import           Control.DeepSeq
import           Control.Concurrent.Mesosync.Lifted.Safe
import qualified Data.Conduit.Network as CN

spawnAndWaitForWorker :: MonadIO m => m ProcessHandle
spawnAndWaitForWorker = liftIO $ do
    program <- getExecutablePath
    args <- getArgs
    runProcess program ("--spawn-worker":map unpack args) Nothing Nothing Nothing Nothing Nothing

logErrors :: MonadIO m => LoggingT m a -> m a
logErrors = runStdoutLoggingT . filterLogger (\_ ll -> ll >= LevelError)

performRequest :: forall m response request.
                 ( MonadConnect m
                 , Store request, Store response
                 , HasTypeHash request, HasTypeHash response
                 , Show response)
                 => m request -> JobClient response -> m ()
performRequest generateRequest jc = do
    liftIO initializeTime
    tzero <- liftIO getTime
    rid <- uniqueRequestId
    req <- generateRequest
    t0 <- liftIO getTime
    submitRequest (jc :: JobClient response) rid req
    mRes <- waitForResponse_ jc rid
    liftIO . putStrLn $ unwords [ "result:", pack $ show (mRes :: Maybe response)]
    t1 <- liftIO getTime
    liftIO . putStrLn $ "Generating the request took" ++ pack (show (t0 - tzero)) ++ " seconds."
    liftIO . putStrLn $ "The request took " ++ pack (show (t1 - t0)) ++ " seconds."



runWithNM :: forall m context input output state request response.
    ( MonadConnect m
    , NFData state, NFData output
    , Store state, Store context, Store input, Store output, Store request, Store response
    , HasTypeHash state, HasTypeHash context, HasTypeHash input, HasTypeHash output, HasTypeHash request, HasTypeHash response
    , Show response)
    => JobQueueConfig
    -> Bool
    -> MasterArgs m state context input output
    -> Int
    -> (request
      -> MasterHandle m state context input output
      -> m response)
    -> m request
    -> m ()
runWithNM jqc spawnWorker masterArgs nSlaves workerFunc generateRequest =
    if spawnWorker
       then do putStrLn "spawning worker"
               let ss = CN.serverSettings 3333 "*"
               runJobQueueStatefulWorker
                   jqc
                   ss
                   "127.0.0.1"
                   Nothing
                   masterArgs
                   (NMStatefulMasterArgs (Just nSlaves) (Just nSlaves) (1000 * 1000))
                   (\mh _rid req -> DontReenqueue <$> workerFunc req mh)
        else
            bracket
                (mapConcurrently (const spawnAndWaitForWorker) [0..(nSlaves :: Int)])
                (mapM (liftIO . terminateProcess))
                (\_ -> withJobClient jqc $ \(jc :: JobClient response) -> performRequest generateRequest jc)

runWithoutNM ::
  ( MonadConnect m
  , Show response, NFData state
  , NFData output, Store state) =>
  MasterArgs m state context input output
  -> Int
  -> (request
      -> MasterHandle m state context input output
      -> m response)
  -> m request
  -> m ()
runWithoutNM masterArgs nSlaves masterFunc generateRequest = do
    req <- generateRequest
    liftIO initializeTime
    t0 <- liftIO getTime
    res <- runSimplePureStateful masterArgs nSlaves (masterFunc req)
    print res
    t1 <- liftIO getTime
    putStrLn $ "The request took " ++ pack (show (t1 - t0)) ++ " seconds."
