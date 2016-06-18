{-# OPTIONS_GHC -fno-warn-orphans #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RecordWildCards #-}

import           Control.Concurrent.Async
import           Control.Exception (evaluate)
import           Control.Monad
import           Control.Monad.IO.Class
import           Control.Monad.Logger
import           Criterion.Measurement
import           Data.ByteString (ByteString)
import           Data.ByteString.Char8 (pack)
import qualified Data.Conduit.Network as CN
import Data.HashMap.Strict (HashMap)
import Data.Store (Store)
import           Data.Store.TypeHash (mkManyHasTypeHash)
import qualified Data.Vector.Unboxed as UV
import           Data.Void
import           Distributed.JobQueue.Client
import           Distributed.JobQueue.Worker
import           Distributed.Stateful
import           Distributed.Stateful.Master
import           Distributed.Types
import           FP.Redis (MonadConnect)
import           System.Environment (getArgs)
import GHC.Generics (Generic)
import Control.DeepSeq
import System.Random
import Control.Monad.Random

type Request = [State]
type Response = Double

data State = State
             { v1 :: !(UV.Vector Double)
             , v2 :: !(UV.Vector Double)
             } deriving (Generic, Show)
instance NFData State
instance Store State
type Input = UV.Vector Double
type Context = Double
type Output = Double

$(mkManyHasTypeHash [ [t| ByteString |]
                    , [t| Double |]
                    , [t| Input |]
                    , [t| State |]
                    , [t| Request |]
                    ])

jqc :: JobQueueConfig
jqc = defaultJobQueueConfig "wq-performance:"

ss :: CN.ServerSettings
ss = CN.serverSettings 0 "*"

-- | Perform some computations.  The only aim is to take about a
-- fraction of a millisecond, which is similar to an update in the
-- distributed particle filter.
myUpdate :: MonadConnect m => Update m State Context Input Output
myUpdate context input State{..} = do
    -- t0 <- liftIO getTime
    let prod = UV.zipWith (*) v1 v2
        result = UV.sum prod
        v1' = UV.map (*UV.sum input) v1
        v2' = UV.map (*context) $ UV.zipWith (/) v1' v2
    state' <- liftIO . evaluate $ State v1' v2'
    -- t1 <- liftIO getTime
    -- liftIO . putStrLn $ "The update took " ++ show (t1 - t0) ++ " seconds."
    return (state', result)


masterArgs :: MonadConnect m => MasterArgs m State Context Input Output
masterArgs = MasterArgs
    { maMaxBatchSize = Just 5
    , maUpdate = myUpdate
    }

-- | Random states (with fixed random generator)
myStates :: [State]
myStates =
    let r = mkStdGen 42
    in concat $ mapM (const (evalRandT go r)) [1..2000 :: Int]
  where
    go :: RandT StdGen [] State
    go = do
        v1 <- UV.generateM 8000 $ \_ -> getRandomR (0,1)
        v2 <- UV.generateM 8000 $ \_ -> getRandomR (0,1)
        return $ State v1 v2

-- | We'll use the same input over and over again.
myInputs :: [Input]
myInputs = [UV.enumFromN 1 20]

runWorker :: forall m void . MonadConnect m
             => Int -- ^ Number of slaves to accept
             -> m void
runWorker n = runJobQueueStatefulWorker jqc ss "127.0.0.1" Nothing masterArgs nmsma action
  where
    nmsma = NMStatefulMasterArgs
        { nmsmaMinimumSlaves = Nothing
        , nmsmaMaximumSlaves = Just n
        , nmsmaSlavesWaitingTime = 1000 * 1000
        }
    action :: MasterHandle m State Context Input Output -> RequestId -> Request -> m (Reenqueue Response)
    action mh _rid req = do
        _ <- resetStates mh req
        finalStates <- foldM (\_ _ -> do

            states <- getStates mh
            -- liftIO . putStrLn $ show (length states) ++ " states: " -- ++ show states
            let inputs = const myInputs <$> states
            newStates <- update mh 5 inputs
            -- stateKeys' <- resetStates mh req
            -- liftIO . putStrLn $ show (length stateKeys') ++ " states"

            -- states' <- getStates mh
            -- liftIO . putStrLn $ show (length states') ++ " states: " -- ++ show states'
            return newStates
            ) undefined [1..20 :: Int]
        closeMaster mh
        return $ DontReenqueue (sum $ sum <$> (finalStates :: HashMap StateId (HashMap StateId Output)))

mainRequest :: IO ()
mainRequest = logErrors $ withJobClient jqc $ \jc -> do
    t0 <- liftIO getTime
    let rid = RequestId . pack . show $ t0
        -- request = [1..100] :: Request
        request = myStates :: Request
    submitRequest jc rid request
    waitForResponse_ jc rid >>= \case
        Just (res :: Response) -> do
            t1 <- liftIO getTime
            liftIO . putStrLn $ "The request took " ++ show (t1 - t0) ++ " seconds."
            liftIO . print $ res
        Nothing ->
            liftIO . putStrLn $ "Failed to get a result."

logErrors :: LoggingT IO a -> IO a
logErrors = runStdoutLoggingT . filterLogger (\_ ll -> ll >= LevelError)

main :: IO ()
main = do
    [nSlaves0] <- getArgs
    initializeTime
    let nSlaves = read nSlaves0 :: Int
    let runWorkers = do
            _ <- mapConcurrently (\n -> do
                                        putStrLn $ "started worker " ++ show n
                                        logErrors $ runWorker nSlaves) [0..nSlaves]
            fail "Workers failed"
    either absurd id <$> race runWorkers mainRequest
