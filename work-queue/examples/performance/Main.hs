{-# OPTIONS_GHC -fno-warn-orphans #-}
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

import           Control.DeepSeq
import           Control.Exception (evaluate, bracket)
import           Control.Monad (forM)
import           Control.Monad.IO.Class
import           Control.Monad.Logger
import           Control.Monad.Random
import           Criterion.Measurement
import           Data.ByteString (ByteString)
import           Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as HMS
import qualified Data.HashSet as HS
import           Data.List (foldl')
import           Data.Store (Store)
import           Data.Store.Streaming
import           Data.Store.TypeHash (mkManyHasTypeHash)
import qualified Data.Vector as V
import           Distributed.Stateful
import           Distributed.Stateful.Master
import           FP.Redis (MonadConnect)
import           System.Environment (getArgs)
import qualified System.IO.ByteBuffer as BB
import           System.Environment (getExecutablePath)
import Distributed.JobQueue.Client
import Distributed.JobQueue.Worker
import qualified Data.Conduit.Network as CN
import Control.Concurrent.Mesosync.Lifted.Safe
import System.Process

type Request = [State]
type Response = Double
type State = ( V.Vector Double
             , V.Vector Double
             )
type Input = V.Vector Double
type Context = Double
type Output = Double

$(mkManyHasTypeHash [ [t| ByteString |]
                    , [t| Double |]
                    , [t| Input |]
                    , [t| Request |]
                    ])

vlength :: Int
vlength = 1000

roundtrip :: forall a m . (MonadConnect m, Store a) => a -> m a
roundtrip x = BB.with (Just $ 64 * 1024) $ \bb -> do
    BB.copyByteString bb (encodeMessage . Message $ x)
    mm <- decodeMessage bb (return Nothing) :: m (Maybe (Message a))
    return (maybe (error "could not decode") fromMessage mm :: a)

-- | Perform some computations.  The only aim is to take about a
-- fraction of a millisecond, which is similar to an update in the
-- distributed particle filter.
myUpdate :: MonadConnect m => Update m State Context Input Output
myUpdate context input (!v1, !v2) = do
    foo <- forM [1..100::Double] $ \i -> do
        let prod = V.zipWith (\x y -> x*y*i) v1 v2
            result = V.sum prod
            v1' = V.map (*(i*V.sum input)) v1
            v2' = V.map (*(context*i)) $ V.zipWith (/) v1' v2
        state' <- liftIO . evaluate $ result `deepseq` v1' `deepseq` v2' `deepseq` (v1', v2')
        return (state', result)
    return $! foldl' (\((v1,v2),x) ((v1',v2'),x') -> ((V.zipWith (+) v1 v1',V.zipWith (+) v2 v2'),x+x')) ((V.replicate vlength 0, V.replicate vlength 0),0) foo


masterArgs :: MonadConnect m => MasterArgs m State Context Input Output
masterArgs = MasterArgs
    { maMaxBatchSize = Just 5
    , maUpdate = myUpdate
    }

-- | Random states (with fixed random generator)
myStates :: [State]
myStates =
    let r = mkStdGen 42
    in concat $ mapM (const (evalRandT go r)) [1..200 :: Int]
  where
    go :: RandT StdGen [] State
    go = do
        v1 <- V.generateM vlength $ \_ -> getRandomR (0,1)
        v2 <- V.generateM vlength $ \_ -> getRandomR (0,1)
        v1 `deepseq` v2 `deepseq` return (v1, v2)

-- | We'll use the same input over and over again.
myInputs :: [Input]
myInputs = [V.enumFromN 1 20]

logErrors :: LoggingT IO a -> IO a
logErrors = runStdoutLoggingT . filterLogger (\_ ll -> ll >= LevelError)

myAction :: MonadConnect m => Request -> MasterHandle m State Context Input Output -> m Response
myAction req mh = do
    t00 <- liftIO getTime
    _ <- resetStates mh req
    t01 <- liftIO getTime
    liftIO . putStrLn $ unwords ["resetStates:", show (t01 - t00)]
    finalStates <- mapM (\_ -> do
                                t0 <- liftIO getTime
                                stateIds <- getStateIds mh
                                t1 <- liftIO getTime
                                let inputs = HMS.fromList $ zip (HS.toList stateIds) (repeat myInputs)
                                newStates <- update mh 5 inputs
                                t2 <- liftIO getTime
                                liftIO . putStrLn $ unlines [ "getStateIds: " ++ show (t1 - t0)
                                                            , "update   : " ++ show (t2 - t1)]
                                return newStates
                        ) [1..5::Int]
    return (sum $ sum <$> (head finalStates :: HashMap StateId (HashMap StateId Output)))

-- * functionality to spawn workers in separate processed, similar as in the dpf.

spawnAndWaitForWorker :: Int -> IO ProcessHandle
spawnAndWaitForWorker numSlaves = do
  program <- getExecutablePath
  runProcess program [show numSlaves, "nmSlave"] Nothing Nothing Nothing Nothing Nothing

jqc :: JobQueueConfig
jqc = defaultJobQueueConfig "performance:"

performRequest :: IO ()
performRequest = logErrors $ withJobClient jqc $ \jc -> do
    t0 <- liftIO getTime
    rid <- uniqueRequestId
    submitRequest jc rid myStates
    mRes <- waitForResponse_ jc rid
    t1 <- liftIO getTime
    liftIO . putStrLn $ "The request took " ++ show (t1 - t0) ++ " seconds."
    liftIO $ print (mRes :: Maybe Response)

main :: IO ()
main = do
    initializeTime
    args <- getArgs
    print args
    case args of
        [numSlaves, "nmSlave"] -> do -- spawn nm slave
            putStrLn "spawning worker"
            let ss = CN.serverSettings 3333 "*"
            logErrors $ runJobQueueStatefulWorker
                jqc
                ss
                "127.0.0.1"
                Nothing
                masterArgs
                (NMStatefulMasterArgs (Just $ read numSlaves) (Just $ read numSlaves) (1000 * 1000))
                (\mh _rid req -> DontReenqueue <$> myAction req mh)
        [numSlaves, "nm"] -> do -- run calculation
            bracket
                (mapConcurrently (const (spawnAndWaitForWorker (read numSlaves))) [0..(read numSlaves :: Int)])
                (mapM terminateProcess)
                (\_ -> performRequest)
        [numSlaves, "stm"] -> do
            t0 <- getTime
            res <- logErrors $
                runSimplePureStateful masterArgs (read numSlaves) (myAction myStates)
            t1 <- getTime
            putStrLn $ "The request took " ++ show (t1 - t0) ++ " seconds."
            print res
        _ -> putStrLn "Wrong arguments. Arguments should be <nSlaves> [stm|nm]"
