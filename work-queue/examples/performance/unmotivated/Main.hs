{-# OPTIONS_GHC -fno-warn-orphans #-}
{-# LANGUAGE NoImplicitPrelude #-}
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

import           ClassyPrelude
import           Control.DeepSeq
import           Control.Exception (evaluate)
import           Control.Monad.Random
import           Criterion.Measurement
import qualified Data.HashMap.Strict as HMS
import qualified Data.HashSet as HS
import           Data.Store (Store)
import           Data.Store.Streaming
import           Data.Store.TypeHash (mkManyHasTypeHash)
import qualified Data.Vector as V
import           Distributed.JobQueue.Client
import           Distributed.Stateful
import           Distributed.Stateful.Master
import           FP.Redis (MonadConnect)
import qualified Options.Applicative as OA
import           PerformanceUtils
import qualified System.IO.ByteBuffer as BB

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

roundtrip :: forall a m . (MonadConnect m, Store a) => a -> m a
roundtrip x = BB.with (Just $ 64 * 1024) $ \bb -> do
    BB.copyByteString bb (encodeMessage . Message $ x)
    mm <- decodeMessage bb (return Nothing) :: m (Maybe (Message a))
    return (maybe (error "could not decode") fromMessage mm :: a)

-- | Perform some computations.  The only aim is to take about a
-- fraction of a millisecond, which is similar to an update in the
-- distributed particle filter.
myUpdate :: MonadConnect m => Options -> Update m State Context Input Output
myUpdate Options{..} context input (!v, !v') = do
    foo <- forM [1..100::Double] $ \i -> do
        let prod = V.zipWith (\x y -> x*y*i) v v'
            result = V.sum prod
            v1' = V.map (*(i*V.sum input)) v
            v2' = V.map (*(context*i)) $ V.zipWith (/) v1' v'
        state' <- liftIO . evaluate $ result `deepseq` v1' `deepseq` v2' `deepseq` (v1', v2')
        return (state', result)
    return $! foldl' (\((v1,v2),x) ((v1',v2'),x') -> ((V.zipWith (+) v1 v1',V.zipWith (+) v2 v2'),x+x')) ((V.replicate optVLength 0, V.replicate optVLength 0),0) foo


masterArgs :: MonadConnect m => Options -> MasterArgs m State Context Input Output
masterArgs opts = MasterArgs
    { maMaxBatchSize = Just 5
    , maUpdate = myUpdate opts
    }

-- | Random states (with fixed random generator)
myStates :: Options -> [State]
myStates Options{..} =
    let r = mkStdGen 42
    in concat $ mapM (const (evalRandT go r)) [1..200 :: Int]
  where
    go :: RandT StdGen [] State
    go = do
        v1 <- V.generateM optVLength $ \_ -> getRandomR (0,1)
        v2 <- V.generateM optVLength $ \_ -> getRandomR (0,1)
        v1 `deepseq` v2 `deepseq` return (v1, v2)

-- | We'll use the same input over and over again.
myInputs :: [Input]
myInputs = [V.enumFromN 1 20]

myAction :: MonadConnect m => Request -> MasterHandle m State Context Input Output -> m Response
myAction req mh = do
    t00 <- liftIO getTime
    _ <- resetStates mh req
    t01 <- liftIO getTime
    liftIO . putStrLn $ unwords ["resetStates:", pack $ show (t01 - t00)]
    finalStates <- mapM (\_ -> do
                                t0 <- liftIO getTime
                                stateIds <- getStateIds mh
                                t1 <- liftIO getTime
                                let inputs = HMS.fromList $ zip (HS.toList stateIds) (repeat myInputs)
                                newStates <- update mh 5 inputs
                                t2 <- liftIO getTime
                                liftIO . putStrLn $ unlines [ "getStateIds: ", pack $ show (t1 - t0)
                                                            , "update   : ", pack $ show (t2 - t1)]
                                return newStates
                        ) [1..5::Int]
    return (sum $ sum <$> (unsafeHead finalStates :: HashMap StateId (HashMap StateId Output)))

jqc :: JobQueueConfig
jqc = defaultJobQueueConfig "perf:unmotivated:"

data Options = Options
               { optNoNetworkMessage :: Bool
               , optVLength :: Int
               , optNSlaves :: Int
               , optSpawnWorker :: Bool
               }

options :: OA.Parser Options
options = Options
    <$> OA.switch (OA.long "no-network-message"
                   `mappend` OA.help "Run in a single process, communicating via STM (instead of using NetworkMessage")
    <*> (OA.option OA.auto (OA.short 'N'
                            `mappend` OA.help "length of vectors")
         OA.<|> pure 1000)
    <*> OA.option OA.auto (OA.long "nslaves"
                           `mappend` OA.short 'n'
                           `mappend` OA.help "Number of slave nodes")
    <*> OA.switch (OA.long "spawn-worker"
                   `mappend` OA.help "Used internally to spawn a worker")


main :: IO ()
main = do
    opts <- OA.execParser
        (OA.info (OA.helper <*> options)
         (OA.fullDesc
          `mappend` OA.progDesc "Run a benchmark that is not motivated by anything, just performs some calculations with vectors."))
    let reqParas = ( "unmotivated-bench.csv"
                   , [ ("NetworkMessage", pack . show . not . optNoNetworkMessage $ opts)
                     , ("N", pack . show . optVLength $ opts)
                     ])
    logErrors $
        if optNoNetworkMessage opts
        then runWithoutNM reqParas (masterArgs opts) (optNSlaves opts) myAction (return $ myStates opts)
        else runWithNM reqParas jqc (optSpawnWorker opts) (masterArgs opts) (optNSlaves opts) myAction (return $ myStates opts)
