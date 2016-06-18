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
import           Control.Exception (evaluate)
import           Control.Monad.IO.Class
import           Control.Monad.Logger
import           Control.Monad.Random
import           Criterion.Measurement
import           Data.ByteString (ByteString)
import           Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as HMS
import qualified Data.HashSet as HS
import           Data.Store (Store)
import           Data.Store.Streaming
import           Data.Store.TypeHash (mkManyHasTypeHash)
import qualified Data.Vector as V
import           Distributed.Stateful
import           Distributed.Stateful.Master
import           FP.Redis (MonadConnect)
import           System.Environment (getArgs)
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
myUpdate :: MonadConnect m => Update m State Context Input Output
myUpdate context input (!v1, !v2) = do
    let prod = V.zipWith (*) v1 v2
        result = V.sum prod
        v1' = V.map (*V.sum input) v1
        v2' = V.map (*context) $ V.zipWith (/) v1' v2
    state' <- liftIO . evaluate $ result `deepseq` v1' `deepseq` v2' `deepseq` (v1', v2')
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
        v1 <- V.generateM 3000 $ \_ -> getRandomR (0,1)
        v2 <- V.generateM 3000 $ \_ -> getRandomR (0,1)
        -- v1 `seq` v2 `seq` return $ State v1 v2
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
                        ) [1..20::Int]
    return (sum $ sum <$> (head finalStates :: HashMap StateId (HashMap StateId Output)))

doRequest :: Int -> String -> IO ()
doRequest nSlaves mode = do
    t0 <- getTime
    res <- logErrors $ case mode of
        "stm" -> runSimplePureStateful masterArgs nSlaves (myAction myStates)
        "nm" -> runSimpleNMStateful "127.0.0.1" masterArgs nSlaves (myAction myStates)
        _ -> error "Arguments should be <nSlaves> [stm|nm]"
    t1 <- getTime
    putStrLn $ "The request took " ++ show (t1 - t0) ++ " seconds."
    print res


main :: IO ()
main = do
    [nSlaves0, mode] <- getArgs
    initializeTime
    doRequest (read nSlaves0) mode
