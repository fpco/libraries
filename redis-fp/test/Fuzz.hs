{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE FlexibleContexts #-}

module Main where

import ClassyPrelude
import Test.Hspec (it, hspec)
import Test.Hspec.Core.Spec (SpecM, runIO)
import FP.Redis (Connection, VKey(..), Key(..))
import qualified Data.Map as M
import qualified FP.Redis as R
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BS8
import Control.Monad.Trans.Control
import Control.Monad.Trans.Reader (ReaderT(..))
import Control.Monad.Logger (runStdoutLoggingT, LoggingT, MonadLogger)
import Control.Monad.Base (MonadBase)
import Prelude (read)
import qualified Control.Concurrent.Async.Lifted.Safe as Async
import Control.Monad.Reader (asks)
import System.Environment (lookupEnv)
import qualified Data.UUID as UUID
import qualified Data.UUID.V4 as UUID.V4
import Control.Monad.Trans.State (evalStateT, StateT)
import qualified Data.Random as Rand
import qualified Data.Random.Source.PureMT as Rand

main :: IO ()
main = hspec $ do
  it "passes test using incr and get commands (light)" (fuzzTest 5 100 100)
  flakyTest $ stressfulTest $ it "passes test using incr and get commands (heavy)" (fuzzTest 50 1000 1000)

fuzzTest :: Int -> Int -> Int -> IO ()
fuzzTest maxConns threads runs = runFuzzM maxConns $ do
  clearRedisKeys
  -- Fork 100 threads, and have each perform 100 actions
  void $ flip Async.mapConcurrently [1..threads] $ \n -> do
    flip evalStateT (Rand.pureMT (fromIntegral n)) $ do
      forM_ [1..runs] $ \(_ :: Int) -> do
        which <- stdUniformSample
        void $ if which
          then incr =<< selectOrGenerateKey
          else getAndCheck =<< selectOrGenerateKey

uniformSample :: (Rand.Distribution Rand.Uniform a) => a -> a -> StateT Rand.PureMT FuzzM a
uniformSample lo hi = Rand.sample (Rand.uniform lo hi)

stdUniformSample :: (Rand.Distribution Rand.StdUniform a) => StateT Rand.PureMT FuzzM a
stdUniformSample = Rand.sample Rand.stdUniform

-- Implementation of FuzzM

newtype FuzzM a = FuzzM { unFuzzM :: ReaderT State (LoggingT IO) a }
  deriving (Functor, Applicative, Monad, MonadBase IO, MonadReader State, MonadIO, MonadMask, MonadLogger, MonadCatch, MonadThrow)

instance MonadBaseControl IO FuzzM where
  type StM FuzzM a = a
  liftBaseWith f = FuzzM $ liftBaseWith $ \q -> f (q . unFuzzM)
  restoreM = FuzzM . restoreM

data State = State
  { stateRedis :: IORef (M.Map Key Value)
    -- ^ State of redis key-value store.
  , stateConn :: R.ManagedConnection
  }

-- For now, all values are Ints that support incrementing.
data Value = Value
  { curValue :: Int64
  , incrsInFlight :: Int64
  }

instance Semigroup Value where
  Value v1 i1 <> Value v2 i2 = Value (v1 + v2) (i1 + i2)

instance Monoid Value where
  mempty = Value 0 0
  mappend = (<>)

minPossibleValue, maxPossibleValue :: Value -> Int64
minPossibleValue = curValue
maxPossibleValue v = curValue v + incrsInFlight v

runFuzzM :: forall a. Int -> FuzzM a -> IO a
runFuzzM maxConns f = runStdoutLoggingT $ do
  stateRedis <- newIORef M.empty
  R.withManagedConnection (R.connectInfo "localhost") maxConns $ \stateConn ->
    runReaderT (unFuzzM f) (State {..})

incr :: Key -> StateT Rand.PureMT FuzzM Int64
incr key = do
  mpRef <- asks stateRedis
  bracket
    (atomicModifyIORef' mpRef $ \mp ->
      let mp' = M.insertWithKey' (\_ -> (<>)) key (Value 0 1) mp
       in (mp', ()))
    (\() -> atomicModifyIORef' mpRef $ \mp ->
      let mp' = M.insertWithKey' (\_ -> (<>)) key (Value 1 (-1)) mp
       in (mp', ()))
    (\() -> lift (withConnection (\conn -> R.runCommand conn (R.incr (VKey key)))))

getAndCheck :: Key -> StateT Rand.PureMT FuzzM Int64
getAndCheck key = do
  before <- readValue key
  actual <- maybe 0 (read . BS8.unpack) <$> lift (withConnection (\conn -> R.runCommand conn (R.get (VKey key))))
  after <- readValue key
  let lower = minPossibleValue before
      upper = maxPossibleValue after
  when (actual < lower || actual > upper) $ do
    fail $ "Expected value in range [" ++ show lower ++ ", " ++ show upper ++ "], but got " ++ show actual
  -- when (actual /= lower || actual /= upper) $ liftIO $ do
  --   putStrLn "===="
  --   print (lower, actual, upper)
  --   putStrLn "===="
  return actual

readValue :: Key -> StateT Rand.PureMT FuzzM Value
readValue key =
  asks stateRedis >>=
  fmap (fromMaybe (Value 0 0) . M.lookup key) . liftIO . readIORef

selectOrGenerateKey :: StateT Rand.PureMT FuzzM Key
selectOrGenerateKey = do
  mpRef <- asks stateRedis
  let randomKey = Key . (testPrefix <>) <$> liftIO randomShortBS
  makeKey <- (0 ==) <$> uniformSample (0 :: Int) 10
  if makeKey
    then randomKey
    else do
      mp <- readIORef mpRef
      if M.null mp
        then randomKey
        else do
          ix <- uniformSample 0 (M.size mp - 1)
          return $ fst (M.elemAt ix mp)

-- Random utils

randomShortBS :: IO BS.ByteString
randomShortBS = do
  UUID.toASCIIBytes <$> UUID.V4.nextRandom

-- Redis utils

testPrefix :: BS.ByteString
testPrefix = "redis-fp.fuzz."

withConnection :: (Connection -> FuzzM a) -> FuzzM a
withConnection cont = do
  conn <- asks stateConn
  R.useConnection conn cont

clearRedisKeys :: FuzzM ()
clearRedisKeys = do
  withConnection $ \redis -> do
    R.runCommand redis R.flushall

-- | Does not run the action if we have NO_STRESSFUL=1 in the env
stressfulTest :: SpecM a () -> SpecM a ()
stressfulTest m = do
    mbS <- runIO (lookupEnv "NO_STRESSFUL")
    case mbS of
        Just "1" -> return ()
        _ -> m

-- | Only runs the test if we have FLAKY=1 in the env
flakyTest :: SpecM a () -> SpecM a ()
flakyTest m = do
    mbS <- runIO (lookupEnv "FLAKY")
    case mbS of
        Just "1" -> m
        _ -> return ()
