{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}

module Main where

import ClassyPrelude
import Test.Hspec (it, hspec)
import Test.Hspec.Core.Spec (SpecM, runIO)
import System.Random
import FP.Redis (Connection, VKey(..), Key(..))
import qualified Data.Map as M
import qualified FP.Redis as R
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BS8
import Control.Monad.Trans.Control
import Control.Monad.Trans.Reader (ReaderT(..))
import qualified Data.List.NonEmpty as LNE
import Control.Monad.Logger (runStdoutLoggingT, LoggingT, MonadLogger)
import Control.Monad.Base (MonadBase)
import Prelude (read)
import qualified Control.Concurrent.Async.Lifted.Safe as Async
import Control.Monad.Reader (asks)
import System.Environment (lookupEnv)

main :: IO ()
main = hspec $ do
  it "passes test using incr and get commands (light)" (fuzzTest 5 100 100)
  stressfulTest $ it "passes test using incr and get commands (heavy)" (fuzzTest 50 1000 1000)

fuzzTest :: Int -> Int -> Int -> IO ()
fuzzTest maxConns threads runs = runFuzzM maxConns $ do
  clearRedisKeys
  -- Fork 100 threads, and have each perform 100 actions
  void $ flip Async.mapConcurrently [1..(threads :: Int)] $ \_ -> do
    forM_ [1..runs] $ \(_ :: Int) -> do
      which <- liftIO randomIO
      void $ if which
        then incr =<< selectOrGenerateKey
        else getAndCheck =<< selectOrGenerateKey

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

incr :: Key -> FuzzM Int64
incr key = do
  mpRef <- asks stateRedis
  bracket
    (atomicModifyIORef' mpRef $ \mp ->
      let mp' = M.insertWithKey' (\_ -> (<>)) key (Value 0 1) mp
       in (mp', ()))
    (\() -> atomicModifyIORef' mpRef $ \mp ->
      let mp' = M.insertWithKey' (\_ -> (<>)) key (Value 1 (-1)) mp
       in (mp', ()))
    (\() -> withConnection (\conn -> R.runCommand conn (R.incr (VKey key))))

getAndCheck :: Key -> FuzzM Int64
getAndCheck key = do
  before <- readValue key
  actual <- maybe 0 (read . BS8.unpack) <$> withConnection (\conn -> R.runCommand conn (R.get (VKey key)))
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

readValue :: Key -> FuzzM Value
readValue key =
  asks stateRedis >>=
  fmap (fromMaybe (Value 0 0) . M.lookup key) . liftIO . readIORef

selectOrGenerateKey :: FuzzM Key
selectOrGenerateKey = do
  mpRef <- asks stateRedis
  let randomKey = Key . (testPrefix <>) <$> randomShortBS
  liftIO $ do
    makeKey <- (0 ==) <$> randomRIO (0, 10 :: Int)
    if makeKey
      then randomKey
      else do
        mp <- readIORef mpRef
        if M.null mp then randomKey else do
          ix <- randomRIO (0, M.size mp - 1)
          return $ fst (M.elemAt ix mp)

-- Random utils

randomShortBS :: IO BS.ByteString
randomShortBS = do
  l <- randomRIO (0, 16)
  BS8.pack <$> replicateM l randomIO

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
    matches <- liftIO $ R.runCommand redis $ R.keys (testPrefix <> "*")
    liftIO $ mapM_ (R.runCommand_ redis . R.del) (LNE.nonEmpty matches)

-- | Does not run the action if we have NO_STRESSFUL=1 in the env
stressfulTest :: SpecM a () -> SpecM a ()
stressfulTest m = do
    mbS <- runIO (lookupEnv "NO_STRESSFUL")
    case mbS of
        Just "1" -> return ()
        _ -> m
