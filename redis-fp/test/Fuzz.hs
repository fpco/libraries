{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import Test.Hspec (it, hspec)
import System.Random
import qualified Data.Vector.Mutable as MV
import FP.Redis (Connection, VKey(..), Key(..))
import qualified Data.Map as M
import qualified FP.Redis as R
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BS8
import Control.Monad.Trans.Control
import Control.Exception (bracket)
import Control.Monad.Trans.Reader (ReaderT(..))
import Control.Monad.Reader.Class (MonadReader, asks)
import Control.Concurrent.Lifted (fork)
import Data.Monoid ((<>))
import qualified Data.List.NonEmpty as LNE
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad (replicateM, when, void)
import Control.Monad.Logger (runStdoutLoggingT)
import Data.Traversable (forM)
import Data.Foldable (mapM_, forM_)
import Prelude hiding (mapM, mapM_)
import Data.IORef
import Data.Int (Int64)
import Data.Maybe (fromMaybe)
import Control.Monad.Base (MonadBase)
import Control.Concurrent.MVar.Lifted

main :: IO ()
main = hspec $ it "passes test using incr and get commands" fuzzTest

fuzzTest :: IO ()
fuzzTest = runFuzzM 5 $ do
  clearRedisKeys
  -- Fork 100 threads, and have each perform 100 actions
  doneVars <- forM [1..(100 :: Int)] $ \_ -> do
    done <- newEmptyMVar
    void $ fork $ do
      forM_ [1..100] $ \(_ :: Int) -> do
        which <- liftIO randomIO
        if which
          then incr =<< selectOrGenerateKey
          else getAndCheck =<< selectOrGenerateKey
      putMVar done ()
    return done
  mapM_ takeMVar doneVars

-- Implementation of FuzzM

newtype FuzzM a = FuzzM { unFuzzM :: ReaderT State IO a }
  deriving (Functor, Applicative, Monad, MonadBase IO, MonadReader State, MonadIO)

instance MonadBaseControl IO FuzzM where
  type StM FuzzM a = a
  liftBaseWith f = FuzzM $ liftBaseWith $ \q -> f (q . unFuzzM)
  restoreM = FuzzM . restoreM

data State = State
  { stateRedis :: IORef (M.Map Key Value)
    -- ^ State of redis key-value store.
  , stateConns :: MV.IOVector (Maybe Connection)
    -- ^ Redis connections
  }

-- For now, all values are Ints that support incrementing.
data Value = Value
  { curValue :: Int64
  , incrsInFlight :: Int64
  }

instance Monoid Value where
  mempty = Value 0 0
  mappend (Value v1 i1) (Value v2 i2) = Value (v1 + v2) (i1 + i2)

minPossibleValue, maxPossibleValue :: Value -> Int64
minPossibleValue = curValue
maxPossibleValue v = curValue v + incrsInFlight v

runFuzzM :: Int -> FuzzM a -> IO a
runFuzzM maxConns f = do
  stateRedis <- newIORef M.empty
  stateConns <- MV.replicate maxConns Nothing
  runReaderT (unFuzzM f) (State {..})

incr :: Key -> FuzzM Int64
incr key = do
  mpRef <- asks stateRedis
  conn <- getConnection
  liftIO $ bracket
    (atomicModifyIORef' mpRef $ \mp ->
      let mp' = M.insertWithKey' (\_ -> (<>)) key (Value 0 1) mp
       in (mp', ()))
    (\() -> atomicModifyIORef' mpRef $ \mp ->
      let mp' = M.insertWithKey' (\_ -> (<>)) key (Value 1 (-1)) mp
       in (mp', ()))
    (\() -> R.runCommand conn $ R.incr (VKey key))

getAndCheck :: Key -> FuzzM Int64
getAndCheck key = do
  conn <- getConnection
  before <- readValue key
  actual <- maybe 0 (read . BS8.unpack) <$> liftIO (R.runCommand conn (R.get (VKey key)))
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

getConnection :: FuzzM Connection
getConnection = do
  conns <- asks stateConns
  liftIO $ do
    ix <- randomRIO (0, MV.length conns - 1)
    mconn <- MV.read conns ix
    case mconn of
      Nothing -> do
        conn <- runStdoutLoggingT $ R.connect (R.connectInfo "localhost")
        MV.write conns ix (Just conn)
        return conn
      Just conn -> return conn

clearRedisKeys :: FuzzM ()
clearRedisKeys = do
  redis <- getConnection
  matches <- liftIO $ R.runCommand redis $ R.keys (testPrefix <> "*")
  liftIO $ mapM_ (R.runCommand_ redis . R.del) (LNE.nonEmpty matches)
