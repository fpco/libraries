{-# OPTIONS_GHC -fno-warn-orphans #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections #-}
import Control.Concurrent.STM
import Control.Monad
import Control.Monad.Logger
import Data.ByteString (ByteString)
import Data.ByteString.Char8 (pack)
import Data.Maybe
import Data.Time
import Data.TypeFingerprint (mkHasTypeFingerprint)
import Distributed.JobQueue.Client.NewApi
import           Control.Exception (assert)
import           Data.Foldable (fold)
import qualified Data.HashMap.Strict as HMS
import           Data.List (sort)
import           Data.Monoid ((<>))
import           Distributed.RedisQueue (RedisInfo)
import           Distributed.Stateful
import           FP.Redis (connectInfo, Seconds(..))
import           System.Environment (getArgs)

$(mkHasTypeFingerprint =<< [t| ByteString |])

type Request = ByteString
type Response = ByteString

type Context = ()
type Input = ByteString
type State = ByteString
type Output = ByteString

mainClient :: IO ()
mainClient = do
  logFunc <- runStdoutLoggingT askLoggerIO
  jc <- newJobClient logFunc defaultJobClientConfig { jccRedisPrefix = "stateful-demo:" }
  -- Use the current time as the request id, to avoid cached results.
  now <- getCurrentTime
  let rid = RequestId (pack (show now))
  let request = ", request input " :: Request
  mresp <- submitRequest jc rid request
  when (isJust mresp) $ fail "Didn't expect cached result"
  stmResponse <- waitForResponse jc rid
  eres <- atomically $ do
    meres <- stmResponse
    case meres of
      Nothing -> retry
      Just eres -> return eres
  print (eres :: Either DistributedJobQueueException Response)

mainWorker :: IO ()
mainWorker = do
    logFunc <- runStdoutLoggingT askLoggerIO
    runWorker args logFunc slaveFunc masterFunc
  where
    args = WorkerArgs
      { waConfig = defaultWorkerConfig "stateful-demo:" (connectInfo "localhost") "localhost"
      , waMasterArgs = MasterArgs
        { maMaxBatchSize = Just 5
        , maMinBatchSize = Nothing
        }
      , waRequestSlaveCount = 2
      , waMasterWaitTime = Seconds 10
      }

slaveFunc :: Context -> Input -> State -> IO (State, Output)
slaveFunc () input state = return (output, output)
  where
    output = state <> input

masterFunc :: RedisInfo -> RequestId -> Request -> MasterHandle State Context Input Output -> IO Response
masterFunc _ _ req mh = do
  assignments <- resetStates mh ["initial state 1", "initial state 2"]
  results <- update mh () (HMS.fromList (map (, [", first input"]) (HMS.keys assignments)))
  let outputs = fold results
  assert (sort (HMS.elems outputs) == sort ["initial state 1, first input", "initial state 2, first input"]) (return ())
  results' <- update mh () (HMS.fromList (map (, [req, ", multiplicity! "]) (HMS.keys outputs)))
  return $ fold $ sort $ HMS.elems $ fold results'

main :: IO ()
main = do
  [mode] <- getArgs
  case mode of
    "client" -> mainClient
    "worker" -> mainWorker
    _ -> fail "bad usage"
