{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections #-}

import           Control.Exception (assert)
import           Control.Monad.Logger
import           Data.ByteString (ByteString)
import           Data.Foldable (fold)
import qualified Data.HashMap.Strict as HMS
import           Data.List (sort)
import           Data.Monoid ((<>))
import           Data.TypeFingerprint (mkHasTypeFingerprint)
import           Distributed.RedisQueue (RedisInfo, RequestId)
import           Distributed.Stateful
import           FP.Redis (connectInfo, Seconds(..))

$(mkHasTypeFingerprint =<< [t| ByteString |])

main :: IO ()
main = do
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

type Request = ByteString
type Response = ByteString

type Context = ()
type Input = ByteString
type State = ByteString
type Output = ByteString

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
