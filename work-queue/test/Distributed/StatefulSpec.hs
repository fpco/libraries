{-# LANGUAGE TupleSections #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ParallelListComp #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
module Distributed.StatefulSpec (spec) where

import           ClassyPrelude
import           Control.Concurrent.Async
import           Control.Concurrent.STM (retry, check)
import           Control.DeepSeq (NFData)
import           Control.Exception (BlockedIndefinitelyOnSTM(..))
import           Control.Monad.Logger
import           Data.Binary (Binary)
import           Data.Conduit.Network (serverSettings, clientSettings)
import qualified Data.Conduit.Network as CN
import qualified Data.HashMap.Strict as HMS
import qualified Data.HashSet as HS
import           Data.Streaming.NetworkMessage
import qualified Data.Streaming.NetworkMessage as NM
import           Distributed.JobQueue.Client.NewApi
import           Distributed.RedisQueue (RedisInfo)
import           Distributed.Stateful
import           Distributed.Stateful.Internal (StateId(..))
import           Distributed.Stateful.Master
import           Distributed.Stateful.Slave
import           Distributed.TestUtil
import           FP.Redis
import           Test.Hspec (shouldBe)
import qualified Test.Hspec as Hspec
import           Test.QuickCheck hiding (output)

spec :: Hspec.Spec
spec = do
    it "Sends data around properly" $ do
        let slaveUpdate :: () -> String -> [Either Int String] -> IO ([Either Int String], String)
            slaveUpdate _context input state =
                return (Right input : state, input)
        let initialStates :: [[Either Int String]]
            initialStates = map ((:[]) . Left) [1..4]
        runMasterAndSlaves 7000 4 slaveUpdate initialStates $ \mh -> do
            states <- getStates mh
            print states
            sids <- getStateIds mh
            let inputs = HMS.fromList $ map (, ["input 1"]) $ HS.toList sids
            outputs <- update mh () inputs
            print outputs
            -- states `shouldBe` (HMS.map (\(_input, initial) -> [Right "step 1", Left initial]) inputsy)
    Hspec.it "Passes quickcheck comparison with pure implementation" $
      property $ forAll arbitrary $
      \( Blind (function :: Context -> Input -> State -> (State, Output))
       , initialStates :: [State]
       , updates :: [(Context, [[Input]])]
       ) -> ioProperty $ do
         runMasterAndSlaves 7000 4 (\c i s -> return (function c i s)) initialStates $ \mh -> do
           let go :: PureState State -> (Context, [[Input]]) -> IO (PureState State)
               go ps (ctx, inputs) = do
                 let sids' = sort (HMS.keys (pureStates ps))
                 let inputMap = HMS.fromList (zip sids' inputs)
                 let (ps', outputs') = pureUpdate function ctx inputMap ps
                 -- putStrLn "===="
                 -- print ctx
                 -- print inputMap
                 -- print ps
                 -- print ps'
                 sids <- getStateIds mh
                 sort (HS.toList sids) `shouldBe` sids'
                 curStates <- getStates mh
                 curStates `shouldBe` pureStates ps
                 outputs <- update mh ctx inputMap
                 -- print outputs
                 -- print outputs'
                 outputs `shouldBe` outputs'
                 return ps'
           void $ foldM go (initialPureState initialStates) (take 4 updates)
         return True
    Hspec.it "Integrates with job-queue" $ do
        clearRedisKeys
        let args = WorkerArgs
              { waConfig = defaultWorkerConfig redisTestPrefix localhost "localhost"
              , waMasterArgs = MasterArgs Nothing (Just 5)
              , waRequestSlaveCount = 2
              , waMasterWaitTime = Seconds 1
              }
        logFunc <- runStdoutLoggingT askLoggerIO
        let slaveFunc (Context x) (Input y) (State z) = return (State (z + y), Output (z + x))
        let masterFunc :: RedisInfo -> RequestId -> Context -> MasterHandle State Context Input Output -> IO (HMS.HashMap StateId (HMS.HashMap StateId Output))
            masterFunc _redis _rid r mh = do
              sids <- HMS.keys <$> resetStates mh (map State [1..10])
              update mh r (HMS.fromList (zip sids (map ((:[]) . Input) [1..])))
        void $ mapConcurrently (\_ -> runWorker args logFunc slaveFunc masterFunc) [1..2]
          `race` do
            jc <- newJobClient logFunc defaultJobClientConfig
                { jccRedisPrefix = redisTestPrefix }
            fetchOutput <- submitRequest jc (RequestId "request") (Context 1)
            res <- atomically $ do
                mres <- fetchOutput
                case mres of
                    Just res -> return res
                    Nothing -> retry
            putStrLn "=========================================="
            print (res :: Either DistributedJobQueueException (HMS.HashMap StateId (HMS.HashMap StateId Output)))
            putStrLn "=========================================="

it :: String -> IO () -> Hspec.Spec
it name f = Hspec.it name f

runMasterAndSlaves
    :: forall state context input output a.
       (NFData state, Binary state, Binary context, Binary input, NFData output, Binary output, Show state)
    => Int
    -> Int
    -> (context -> input -> state -> IO (state, output))
    -> [state]
    -> (MasterHandle state context input output -> IO a)
    -> IO a
runMasterAndSlaves port slaveCnt slaveUpdate initialStates inner = do
    nms <- nmsSettings
    logFunc <- runStdoutLoggingT askLoggerIO
    -- Running slaves
    let slaveArgs = SlaveArgs
            { saUpdate = slaveUpdate
            , saInit = return ()
            , saNMSettings = nms
            , saClientSettings = clientSettings port "localhost"
            , saLogFunc = logFunc
            }
    let runSlaves = mapConcurrently (\_ -> runSlave slaveArgs) (replicate slaveCnt () :: [()])
    -- Running master
    let masterArgs = MasterArgs
            { maMinBatchSize = Nothing
            , maMaxBatchSize = Just 5
            }
    mh <- mkMasterHandle masterArgs logFunc
    masterReady <- newEmptyMVar
    someConnected <- newEmptyMVar
    doneVar <- newEmptyMVar
    let ss = CN.setAfterBind (\_ -> tryPutMVar masterReady () >> return ()) (serverSettings port "*")
    let acceptConns =
            -- timeout (1000 * 1000 * 2) $
            CN.runGeneralTCPServer ss $ NM.generalRunNMApp nms (const "") (const "") $ \nm -> do
                addSlaveConnection mh nm
                void $ tryPutMVar someConnected ()
                readMVar doneVar
    withAsync ((takeMVar masterReady >> runSlaves) `concurrently` acceptConns) $ \_ -> do
        atomically (check . (> 0) =<< getSlaveCount mh)
          `catch` \BlockedIndefinitelyOnSTM -> fail "No slaves connected"
        void $ resetStates mh initialStates
        r <- inner mh
        putMVar doneVar ()
        return r

newtype Context = Context Int deriving (CoArbitrary, Arbitrary, Show, Binary, Eq)
newtype Input = Input Int deriving (CoArbitrary, Arbitrary, Show, Binary, Eq)
newtype State = State Int deriving (CoArbitrary, Arbitrary, Show, Binary, Eq, NFData)
newtype Output = Output Int deriving (CoArbitrary, Arbitrary, Show, Binary, Eq, NFData)

data PureState state = PureState
    { pureStates :: HMS.HashMap StateId state
    , pureIdCounter :: Int
    } deriving (Show)

initialPureState :: [state] -> PureState state
initialPureState states = PureState
    { pureStates = HMS.fromList $ zip (map StateId [0..]) states
    , pureIdCounter = length states
    }

pureUpdate :: forall state context input output.
              (context -> input -> state -> (state, output))
           -> context
           -> HMS.HashMap StateId [input]
           -> PureState state
           -> (PureState state, HMS.HashMap StateId (HMS.HashMap StateId output))
pureUpdate f context inputs ps = (ps', outputs)
  where
    sortedInputs :: [(StateId, [input])]
    sortedInputs = sortBy (comparing fst) (HMS.toList inputs)
    labeledInputs :: [(StateId, StateId, input)]
    labeledInputs =
      zipWith (\sid' (sid, inp) -> (sid, StateId sid', inp))
              [pureIdCounter ps ..]
              (concatMap (\(sid, inps) -> map (sid, ) inps) sortedInputs)
    ps' = PureState
      { pureStates = HMS.fromList (map (\(_, sid, (state, _)) -> (sid, state)) results)
      , pureIdCounter = pureIdCounter ps + length labeledInputs
      }
    results :: [(StateId, StateId, (state, output))]
    results = map (\(sid, sid', input) -> (sid, sid', f context input (pureStates ps HMS.! sid))) labeledInputs
    outputs :: HMS.HashMap StateId (HMS.HashMap StateId output)
    outputs = HMS.fromListWith (<>) (map (\(sid, sid', (_, output)) -> (sid, HMS.singleton sid' output)) results)

nmsSettings :: IO NMSettings
nmsSettings = do
    nms <- defaultNMSettings
    return $ setNMHeartbeat 5000000 nms -- 5 seconds
