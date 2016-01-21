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
import           Control.DeepSeq (NFData)
import           Control.Exception (BlockedIndefinitelyOnMVar(..))
import           Data.Binary (Binary)
import           Data.Conduit.Network (serverSettings, clientSettings)
import qualified Data.Conduit.Network as CN
import qualified Data.HashMap.Strict as HMS
import qualified Data.HashSet as HS
import           Data.Streaming.NetworkMessage
import qualified Data.Streaming.NetworkMessage as NM
import           Distributed.Stateful.Slave
import           Distributed.Stateful.Master
import           Distributed.Stateful.Internal (StateId(..))
import           System.Timeout (timeout)
import           Test.Hspec (shouldBe)
import qualified Test.Hspec as Hspec
import           Test.QuickCheck

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
            -- states `shouldBe` (HMS.map (\(_input, initial) -> [Right "step 1", Left initial]) inputs)
    Hspec.it "Passes quickcheck comparison with pure implementation" $
      property $ forAll arbitrary $
      \( Blind (function :: Context -> Input -> State -> (State, Output))
       , initialStates :: [State]
       , updates :: [(Context, [[Input]])]
       ) -> ioProperty $ do
         results <- runMasterAndSlaves 7000 4 (\c i s -> return (function c i s)) initialStates $ \mh -> do
           let go :: PureState State -> (Context, [[Input]]) -> IO (PureState State)
               go ps (ctx, inputs) = do
                 sids <- getStateIds mh
                 let sids' = sort (HMS.keys (pureStates ps))
                 sort (HS.toList sids) `shouldBe` sids'
                 let inputMap = HMS.fromList (zip sids' inputs)
                 outputs <- update mh ctx inputMap
                 let (ps', outputs') = pureUpdate function ctx inputMap ps
                 -- putStrLn "===="
                 -- print (ctx, inputs)
                 -- print ps
                 -- print ps'
                 -- print outputs
                 -- print outputs'
                 outputs `shouldBe` outputs'
                 return ps'
           foldM go (initialPureState initialStates) updates
         return True

it :: String -> IO () -> Hspec.Spec
it name f = Hspec.it name f

runMasterAndSlaves
    :: forall state context input output a.
       (NFData state, Binary state, Binary context, Binary input, Binary output)
    => Int
    -> Int
    -> (context -> input -> state -> IO (state, output))
    -> [state]
    -> (MasterHandle state context input output -> IO a)
    -> IO a
runMasterAndSlaves port slaveCnt slaveUpdate initialStates inner = do
    nms <- nmsSettings
    -- Running slaves
    let slaveArgs = SlaveArgs
            { saUpdate = slaveUpdate
            , saInit = return ()
            , saNMSettings = nms
            , saClientSettings = clientSettings port "localhost"
            }
    let runSlaves = mapConcurrently (\_ -> runSlave slaveArgs) (replicate slaveCnt () :: [()])
    -- Running master
    let masterArgs = MasterArgs
            { maMinBatchSize = Nothing
            , maMaxBatchSize = Just 5
            }
    mh <- mkMasterHandle masterArgs
    masterReady <- newEmptyMVar
    someConnected <- newEmptyMVar
    doneVar <- newEmptyMVar
    let ss = CN.setAfterBind (\_ -> tryPutMVar masterReady () >> return ()) (serverSettings port "*")
    let acceptConns =
            timeout (1000 * 1000 * 2) $
            CN.runGeneralTCPServer ss $ NM.generalRunNMApp nms (const "") (const "") $ \nm -> do
                addSlaveConnection mh nm
                void $ tryPutMVar someConnected ()
                readMVar doneVar
    withAsync ((takeMVar masterReady >> runSlaves) `concurrently` acceptConns) $ \_ -> do
        takeMVar someConnected `catch` \BlockedIndefinitelyOnMVar -> fail "No slaves connected"
        resetStates mh initialStates
        r <- inner mh
        putMVar doneVar ()
        return r

newtype Context = Context Int deriving (CoArbitrary, Arbitrary, Show, Binary)
newtype Input = Input Int deriving (CoArbitrary, Arbitrary, Show, Binary)
newtype State = State Int deriving (CoArbitrary, Arbitrary, Show, Binary, NFData)
newtype Output = Output Int deriving (CoArbitrary, Arbitrary, Show, Binary, Eq)

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
    return $ setNMHeartbeat 5000000 nms -- We use 5 seconds
