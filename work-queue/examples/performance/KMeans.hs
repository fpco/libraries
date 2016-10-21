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
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TypeFamilies #-}
module KMeans ( Options (..)
              , masterArgs
              , distributeKMeans
              , generateRequest
              , csvInfo
              , jqc
              ) where

import           ClassyPrelude
import           Control.DeepSeq
import           Control.Monad.Logger
import           Control.Monad.Random hiding (fromList)
import qualified Control.Monad.Trans.Class as Trans
import qualified Data.HashMap.Strict as HMS
import qualified Data.HashSet as HS
import           Data.List (unfoldr)
import           Data.List.Extra (chunksOf)
import qualified Data.List.NonEmpty as NE
import           Data.Store (Store)
import           Data.Store.TypeHash (mkManyHasTypeHash)
import qualified Data.Vector as V
import qualified Data.Vector.Mutable as MVector
import qualified Data.Vector.Unboxed as UV
import           Distributed.JobQueue.Client
import           Distributed.Stateful
import           Distributed.Stateful.Master
import           FP.Redis (MonadConnect)
import           PerformanceUtils
import           TypeHash.Orphans ()

-- * Command line options

data Options = Options
               { optDim :: Int
               , optNClusters :: Int
               , optNPoints :: Int
               , optGranularity :: Int
               , optNIterations :: Int
               , optOutput :: FilePath
               }

-- * Types and instances


data Point = Point (UV.Vector Double)
             deriving (Generic, NFData, Show)
instance Store Point

data Cluster = Cluster
               { clId :: Int
               , clCent :: Point
               } deriving (Generic, NFData, Show)
instance Store Cluster

data PointSum = PointSum !Int !(UV.Vector Double)
                deriving (Generic, NFData, Show)
instance Store PointSum

instance Semigroup PointSum where
    (PointSum count v) <> (PointSum count' v') =
        PointSum (count + count') (UV.zipWith (+) v v')

type State = ()
data Input = DistributePoints (V.Vector Point)
           | SumPointSums [Output]
             deriving (Generic, NFData, Show)
instance Store Input

data Request = Request
               { rInitialClusters :: !(V.Vector Cluster) -- ^ starting guess for clusters
               , rPoints :: !(V.Vector Point) -- ^ points to cluster
               , rGranularity :: !Int -- ^ number of points to process in a single batch
               , rMaxIterations :: !Int -- ^ maximal number of iterations
               } deriving (Generic, Show)
instance Store Request

type Response = V.Vector Cluster

data Context = OldClusters (V.Vector Cluster)
             | NoContext
             deriving (Generic, NFData, Show)
instance Store Context

newtype Output = Output (V.Vector PointSum)
               deriving (Generic, NFData, Show)
instance Store Output
instance Semigroup Output where
    (Output v1) <> (Output v2) = Output $ V.zipWith (<>) v1 v2

$(mkManyHasTypeHash [ [t| Input |]
                    , [t| Request |]
                    , [t| Response |]
                    , [t| Output |]
                    , [t| Context |]
                    ])

sqDistance :: Point -> Point -> Double
sqDistance (Point v1) (Point v2) = UV.sum $ UV.zipWith (\x y -> (x-y)*(x-y)) v1 v2

addToPointSum :: PointSum -> Point -> PointSum
addToPointSum (PointSum count v1) (Point v2) =
    PointSum (count + 1) (UV.zipWith (+) v1 v2)

emptyPointSum :: Options -> PointSum
emptyPointSum Options{..} = PointSum 0 (UV.replicate optDim 0)

emptyOutput :: Options -> Output
emptyOutput opts = Output $ V.replicate (optNClusters opts) (emptyPointSum opts)

pointSumToCluster :: Int -> PointSum -> Cluster
pointSumToCluster i (PointSum count v) =
    Cluster { clId = i
            , clCent = Point $ UV.map (/(fromIntegral count)) v
            }

-- | Assign points to their nearest cluster.
assign :: Options -> V.Vector Cluster -> V.Vector Point -> V.Vector PointSum
assign Options{..} clusters points = V.create $ do
    vec <- assert (optNClusters == length clusters) $ MVector.replicate optNClusters (PointSum 0 (UV.replicate optDim 0))
    let addpoint p = do
            let c = nearest p
                cid = clId c
            ps <- MVector.read vec cid
            MVector.write vec cid $! addToPointSum ps p
    (mapM_ addpoint points)
    return vec
  where
      nearest p = fst $ minimumByEx (compare `on` snd) $ V.map (\c -> (c, sqDistance (clCent c) p)) clusters

makeNewClusters :: Output -> V.Vector Cluster
makeNewClusters (Output vec) =
    V.imap (\i ps -> pointSumToCluster i ps) . V.filter (\(PointSum count _) -> count > 0) $ vec

updateFn :: MonadConnect m => Options -> Update m State Context Input Output
updateFn opts (OldClusters clusters) (DistributePoints points) () = distributePoints opts clusters points
updateFn opts NoContext (SumPointSums pss) () = sumPointSums opts pss
updateFn _ _ _ _ = error "Internal error: Wrong call of update function."

distributePoints :: MonadConnect m => Options -> (V.Vector Cluster) -> V.Vector Point -> m (State, Output)
distributePoints opts clusters points = do
    let output = assign opts clusters points
    return ((), Output output)

sumPointSums :: Monad m => Options -> [Output] -> m (State, Output)
sumPointSums opts pss =
    let output = foldl' (<>) (emptyOutput opts) $ pss
    in return ((), output)

chunksOfVec :: Int -> Vector a -> [Vector a]
chunksOfVec n vec = unfoldr (\v -> case splitAt n v of
                                    (emptyvec, _) | V.null emptyvec -> Nothing
                                    (chunk, rest) -> Just (chunk, rest)) vec

distributeKMeans :: forall m . MonadConnect m => Request -> MasterHandle m State Context Input Output -> m (Response)
distributeKMeans Request{..} mh = do
    resetStates mh (replicate nStates ()) >> distrib rMaxIterations rInitialClusters
  where
      inputs = chunksOfVec rGranularity rPoints :: [(V.Vector Point)]
      nStates = length inputs
      distrib 0 clusters = return clusters
      distrib n clusters = do
          stateIds <- getStateIds mh
          let inputMap = (HMS.fromList $ zipWith (\sid ps -> (sid, [DistributePoints ps]))
                          (assert (HS.size stateIds == nStates) $ HS.toList stateIds)
                          inputs) :: HMS.HashMap StateId [Input]
          pointgroups <- update mh (OldClusters clusters) inputMap
          reduce n pointgroups

      reduce n pointgroups = do
          stateIds <- getStateIds mh
          let pgs' = chunksOf rGranularity . concat . map HMS.elems . HMS.elems $ pointgroups :: [[Output]]
              inputMap = (HMS.fromList $ zipWith (\sid ps -> (sid, [SumPointSums ps])) (HS.toList stateIds) (pgs' ++ repeat []))
          pointgroups' <- update mh NoContext inputMap :: m (HMS.HashMap StateId (HMS.HashMap StateId Output))
          let summedPgs = sconcat . NE.fromList . HMS.elems $ sconcat . NE.fromList . HMS.elems $ pointgroups'
          let clusters' = makeNewClusters summedPgs
          distrib (n-1) clusters'

masterArgs :: MonadConnect m => Options -> MasterArgs m State Context Input Output
masterArgs opts = (defaultMasterArgs (updateFn opts)) { maDoProfiling = DoProfiling }

instance MonadLogger m => MonadLogger (RandT StdGen m) where
    monadLoggerLog a b c d = Trans.lift $ monadLoggerLog a b c d

generateRequest :: MonadConnect m => Options -> m Request
generateRequest Options{..} = do
    let r = mkStdGen 42
        randPoint range = Point <$> (UV.generateM optDim $ (const $ getRandomR range))
        randPointAround (Point v) = do
            (Point v') <- randPoint (-1,1)
            return $ Point (UV.zipWith (+) v v')
    (flip evalRandT) r $ do
        centerPoints <-
            mapM (const $ randPoint (-1000,1000)) [0..optNClusters-1]
        let pointsPerCluster = optNPoints `div` optNClusters
            clusterAroundPoint p = V.generateM pointsPerCluster (const $ randPointAround p)
        ps <- mapM clusterAroundPoint centerPoints
        initialClusters <- V.mapM (\(i, (Point p)) -> do
                                          (Point jitter) <- randPoint (-10,10)
                                          let p' = UV.zipWith (\x y -> (x + y)) p jitter
                                          return $ Cluster i (Point p')) (V.fromList $ zip [0..] centerPoints)
        when (length initialClusters /= optNClusters) (error $ unwords ["wrong numnber of clusters:"
                                                                       , show (length initialClusters)
                                                                       , "instead of"
                                                                       , show optNClusters])
        $logInfoS logSourceBench (unwords [ "centerPoints:", pack $ show centerPoints
                                                 , "\ninitial clusters:", pack $ show initialClusters
                                                 ])
        return $ Request { rInitialClusters = initialClusters
                         , rPoints = V.concat ps
                         , rGranularity = optGranularity
                         , rMaxIterations = optNIterations
                         }


jqc :: JobQueueConfig
jqc = defaultJobQueueConfig "perf:kmeans:"

csvInfo :: Options -> ProfilingColumns
csvInfo opts =
    [ ("clusters", pack . show . optNClusters $ opts)
    , ("points", pack . show . optNPoints $ opts)
    , ("granularity", pack . show . optGranularity $ opts)
    , ("iterations", pack . show . optNIterations $ opts)
    ]
