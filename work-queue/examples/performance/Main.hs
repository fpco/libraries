{-# LANGUAGE OverloadedStrings #-}
module Main where

import           Data.IORef.Lifted
import           Data.Random.Source.PureMT
import qualified Data.Text as T
import           Distributed.Stateful.Master
import qualified KMeans
import           Options.Applicative
import qualified PFilter
import           PerformanceUtils
import           System.Process (readCreateProcess, shell)
import qualified Vectors

data Options = Options
               { optNoNetworkMessage :: Bool
               , optNSlaves :: Int
               , optSpawnWorker :: Bool
               , optBench :: Benchmark
               }

data Benchmark =
      BenchPFilter PFilter.Options
    | BenchVectors Vectors.Options
    | BenchKMeans KMeans.Options

benchPFilter :: Parser PFilter.Options
benchPFilter = PFilter.Options
    <$> (option auto
         (long "stepsize" <> short 'h'
          <> help "Stepsize for Runge-Kutta integration")
         <|> pure 0.0001)
    <*> (option auto
         (long "deltat" <> short 'd'
          <> help "Time interval between to particle filter steps")
         <|> pure 0.1)
    <*> (option auto
         (long "steps" <> short 's'
          <> help "Number of particle filter steps")
         <|> pure 10)
    <*> (option auto
         (long "omega2" <> short 'w'
          <> help "Squared eigenfrequency of the pendulum")
         <|> pure 1)
    <*> (option auto
         (long "omega2-interval" <> short 'i'
          <> help "Size of the interval in which we perform the parameter search")
         <|> pure 2)
    <*> (option auto
         (long "phi0" <> short 'p'
          <> help "Initial condition")
         <|> pure (pi/2))
    <*> (option auto
         (long "resample-threshold" <> short 't'
          <> help "When the effective number of particles divided my the number of particles drops below this ratio, we perform resampling.  Should be in the interval [0,1], where 0 means no resampling, and 1 means resampling during every update.")
         <|> pure 0.5)
    <*> (option auto
         (long "nparticles" <> short 'N'
          <> help "Number of particles")
         <|> pure 1000)
    <*> (strOption
         (long "output" <> short 'o'
          <> help "FilePath for the csv output")
         <|> pure "bench-pfilter.csv")

benchVectors :: Parser Vectors.Options
benchVectors = Vectors.Options
    <$> (option auto
         (short 'l'
          <> help "length of vectors")
         <|> pure 1000)
    <*> (option auto
         (short 'N'
          <> help "number of states")
         <|> pure 200)
    <*> (strOption
         (long "output" <> short 'o'
          <> help "FilePath for the csv output")
         <|> pure "bench-vectors.csv")

benchKMeans :: Parser KMeans.Options
benchKMeans = KMeans.Options
    <$> (option auto
         (long "dim" <> short 'v'
          <> help "Vector space dimension")
         <|> pure 10)
    <*> (option auto
         (long "clusters" <> short 'c'
          <> help "Number of clusters")
         <|> pure 4)
    <*> (option auto
         (long "points" <> short 'p'
          <> help "Number of points")
         <|> pure 1000000)
    <*> (option auto
         (long "granularity" <> short 'g'
          <> help "Points will be grouped into groups of this size for processing")
         <|> pure 50000)
    <*> (option auto
         (long "iterations" <> short 'i'
          <> help "Number of iterations")
         <|> pure 3)
    <*> (strOption (long "output" <> short 'o'
                           <> help "FilePath for the csv output")
         <|> pure "kmeans-bench.csv")

options :: Parser Options
options = Options
    <$> switch (long "no-network-message"
                <> help "Run in a single process, communicating via STM (instead of using NetworkMessage")
    <*> (option auto
         (long "nslaves" <> short 'n'
          <> help "Number of slave nodes"))
    <*> switch (long "spawn-worker"
                   <> help "Used internally to spawn a worker")
    <*> subparser
        (   command "pfilter" (BenchPFilter <$> benchPFilter `info` progDesc "Benchmark with a simple particle filter.")
         <> command "vectors" (BenchVectors <$> benchVectors `info` progDesc "Benchmark that performs some arithmetic with vectors.")
         <> command "kmeans"  (BenchKMeans <$> benchKMeans `info` progDesc "Benchmark with a parallel KMeans algorithm.")
        )

main :: IO ()
main = do
    opts <- execParser
        (info (helper <*> options)
         (fullDesc <> progDesc
          (unlines [])))
    gitHash <- readCreateProcess (shell "git rev-parse HEAD") ""
    nodename <- readCreateProcess (shell "uname -n") ""
    let commonCsvInfo =
            [ ("commit", T.pack $ take 8 gitHash)
            , ("node", T.pack $ init nodename)
            , ("NetworkMessage", T.pack . show . optNoNetworkMessage $ opts)
            , ("slaves", T.pack . show . optNSlaves $ opts)
            ]
    case optBench opts of
        BenchPFilter pfOpts -> do
            let cfg = PFilter.pfConfig pfOpts
                slave = PFilter.dpfSlave cfg
                masterArgs = MasterArgs
                    { maMaxBatchSize = Just 5
                    , maUpdate = slave
                    }
            randomsrc <- newIORef (pureMT 42)
            let master = PFilter.dpfMaster cfg randomsrc
                request = PFilter.generateRequest pfOpts randomsrc
                fp = PFilter.optOutput pfOpts
                csvInfo = commonCsvInfo ++ PFilter.csvInfo pfOpts
            logErrors $
                if optNoNetworkMessage opts
                then runWithoutNM fp csvInfo masterArgs (optNSlaves opts) master request
                else runWithNM fp csvInfo PFilter.jqc (optSpawnWorker opts) masterArgs (optNSlaves opts) master request
        BenchVectors vOpts ->
            let masterArgs = Vectors.masterArgs vOpts
                master = Vectors.myAction
                request = return $ Vectors.myStates vOpts
                fp = Vectors.optOutput vOpts
                csvInfo = commonCsvInfo ++ Vectors.csvInfo vOpts
            in logErrors $
                if optNoNetworkMessage opts
                then runWithoutNM fp csvInfo masterArgs (optNSlaves opts) master request
                else runWithNM fp csvInfo PFilter.jqc (optSpawnWorker opts) masterArgs (optNSlaves opts) master request
        BenchKMeans kOpts ->
            let masterArgs = KMeans.masterArgs kOpts
                master = KMeans.distributeKMeans
                request = KMeans.generateRequest kOpts
                fp = KMeans.optOutput kOpts
                csvInfo = commonCsvInfo ++ KMeans.csvInfo kOpts
            in logErrors $
                   if optNoNetworkMessage opts
                   then runWithoutNM fp csvInfo masterArgs (optNSlaves opts) master request
                   else runWithNM fp csvInfo KMeans.jqc (optSpawnWorker opts) masterArgs (optNSlaves opts) master request
