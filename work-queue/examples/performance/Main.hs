{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE OverloadedStrings #-}
module Main where

import           Control.Monad
import           Criterion.Measurement
import           Data.IORef.Lifted
import           Data.Random.Source.PureMT
import qualified Data.Text as T
import           Distributed.Stateful.Master
import qualified KMeans
import           Options.Applicative
import qualified PFilter
import           PerformanceUtils
import           System.Directory
import           System.Process (readCreateProcess, shell)
import qualified Vectors

data Options = Options
               { optNoNetworkMessage :: Bool
               , optMinSlaves :: Int
               , optMaxSlaves :: Int
               , optPurgeCSV :: Bool
               , optSpawnWorker :: Maybe Int
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
         (long "minslaves" <> short 'm'
          <> help "Minimal number of slaves used in the benchmark.  The benchmark will be run with n slaves, where n is taken from [minslaves .. maxslaves].")
        <|> pure 0)
    <*> (option auto
         (long "maxslaves" <> short 'n'
          <> help "Maximal number of slaves used in the benchmark.  The benchmark will be run with n slaves, where n is taken from [minslaves .. maxslaves].")
        <|> pure 8)
    <*> switch (long "purge-csv"
                <> help (unlines [ "Usually, results are appended to existing CSV files.  If this flag is on, existing csv files will be overwritten instead."
                                 , "Keeping results can be useful to compare data from different revisions."]))
    <*> optional (option auto
                  (long "spawn-worker"
                   <> metavar "NSLAVES"
                   <> help "Used internally to spawn a worker, that will work with NSLAVES slaves."))
    <*> subparser
        (   command "pfilter" (BenchPFilter <$> benchPFilter `info` progDesc "Benchmark with a simple particle filter.")
         <> command "vectors" (BenchVectors <$> benchVectors `info` progDesc "Benchmark that performs some arithmetic with vectors.")
         <> command "kmeans"  (BenchKMeans <$> benchKMeans `info` progDesc "Benchmark with a parallel KMeans algorithm.")
        )

runBench :: Int -> CSVInfo -> Options -> IO ()
runBench nSlaves commonCsvInfo Options{..} =
    case optBench of
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
                csvInfo = commonCsvInfo <> CSVInfo [("slaves", T.pack $ show nSlaves)] <> PFilter.csvInfo pfOpts
            logErrors $
                if optNoNetworkMessage
                then runWithoutNM fp csvInfo masterArgs nSlaves master request
                else runWithNM fp csvInfo PFilter.jqc optSpawnWorker masterArgs nSlaves master request
        BenchVectors vOpts ->
            let masterArgs = Vectors.masterArgs vOpts
                master = Vectors.myAction
                request = return $ Vectors.myStates vOpts
                fp = Vectors.optOutput vOpts
                csvInfo = commonCsvInfo <> CSVInfo [("slaves", T.pack $ show nSlaves)] <> Vectors.csvInfo vOpts
            in logErrors $
                if optNoNetworkMessage
                then runWithoutNM fp csvInfo masterArgs nSlaves master request
                else runWithNM fp csvInfo PFilter.jqc optSpawnWorker masterArgs nSlaves master request
        BenchKMeans kOpts ->
            let masterArgs = KMeans.masterArgs kOpts
                master = KMeans.distributeKMeans
                request = KMeans.generateRequest kOpts
                fp = KMeans.optOutput kOpts
                csvInfo = commonCsvInfo <> CSVInfo [("slaves", T.pack $ show nSlaves)] <> KMeans.csvInfo kOpts
            in logErrors $
                   if optNoNetworkMessage
                   then runWithoutNM fp csvInfo masterArgs nSlaves master request
                   else runWithNM fp csvInfo KMeans.jqc optSpawnWorker masterArgs nSlaves master request

purgeResults :: Options -> IO ()
purgeResults Options{..} =
    let fp = case optBench of
            BenchPFilter pfOpts -> PFilter.optOutput pfOpts
            BenchVectors vOpts -> Vectors.optOutput vOpts
            BenchKMeans kOpts -> KMeans.optOutput kOpts
    in doesFileExist fp >>= \exists -> when exists (removeFile fp)

main :: IO ()
main = do
    initializeTime
    opts <- execParser
        (info (helper <*> options)
         (fullDesc <> progDesc
          (unlines [])))
    gitHash <- readCreateProcess (shell "git rev-parse HEAD") ""
    nodename <- readCreateProcess (shell "uname -n") ""
    let commonCsvInfo = CSVInfo
            [ ("commit", T.pack $ take 8 gitHash)
            , ("node", T.pack $ init nodename)
            , ("NetworkMessage", T.pack . show . optNoNetworkMessage $ opts)
            ]
    when (optPurgeCSV opts) (purgeResults opts)
    case optSpawnWorker opts of
        Just nSlaves -> -- spawn a worker that accepts nSlaves Slaves
            runBench nSlaves commonCsvInfo opts
        Nothing -> -- run the benchmark for every nSlaves in [optMinSlaves .. optMaxSlaves]
            void $ forM [optMinSlaves opts .. optMaxSlaves opts] $
                \nSlaves -> runBench nSlaves commonCsvInfo opts
