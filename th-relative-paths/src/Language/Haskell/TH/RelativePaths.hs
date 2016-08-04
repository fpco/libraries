{-# LANGUAGE TemplateHaskell #-}

-- | This module provides Template Haskell utilities for loading files
-- based on paths relative to the root of your Cabal package.
--
-- Normally when building a cabal package, GHC is run with its current
-- directory set at the package's root directory. This allows using
-- relative paths to refer to files. However, this becomes problematic
-- when you want to load modules from multiple projects, such as when
-- using "stack ghci".
--
-- This solves the problem by getting the current module's filepath from
-- TH via 'location'. It then searches upwards in the directory tree for
-- a .cabal file, and makes the provided path relative to the folder
-- it's in.
module Language.Haskell.TH.RelativePaths where

import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS
import           Data.List (find)
import qualified Data.Text as T
import qualified Data.Text.IO as T
import qualified Data.Text.Lazy as LT
import qualified Data.Text.Lazy.IO as LT
import           Language.Haskell.TH (Q, Loc(loc_filename), location, runIO, reportWarning)
import           Language.Haskell.TH.Syntax (addDependentFile)
import           System.Directory (getDirectoryContents, getCurrentDirectory, setCurrentDirectory)
import           System.FilePath

-- | Reads a file as a strict ByteString. The path is specified relative
-- to the package's root directory, and 'addDependentfile' is invoked on
-- the target file.
readFileBS :: FilePath -> Q BS.ByteString
readFileBS fp = do
    fp' <- pathRelativeToCabalPackage fp
    addDependentFile fp'
    runIO $ BS.readFile fp'

-- | Reads a file as a lazy ByteString. The path is specified relative
-- to the package's root directory, and 'addDependentfile' is invoked on
-- the target file.
readFileLBS :: FilePath -> Q LBS.ByteString
readFileLBS fp = do
    fp' <- pathRelativeToCabalPackage fp
    addDependentFile fp'
    runIO $ LBS.readFile fp'

-- | Reads a file as a strict Text. The path is specified relative
-- to the package's root directory, and 'addDependentfile' is invoked on
-- the target file.
readFileText :: FilePath -> Q T.Text
readFileText fp = do
    fp' <- pathRelativeToCabalPackage fp
    addDependentFile fp'
    runIO $ T.readFile fp'

-- | Reads a file as a lazy Text. The path is specified relative
-- to the package's root directory, and 'addDependentfile' is invoked on
-- the target file.
readFileLazyText :: FilePath -> Q LT.Text
readFileLazyText fp = do
    fp' <- pathRelativeToCabalPackage fp
    addDependentFile fp'
    runIO $ LT.readFile fp'

-- | Reads a file as a String. The path is specified relative
-- to the package's root directory, and 'addDependentfile' is invoked on
-- the target file.
readFileString :: FilePath -> Q String
readFileString fp = do
    fp' <- pathRelativeToCabalPackage fp
    addDependentFile fp'
    runIO $ readFile fp'

-- | Runs the 'Q' action, temporarily setting the current working
-- directory to the root of the cabal package.
withCabalPackageWorkDir :: Q a -> Q a
withCabalPackageWorkDir f = do
    cwd' <- pathRelativeToCabalPackage "."
    cwd <- runIO $ getCurrentDirectory
    runIO $ setCurrentDirectory cwd'
    x <- f
    runIO $ setCurrentDirectory cwd
    return x

-- | This utility takes a path that's relative to your package's cabal
-- file, and resolves it to an absolute location.
--
-- Note that this utility does _not_ invoke 'qAddDependentFile'.
pathRelativeToCabalPackage :: FilePath -> Q FilePath
pathRelativeToCabalPackage fp = do
    loc <- location
    mcabalFile <- runIO $ findCabalFile (loc_filename loc)
    case mcabalFile of
        Just cabalFile -> return (takeDirectory cabalFile </> fp)
        Nothing -> do
            reportWarning "Failed to find cabal file, in order to resolve relative paths in TH.  Using current working directory instead."
            cwd <- runIO getCurrentDirectory
            return (cwd </> fp)

-- | Given the path to a file or directory, search parent directories
-- for a .cabal file.
findCabalFile :: FilePath -> IO (Maybe FilePath)
findCabalFile dir = do
    let parent = takeDirectory dir
    contents <- getDirectoryContents parent
    case find (\fp -> takeExtension fp == ".cabal") contents of
        Nothing
            | parent == dir -> return Nothing
            | otherwise -> findCabalFile parent
        Just fp -> return (Just (parent </> fp))
