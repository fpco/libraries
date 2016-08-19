{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE TemplateHaskell #-}

module Docker.Archive
  ( ImageName
  , Pattern

  , archive
  , extract
  , push
  , pull
  ) where

import Control.Exception
import Control.Monad.IO.Class
import Data.Monoid
import Data.Text (Text)
import qualified Data.Text as T
import Data.String
import Data.Typeable
import GHC.Generics
import Path
import Path.IO
import System.Process
import System.Exit

import Docker.Archive.Dockerfile

newtype ImageName = ImageName { unImageName :: Text }
  deriving (Eq, Ord, Show, Read, Typeable, Generic, IsString)

archive
  :: (MonadIO m)
  => ImageName -> Pattern -> m ExitCode
archive imgName pattern = liftIO $ do
  pwd <- getCurrentDir
  withTempDir pwd ".ark" $ \tmpAbsDir -> do
    let dockerFile = tmpAbsDir </> $(mkRelFile "Dockerfile")
        options    = buildOptions dockerFile

    dockerfileToFile dockerFile $
         cmdFrom "busybox"
      <> cmdCopy pattern $(mkAbsDir "/archive")
      <> cmdCMD "ls /archive"

    runDocker options
  where
    buildOptions dockerfilePath =
      ["build", "-f", toFilePath dockerfilePath, "-t", T.unpack (unImageName imgName), "."]

extract
  :: (MonadIO m)
  => ImageName -> Path Abs Dir -> m ExitCode
extract imgName outputPath = runDocker extractOptions
  where
    extractOptions =
      ["run", "-ti", "--rm", "-v", (toFilePath outputPath) <> ":/output", T.unpack (unImageName imgName), "cp", "-R", "/archive/.", "/output"]

pull
  :: (MonadIO m)
  => ImageName -> m ExitCode
pull imgName = runDocker pullOptions
  where
    pullOptions =
      ["pull", T.unpack (unImageName imgName)]

push
  :: (MonadIO m)
  => ImageName -> m ExitCode
push imgName = runDocker pushOptions
  where
    pushOptions =
      ["push", T.unpack (unImageName imgName)]

-- Utilities

runDocker :: (MonadIO m) => [String] -> m ExitCode
runDocker opts = liftIO $ bracket
  (createProcess (proc "docker" opts))
  (\(_, _, _, ph) -> terminateProcess ph)
    $ \(_, _, _, ph) -> waitForProcess ph
