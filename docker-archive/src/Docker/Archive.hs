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

    (_, _, _, ph) <- createProcess (proc "docker" options)
    waitForProcess ph
  where
    buildOptions dockerfilePath =
      ["build", "-f", toFilePath dockerfilePath, "-t", T.unpack (unImageName imgName), "."]

extract
  :: (MonadIO m)
  => ImageName -> Path Abs Dir -> m ExitCode
extract imgName outputPath = liftIO $ do
  (_, _, _, ph) <- createProcess (proc "docker" extractOptions)
  waitForProcess ph
  where
    extractOptions =
      ["run", "-ti", "--rm", "-v", (toFilePath outputPath) <> ":/output", T.unpack (unImageName imgName), "cp", "-R", "/archive/.", "/output"]

pull
  :: (MonadIO m)
  => ImageName -> m ExitCode
pull imgName = liftIO $ do
  (_, _, _, ph) <- createProcess (proc "docker" pullOptions)
  waitForProcess ph
  where
    pullOptions =
      ["pull", T.unpack (unImageName imgName)]

push
  :: (MonadIO m)
  => ImageName -> m ExitCode
push imgName = liftIO $ do
  (_, _, _, ph) <- createProcess (proc "docker" pushOptions)
  waitForProcess ph
  where
    pushOptions =
      ["push", T.unpack (unImageName imgName)]
