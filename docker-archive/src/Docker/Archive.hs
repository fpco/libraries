{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE TemplateHaskell #-}

module Docker.Archive
  ( ImageName

  , archive
  , extract
  , push
  , pull
  ) where

import Control.Monad.Catch.Pure
import Control.Monad.IO.Class
import Codec.Archive.Tar hiding (extract)
import Codec.Archive.Tar.Entry
import Data.Monoid
import Data.Text (Text)
import qualified Data.ByteString.Lazy as LBS
import qualified Data.Text as T
import Data.String
import Data.Typeable
import GHC.Generics
import Path
import Path.IO
import System.IO (hClose)
import System.Process
import System.Exit
import qualified System.FilePath.Glob as Glob

import Docker.Archive.Dockerfile

newtype ImageName = ImageName { unImageName :: Text }
  deriving (Eq, Ord, Show, Read, Typeable, Generic, IsString)

archive
  :: (MonadIO m)
  => ImageName -> Glob.Pattern -> m ExitCode
archive imgName pattern = liftIO $ do
  pwd <- getCurrentDir
  (dirs, files) <- listDirRecur pwd

  relDirs <- mapM (makeRelative pwd) dirs
  relFiles <- mapM (makeRelative pwd) files

  let filteredDirs = filter (Glob.match pattern . toFilePath) relDirs
      filteredFiles = filter (Glob.match pattern . toFilePath) relFiles

  entryDirs <- mapM mkDirEntry filteredDirs
  entryFiles <- mapM mkFileEntry filteredFiles

  bracket
    (createProcess (proc "docker" archiveOptions) { std_in = CreatePipe })
    (\(_, _, _, ph) -> terminateProcess ph)
    $ \(Just stdin, _, _, ph) -> do
    LBS.hPut stdin $ write $ concat [[dockerfileEntry], entryDirs, entryFiles]
    hClose stdin -- hClose is used because Docker needs to see the EOF for a
                 -- tar before it begins processing on it.
    waitForProcess ph
  where
    archiveOptions = ["build", "-t", T.unpack (unImageName imgName), "-"]
    mkDirEntry d = packDirectoryEntry (toFilePath d) $
      case toTarPath True $ toFilePath $ archiveDirectory </> d of
        Left  e -> error e
        Right v -> v
    mkFileEntry f = packFileEntry (toFilePath f) $
      case toTarPath False $ toFilePath $ archiveDirectory </> f of
        Left  e -> error e
        Right v -> v

archiveDirectory :: Path Rel Dir
archiveDirectory = $(mkRelDir "archive")

dockerfileEntry :: Entry
dockerfileEntry =
  simpleEntry name $ NormalFile content (LBS.length content)
  where
    name :: TarPath
    name = case toTarPath False $ toFilePath $(mkRelFile "Dockerfile") of
             Left  e -> error e
             Right v -> v
    content = dockerfileToLazyByteString $
                 cmdFrom "busybox"
                 -- We are using busybox here because it is the smallest known
                 -- image that also contains cp. Which at present is necessary
                 -- to extract files that are added to the docker archive.
              <> cmdAdd archiveDirectory archiveDirectory
              <> cmdCMD "ls /archive"

extract
  :: (MonadIO m)
  => ImageName -> Path Abs Dir -> m ExitCode
extract imgName outputPath = runDocker extractOptions
  where
    extractOptions =
      ["run", "--rm", "-v", (toFilePath outputPath) <> ":/output", T.unpack (unImageName imgName), "cp", "-a", "/archive/.", "/output"]

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
