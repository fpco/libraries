{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Docker.Archive.Dockerfile
  ( Pattern (..)
  , Dockerfile

  , cmdFrom
  , cmdAdd
  , cmdCopy
  , cmdCMD

  , dockerfileToLazyByteString
  , dockerfileToBuilder
  , dockerfileToFile
  , dockerfileIntoHandle
  ) where

import Control.Exception
import Data.Monoid
import Data.ByteString.Lazy (ByteString)
import Data.ByteString.Builder
import Data.String
import Data.Text (Text)
import Data.Text.Encoding (encodeUtf8Builder)
import Data.Typeable
import GHC.Generics
import Path
import System.IO

newtype Pattern = Pattern { unPattern :: Text }
  deriving (Eq, Ord, Show, Read, Typeable, Generic, IsString)

newtype Dockerfile = Dockerfile { unDockerfile :: [DockerCommand] }

instance Monoid Dockerfile where
  mempty = Dockerfile []
  (Dockerfile xs) `mappend` (Dockerfile ys) = Dockerfile (ys <> xs)

data DockerCommand
  = From Text
  | Add Pattern (Path Abs Dir)
  | Copy Pattern (Path Abs Dir)
  | CMD Text
  deriving (Show)

cmdFrom :: Text -> Dockerfile
cmdFrom = Dockerfile . (:[]) . From

cmdAdd :: Pattern -> Path Abs Dir -> Dockerfile
cmdAdd local inDocker = Dockerfile [Add local inDocker]

cmdCopy :: Pattern -> Path Abs Dir -> Dockerfile
cmdCopy local inDocker = Dockerfile [Copy local inDocker]

cmdCMD :: Text -> Dockerfile
cmdCMD = Dockerfile . (:[]) . CMD

dockerfileToLazyByteString :: Dockerfile -> ByteString
dockerfileToLazyByteString = toLazyByteString . dockerfileToBuilder

dockerfileToBuilder :: Dockerfile -> Builder
dockerfileToBuilder backwardDockerfile = mconcat $ fmap commandToBuilder dockerfile
  where
    dockerfile = reverse $ unDockerfile backwardDockerfile

dockerfileToFile :: Path Abs File -> Dockerfile -> IO ()
dockerfileToFile path dockerfile =
  bracket (openFile filepath WriteMode) hClose
    $ \hdl -> dockerfileIntoHandle hdl dockerfile
  where
    filepath = toFilePath path

dockerfileIntoHandle :: Handle -> Dockerfile -> IO ()
dockerfileIntoHandle hdl dockerfile = do
  hSetBuffering hdl (BlockBuffering Nothing)
  hSetBinaryMode hdl True
  hPutBuilder hdl (dockerfileToBuilder dockerfile)

fromText :: Text -> Builder
fromText = encodeUtf8Builder

commandToBuilder :: DockerCommand -> Builder

commandToBuilder (From super) =
  fromText "FROM " <> fromText super <> fromText "\n"

commandToBuilder (Add local inDocker) =
  fromText "ADD " <> fromText (unPattern local) <> fromText " "
                  <> stringUtf8 (toFilePath inDocker) <> "\n"

commandToBuilder (Copy local inDocker) =
  fromText "COPY " <> fromText (unPattern local) <> fromText " "
                   <> stringUtf8 (toFilePath inDocker) <> "\n"

commandToBuilder (CMD cmd) =
  fromText "CMD " <> fromText cmd <> fromText "\n"
