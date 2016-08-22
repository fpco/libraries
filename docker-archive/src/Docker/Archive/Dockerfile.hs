{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE ExistentialQuantification #-}

module Docker.Archive.Dockerfile
  ( Dockerfile

  , cmdFrom
  , cmdAdd
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
import Data.Text (Text)
import Data.Text.Encoding (encodeUtf8Builder)
import Path
import System.IO

newtype Dockerfile = Dockerfile { unDockerfile :: [DockerCommand] }

instance Monoid Dockerfile where
  mempty = Dockerfile []
  (Dockerfile xs) `mappend` (Dockerfile ys) = Dockerfile (ys <> xs)

data DockerCommand
  = From Text
  | forall b t. Add (Path b t) (Path b t)
  | CMD Text

cmdFrom :: Text -> Dockerfile
cmdFrom = Dockerfile . (:[]) . From

cmdAdd :: Path b t -> Path b t -> Dockerfile
cmdAdd local inDocker = Dockerfile [Add local inDocker]

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
  fromText "ADD " <> stringUtf8 (toFilePath local) <> fromText " "
                  <> stringUtf8 (toFilePath inDocker) <> "\n"

commandToBuilder (CMD cmd) =
  fromText "CMD " <> fromText cmd <> fromText "\n"
