{-# LANGUAGE RecordWildCards #-}

import Control.Monad.Catch.Pure
import Data.String
import Options.Applicative
import Path
import Path.IO
import System.Exit

import Docker.Archive

data Command
  = Archive ArchiveOptions
  | Extract ExtractOptions
  | Push PushOptions
  | Pull PullOptions
  deriving (Show)

data ArchiveOptions
  = ArchiveOptions
    { archiveImageName :: ImageName
    , archivePattern :: Pattern
    }
  deriving (Show)

data ExtractOptions
  = ExtractOptions
    { extractImageName :: ImageName
    , extractOutputDir :: Path Rel Dir
    }
  deriving (Show)

data PushOptions
  = PushOptions
    { pushImageName :: ImageName
    }
  deriving (Show)

data PullOptions
  = PullOptions
    { pullImageName :: ImageName
    }
  deriving (Show)

main :: IO ()
main = do
  run =<< execParser (info parseCommand mempty)

run :: Command -> IO ()

run (Archive ArchiveOptions{..}) =
  exitWith =<< archive archiveImageName archivePattern

run (Extract ExtractOptions{..}) =
  exitWith =<< extract extractImageName =<< makeAbsolute extractOutputDir

run (Push PushOptions{..}) =
  exitWith =<< push pushImageName

run (Pull PullOptions{..}) =
  exitWith =<< pull pullImageName

parseCommand :: Parser Command
parseCommand = subparser
  ( command "archive" (info (Archive <$> parseArchiveOptions)
                        (progDesc "Create a Docker archive image."))
 <> command "extract" (info (Extract <$> parseExtractOptions)
                        (progDesc "Extract a Docker archive image."))
 <> command "push" (info (Push <$> parsePushOptions)
                     (progDesc "Push a docker image to remote."))
 <> command "pull" (info (Pull <$> parsePullOptions)
                     (progDesc "Pull a docker image from remote."))
  )

parseArchiveOptions :: Parser ArchiveOptions
parseArchiveOptions = ArchiveOptions
  <$> argument ostr (metavar "IMAGE:TAG")
  <*> argument ostr (metavar "PATTERN")

parseExtractOptions :: Parser ExtractOptions
parseExtractOptions = ExtractOptions
  <$> argument ostr (metavar "IMAGE:TAG")
  <*> argument reldir (metavar "DIR")

parsePushOptions :: Parser PushOptions
parsePushOptions = PushOptions
  <$> argument ostr (metavar "IMAGE:TAG")

parsePullOptions :: Parser PullOptions
parsePullOptions = PullOptions
  <$> argument ostr (metavar "IMAGE:TAG")

ostr :: IsString a => ReadM a
ostr = fromString <$> str

reldir :: ReadM (Path Rel Dir)
reldir = catchParse <$> str
  where
    catchParse s =
      case runCatch (parseRelDir s) of
        Left  e -> error (show e)
        Right v -> v
