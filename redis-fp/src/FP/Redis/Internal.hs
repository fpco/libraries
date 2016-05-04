{-# LANGUAGE NoImplicitPrelude, OverloadedStrings, ScopedTypeVariables, TypeFamilies,
             DeriveDataTypeable, FlexibleContexts, FlexibleInstances, RankNTypes, GADTs,
             ConstraintKinds, NamedFieldPuns, ViewPatterns, BangPatterns, LambdaCase #-}

-- | Redis internal utilities.

module FP.Redis.Internal where

import Blaze.ByteString.Builder (Builder)
import qualified Blaze.ByteString.Builder as Builder
import qualified Blaze.ByteString.Builder.Char.Utf8 as Builder
import ClassyPrelude.Conduit hiding (Builder, leftover)
import Control.DeepSeq (deepseq)
import qualified Data.Attoparsec.ByteString as Atto
import Data.Attoparsec.ByteString (takeTill)
import Data.Attoparsec.ByteString.Char8
    (Parser, choice, char, isEndOfLine, endOfLine, decimal, signed, take, count)
import qualified Data.ByteString.Lazy as BSL
import qualified Data.Streaming.Network as CN
import qualified Network.Socket.ByteString as NS

import FP.Redis.Types.Internal

-- | Make a command request
makeCommand :: (Result a) => ByteString -> [ByteString] -> CommandRequest a
makeCommand !cmd !args =
    deepseq args $
    CommandRequest (renderRequest (encodeArg cmd:args))

-- | Render list to Redis request protocol (adapted from hedis)
renderRequest :: [ByteString] -> Builder
renderRequest req = concat (argCnt:args)
  where
    argCnt = star
             ++ Builder.fromShow (length req)
             ++ crlf
    args   = map renderArg req
    renderArg arg = dollar
                    ++ argLen arg
                    ++ crlf
                    ++ Builder.fromByteString arg
                    ++ crlf
    argLen = Builder.fromShow . length
    crlf = Builder.copyByteString "\r\n"
    star = Builder.copyByteString "*"
    dollar = Builder.copyByteString "$"

sendCommand :: forall a m.
       (MonadCommand m)
    => Connection -> CommandRequest a -> m a
sendCommand conn (CommandRequest req) = do
    writeRequest conn req
    resp <- readResponse conn
    case decodeResponse resp of
        Just result -> return result
        Nothing -> liftIO (throwM (DecodeResponseException (Builder.toByteString req) resp))

writeRequest :: forall m.
       (MonadCommand m)
    => Connection -> Request -> m ()
writeRequest conn req = do
    liftIO $ mapM_
        (NS.sendAll (connectionSocket conn))
        (BSL.toChunks (Builder.toLazyByteString req))

readResponse :: forall m.
       (MonadCommand m)
    => Connection -> m Response
readResponse conn = do
    leftover0 <- readIORef (connectionLeftover conn)
    (resp, leftover1) <- liftIO (go (Atto.parse responseParser leftover0))
    writeIORef (connectionLeftover conn) leftover1
    return resp
  where
        go = \case
            Atto.Done leftover resp -> return (resp, leftover)
            Atto.Fail _leftover _context _err ->
                throwM (ProtocolException "Could not parse response") -- TODO better error
            Atto.Partial cont -> do
                bs <- CN.safeRecv (connectionSocket conn) 4096 -- TODO make this follow ClientSettings when we upgrade streaming-commons
                go (cont bs)

-- | Redis Response protocol parser (adapted from hedis)
responseParser :: Parser Response
responseParser = response
  where
    response = choice [simpleString
                      ,integer
                      ,bulkString
                      ,array
                      ,error_]
    simpleString = SimpleString <$> (char '+' *> takeTill isEndOfLine <* endOfLine)
    error_ = Error <$> (char '-' *> takeTill isEndOfLine <* endOfLine)
    integer = Integer <$> (char ':' *> signed decimal <* endOfLine)
    bulkString = BulkString <$> do
        len <- char '$' *> signed decimal <* endOfLine
        if len < 0
            then return Nothing
            else Just <$> Data.Attoparsec.ByteString.Char8.take len <* endOfLine
    array = Array <$> do
        len <- char '*' *> signed decimal <* endOfLine
        if len < 0
            then return Nothing
            else Just <$> count len response
