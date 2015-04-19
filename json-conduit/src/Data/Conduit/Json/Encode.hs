{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE DeriveDataTypeable #-}

module Data.Conduit.Json.Encode
    (
    -- * Types
      ValueRenderer, unValueRenderer
    , PairRenderer
    -- * Rendering
    , renderBytes
    , renderText
    -- * Building
    , string
    , number
    , object
    , complexObject
    , pair
    , array
    , bool
    , null
    -- * Re-export of useful utilities
    , BindConsumer (..)
    ) where

import           Data.ByteString (ByteString)
import           Data.ByteString.Builder
import qualified Data.ByteString.Builder.Scientific as SB
import           Data.Conduit
import           Data.Conduit.Bind
import           Data.Conduit.ByteString.Builder (builderToByteString)
import           Data.Conduit.Json.Encode.Internal
import qualified Data.Conduit.List as CL
import qualified Data.Conduit.Text as CT
import           Data.Foldable (forM_)
import           Data.Function (fix)
import           Data.Monoid
import           Data.Scientific (Scientific, base10Exponent)
import           Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import           Data.Typeable (Typeable)
import           Numeric (showHex)
import           Prelude hiding (null)

-- renderBytes :: (Monad m, PrimMonad m) => ValueRenderer i m () -> Conduit i m ByteString
renderBytes (ValueRenderer c) = c =$= builderToByteString

-- renderText :: (Monad m, PrimMonad m) => ValueRenderer i m () -> Conduit i m ByteString
renderText vr = renderBytes vr =$= CT.decode CT.utf8

string :: Monad m => Conduit i m Text -> ValueRenderer i m ()
string contents = ValueRenderer $ do
    yield "\""
    contents $= CL.map jsonStringContentBuilder
    yield "\""

number :: Monad m => Scientific -> ValueRenderer i m ()
number = ValueRenderer . yield . scientificBuilder

object
    :: Monad m
    => [(Text, ValueRenderer i m ())]
    -> ValueRenderer i m ()
object pairs = ValueRenderer $ do
    yield "{"
    case pairs of
        [] -> return ()
        ((k0, v0) : xs) -> do
            unPairRenderer $ pair (yield k0) v0
            forM_ xs $ \(k, v) -> do
                yield ","
                unPairRenderer $ pair (yield k) v
    yield "}"

complexObject
    :: Monad m
    => Consumer i m (Maybe s)
    -> (s -> PairRenderer i m ())
    -> ValueRenderer i m ()
complexObject consumer pr = ValueRenderer $ do
    yield "{"
    commaSep consumer (unPairRenderer . pr)
    yield "}"

pair :: Monad m
     => Conduit i m Text
     -> ValueRenderer i m ()
     -> PairRenderer i m ()
pair k v = PairRenderer $ do
    unValueRenderer (string k)
    yield ":"
    unValueRenderer v

array :: Monad m
      => Consumer i m (Maybe s)
      -> (s -> ValueRenderer i m ())
      -> ValueRenderer i m ()
array consumer v = ValueRenderer $ do
    yield "["
    commaSep consumer (unValueRenderer . v)
    yield "]"

bool :: Monad m => Bool -> ValueRenderer i m ()
bool True = ValueRenderer $ yield "true"
bool False = ValueRenderer $ yield "false"

null :: Monad m => ValueRenderer i m ()
null = ValueRenderer $ yield "null"

commaSep
    :: Monad m
    => Consumer i m (Maybe s)
    -> (s -> Conduit i m Builder)
    -> Conduit i m Builder
commaSep consumer c = do
    ms0 <- consumer
    forM_ ms0 $ \s0 -> do
        c s0
        fix $ \loop -> do
            ms <- consumer
            forM_ ms $ \s -> yield "," >> c s >> loop
