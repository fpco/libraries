{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE OverloadedStrings #-}

module Data.Conduit.Json.Encode.Internal
    ( ValueRenderer(..)
    , PairRenderer(..)
    , unsafeAlreadyEscapedString
    -- * JSON Builder Utilities
    , jsonStringContentBuilder
    , scientificBuilder
    ) where

import           Data.ByteString.Builder
import qualified Data.ByteString.Builder.Scientific as SB
import           Data.Conduit
import qualified Data.Conduit.List as CL
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
import Data.Conduit.Bind (BindConsumer(..))

-- | A 'ValueRenderer' is a wrapper around @Conduit i m Builder@ which
-- always generates a valid JSON value.
newtype ValueRenderer i m r =
    ValueRenderer { unValueRenderer :: ConduitM i Builder m r }
    deriving (Typeable)

-- | A 'PairRenderer' is a wrapper around @Conduit i m Builder@ which
-- always generates a valid JSON key-value pair.
newtype PairRenderer i m r =
    PairRenderer { unPairRenderer :: ConduitM i Builder m r }
    deriving (Typeable)

instance BindConsumer (ValueRenderer i m) i m where
    bindConsumer c f = ValueRenderer $ c >>= unValueRenderer . f
    bindToConsumer vr f = ValueRenderer $ unValueRenderer vr >>= f

instance BindConsumer (PairRenderer i m) i m where
    bindConsumer c f = PairRenderer $ c >>= unPairRenderer . f
    bindToConsumer pr f = PairRenderer $ unPairRenderer pr >>= f

unsafeAlreadyEscapedString :: Monad m => Conduit i m Text -> ValueRenderer i m ()
unsafeAlreadyEscapedString c = ValueRenderer $ do
    yield "\""
    c $= CL.map TE.encodeUtf8Builder
    yield "\""

-- Based on definitions from aeson, modified to target bytestring
-- builder instead of text builder.

jsonStringContentBuilder :: Text -> Builder
jsonStringContentBuilder s = quote s
  where
    quote q = case T.uncons t of
                Nothing      -> TE.encodeUtf8Builder h
                Just (!c,t') -> TE.encodeUtf8Builder h <> (escape c <> quote t')
        where (h,t) = {-# SCC "break" #-} T.break isEscape q
    isEscape c = c == '\"' ||
                 c == '\\' ||
                 c == '<'  ||
                 c == '>'  ||
                 c < '\x20'
    escape '\"' = "\\\""
    escape '\\' = "\\\\"
    escape '\n' = "\\n"
    escape '\r' = "\\r"
    escape '\t' = "\\t"

    -- The following prevents untrusted JSON strings containing </script> or -->
    -- from causing an XSS vulnerability:
    escape '<'  = "\\u003c"
    escape '>'  = "\\u003e"

    escape c
        | c < '\x20' = stringUtf8 $ "\\u" ++ replicate (4 - length h) '0' ++ h
        | otherwise  = charUtf8 c
        where h = showHex (fromEnum c) ""

scientificBuilder :: Scientific -> Builder
scientificBuilder s
    | e < 0     = SB.scientificBuilder s
    | otherwise = SB.formatScientificBuilder SB.Fixed Nothing s
  where
    e = base10Exponent s
