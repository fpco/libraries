{-# LANGUAGE GeneralizedNewtypeDeriving #-}
-- | Wrapper to hold data that is Serialize serializable but not Typeable.
module Data.WrappedSerialize where

import Data.Serialize
import Data.ByteString
import Data.ByteString.Lazy (fromStrict, toStrict)
import Data.Typeable
import Control.Monad (guard)

newtype WrappedSerialize = WrappedSerialize ByteString deriving (Serialize, Typeable)

unwrapSerialize :: Serialize a => WrappedSerialize -> a
unwrapSerialize (WrappedSerialize bs) = case runGet (get <* (guard =<< isEmpty)) bs of
  Left err -> error ("unwrapSerialize: could not decode: " ++ err)
  Right x -> x

wrapSerialize :: Serialize a => a -> WrappedSerialize
wrapSerialize = WrappedSerialize . encode
