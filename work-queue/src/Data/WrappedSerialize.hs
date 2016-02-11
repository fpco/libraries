{-# LANGUAGE GeneralizedNewtypeDeriving #-}
-- | Wrapper to hold data that is Binary serializable but not Typeable.
module Data.WrappedBinary where

import Data.Binary
import Data.ByteString
import Data.ByteString.Lazy (fromStrict, toStrict)
import Data.Typeable

newtype WrappedBinary = WrappedBinary ByteString deriving (Binary, Typeable)

unwrapBinary :: Binary a => WrappedBinary -> a
unwrapBinary (WrappedBinary bs) = decode (fromStrict bs)

wrapBinary :: Binary a => a -> WrappedBinary
wrapBinary = WrappedBinary . toStrict . encode
