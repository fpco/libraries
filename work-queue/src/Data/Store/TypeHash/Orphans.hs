{-# OPTIONS_GHC -fno-warn-orphans #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE DataKinds #-}
module Data.Store.TypeHash.Orphans where

import Data.Store.TypeHash.Internal (combineTypeHashes)
import Data.Store.TypeHash
import           Data.Proxy (Proxy(..))

instance (HasTypeHash a, HasTypeHash b) => HasTypeHash (a, b) where
    typeHash _ = combineTypeHashes
        [ typeHash (Proxy :: Proxy a)
        , typeHash (Proxy :: Proxy b)
        ]

instance (HasTypeHash a, HasTypeHash b, HasTypeHash c) => HasTypeHash (a, b, c) where
    typeHash _ = combineTypeHashes
        [ typeHash (Proxy :: Proxy a)
        , typeHash (Proxy :: Proxy b)
        , typeHash (Proxy :: Proxy c)
        ]

$(mkHasTypeHash =<< [t| () |])
