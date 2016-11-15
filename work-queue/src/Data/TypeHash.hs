{-# OPTIONS_GHC -fno-warn-orphans #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE DataKinds #-}
module Data.TypeHash
    ( Data.Store.TypeHash.Tagged(..)
    , Data.Store.TypeHash.TypeHash
    , Data.Store.TypeHash.HasTypeHash(..)
    -- * TH for generating HasTypeHash instances
    , mkHasTypeHash
    , mkManyHasTypeHash
    ) where

import qualified Data.Store.TypeHash
import           Data.Store.TypeHash hiding (mkHasTypeHash, mkManyHasTypeHash)
import           Language.Haskell.TH
import           Data.Store.TypeHash.Internal (combineTypeHashes)
import           Data.Proxy (Proxy(..))

mkHasTypeHash :: Type -> Q [Dec]
mkHasTypeHash = Data.Store.TypeHash.mkHasTypeHash

mkManyHasTypeHash :: [Q Type] -> Q [Dec]
mkManyHasTypeHash = Data.Store.TypeHash.mkManyHasTypeHash

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

$(Data.Store.TypeHash.mkHasTypeHash =<< [t| () |])
