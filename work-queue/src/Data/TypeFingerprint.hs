{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}

module Data.TypeFingerprint where

import qualified Data.ByteString as BS
import           Data.Hashable (Hashable(hash))
import qualified Data.Serialize as B
import           Data.Typeable.Internal
import           Language.Haskell.TH
import           Language.Haskell.TH.TypeHash

newtype TypeFingerprint = TypeFingerprint { unTypeFingerprint :: Int }
    deriving (Eq, Show, B.Serialize, Hashable)

class HasTypeFingerprint a where typeFingerprint :: Proxy a -> TypeFingerprint

instance (HasTypeFingerprint a, HasTypeFingerprint b) => HasTypeFingerprint (a, b) where
    typeFingerprint _ = TypeFingerprint $ hash
        ( typeFingerprint (Proxy :: Proxy a)
        , typeFingerprint (Proxy :: Proxy b)
        )

instance (HasTypeFingerprint a, HasTypeFingerprint b, HasTypeFingerprint c) => HasTypeFingerprint (a, b, c) where
    typeFingerprint _ = TypeFingerprint $ hash
        ( typeFingerprint (Proxy :: Proxy a)
        , typeFingerprint (Proxy :: Proxy b)
        , typeFingerprint (Proxy :: Proxy c)
        )

instance HasTypeFingerprint () where
    typeFingerprint _ = TypeFingerprint (-605663274052664475)

noFingerprint :: TypeFingerprint
noFingerprint = TypeFingerprint 8016980037224343525

typeableFingerprint :: forall b. Typeable b => Proxy b -> TypeFingerprint
typeableFingerprint _ = TypeFingerprint (hash (a, b))
  where
    TypeRep (Fingerprint a b) _ _ _ = typeRep (Proxy :: Proxy b)
{-# DEPRECATED typeableFingerprint "Using Data.Typeable for fingerprints is not recommended (they change when package keys change)" #-}

mkHasTypeFingerprint :: Type -> Q [Dec]
mkHasTypeFingerprint ty =
    [d| instance HasTypeFingerprint $(return ty) where typeFingerprint _ = TypeFingerprint $(thTypeHash ty) |]

mkManyHasTypeFingerprint :: [Q Type] -> Q [Dec]
mkManyHasTypeFingerprint qtys = concat <$> mapM (mkHasTypeFingerprint =<<) qtys
