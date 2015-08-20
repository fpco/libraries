{-# LANGUAGE DeriveDataTypeable, GeneralizedNewtypeDeriving, CPP #-}

{- |
This module defines 'Binary' and 'Hashable' instances for 'TypeRep'. These are defined on a newtype of 'TypeRep', namely 'ConcreteTypeRep', for two purposes:

  * to avoid making orphan instances

  * the 'Hashable' instance for 'ConcreteTypeRep' may not be pure enough for some people's tastes.

As usual with 'Typeable', this module will typically be used with some variant of @Data.Dynamic@. Two possible uses of this module are:

  * making hashmaps: @HashMap 'ConcreteTypeRep' Dynamic@

  * serializing @Dynamic@s.

-}

module Data.ConcreteTypeRep (
  ConcreteTypeRep,
  cTypeOf,
  toTypeRep,
  fromTypeRep,
 ) where

import Data.Typeable
#if MIN_VERSION_base(4, 4, 0)
import Data.Typeable.Internal
import GHC.Fingerprint.Type
#endif

import Data.Hashable
import Data.Binary

import System.IO.Unsafe

import Control.Applicative((<$>))

-- | Abstract type providing the functionality of 'TypeRep', but additionally supporting hashing and serialization.
--
-- The 'Eq' instance is just the 'Eq' instance for 'TypeRep', so an analogous guarantee holds: @'cTypeOf' a == 'cTypeOf' b@ if and only if @a@ and @b@ have the same type.
-- The hashing and serialization functions preserve this equality.
newtype ConcreteTypeRep = CTR { unCTR :: TypeRep } deriving(Eq, Typeable)

-- | \"Concrete\" version of 'typeOf'.
cTypeOf :: Typeable a => a -> ConcreteTypeRep
cTypeOf = fromTypeRep . typeOf

-- | Converts to the underlying 'TypeRep'
toTypeRep :: ConcreteTypeRep -> TypeRep
toTypeRep = unCTR

-- | Converts from the underlying 'TypeRep'
fromTypeRep :: TypeRep -> ConcreteTypeRep
fromTypeRep = CTR

-- show as a normal TypeRep
instance Show ConcreteTypeRep where
  showsPrec i = showsPrec i . unCTR


-- | This instance is guaranteed to be consistent for a single run of the program, but not for multiple runs.
instance Hashable ConcreteTypeRep where
#if MIN_VERSION_base(4,8,0)
  hashWithSalt salt (CTR (TypeRep (Fingerprint w1 w2) _ _ _)) = salt `hashWithSalt` w1 `hashWithSalt` w2
#elif MIN_VERSION_base(4, 4, 0)
  hashWithSalt salt (CTR (TypeRep (Fingerprint w1 w2) _ _)) = salt `hashWithSalt` w1 `hashWithSalt` w2
#else
  hashWithSalt salt ctr = hashWithSalt salt (unsafePerformIO . typeRepKey . toTypeRep $ ctr)
#endif

------------- serialization: this uses Gökhan San's construction, from
---- http://www.mail-archive.com/haskell-cafe@haskell.org/msg41134.html
toTyConRep :: TyCon -> TyConRep
fromTyConRep :: TyConRep -> TyCon
#if MIN_VERSION_base(4, 4, 0)
type TyConRep = (String, String, String)
toTyConRep (TyCon _ pack mod name) = (pack, mod, name)
fromTyConRep (pack, mod, name) = mkTyCon3 pack mod name
#else
type TyConRep = String
toTyConRep = tyConString
fromTyConRep = mkTyCon
#endif

newtype SerialRep = SR (TyConRep, [SerialRep]) deriving(Binary)

toSerial :: ConcreteTypeRep -> SerialRep
toSerial (CTR t) =
  case splitTyConApp t of
    (con, args) -> SR (toTyConRep con, map (toSerial . CTR) args)

fromSerial :: SerialRep -> ConcreteTypeRep
fromSerial (SR (con, args)) = CTR $ mkTyConApp (fromTyConRep con) (map (unCTR . fromSerial) args)

instance Binary ConcreteTypeRep where
  put = put . toSerial
  get = fromSerial <$> get
