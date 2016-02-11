{-# OPTIONS_GHC -fno-warn-orphans #-}
module Data.Serialize.Orphans () where

import           Data.Serialize

import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import           Control.Monad (liftM, liftM2, liftM3)
import qualified Data.Aeson as A
import           Data.Serialize
import qualified Data.Fixed as Fixed
import qualified Data.HashMap.Lazy as HM
import qualified Data.HashSet as HS
import           Data.Hashable (Hashable)
import qualified Data.List.NonEmpty as NE
import qualified Data.Monoid as Monoid
import qualified Data.Semigroup as Semigroup
import qualified Data.Tagged as Tagged
import qualified Data.Time as Time
import qualified Data.Time.Clock.TAI as Time
import qualified Data.Scientific as S
import qualified Data.Vector.Generic   as G
import qualified Data.Vector.Unboxed   as U
import qualified Data.Vector.Storable  as S
import qualified Data.Vector.Primitive as P
import           Data.Vector (Vector)
import           Foreign.Storable (Storable)

instance Serialize T.Text where
  put = put . T.encodeUtf8
  get = do
    bs <- get
    case T.decodeUtf8' bs of
      Left exn -> fail (show exn)
      Right a -> return a

instance Serialize A.Value where
  get = do
    t <- get :: Get Int
    case t of
      0 -> fmap A.Object get
      1 -> fmap A.Array get
      2 -> fmap A.String get
      3 -> fmap A.Number get
      4 -> fmap A.Bool get
      5 -> return A.Null
      _ -> fail $ "Invalid Value tag: " ++ show t

  put (A.Object v) = put (0 :: Int) >> put v
  put (A.Array v)  = put (1 :: Int) >> put v
  put (A.String v) = put (2 :: Int) >> put v
  put (A.Number v) = put (3 :: Int) >> put v
  put (A.Bool v)   = put (4 :: Int) >> put v
  put A.Null       = put (5 :: Int)


instance  (Hashable k, Eq k, Serialize k, Serialize v) => Serialize (HM.HashMap k v) where
  get = fmap HM.fromList get
  put = put . HM.toList

instance (Hashable v, Eq v, Serialize v) => Serialize (HS.HashSet v) where
  get = fmap HS.fromList get
  put = put . HS.toList

instance Serialize S.Scientific where
  get = liftM2 S.scientific get get
  put s = put (S.coefficient s) >> put (S.base10Exponent s)

instance Serialize b => Serialize (Tagged.Tagged s b) where
  put = put . Tagged.unTagged
  get = fmap Tagged.Tagged get

instance Serialize (Fixed.Fixed a) where
  put (Fixed.MkFixed a) = put a
  get = Fixed.MkFixed `liftM` get

instance Serialize Time.Day where
  get = fmap Time.ModifiedJulianDay get
  put = put . Time.toModifiedJulianDay

instance Serialize Time.UniversalTime where
  get = fmap Time.ModJulianDate get
  put = put . Time.getModJulianDate

instance Serialize Time.DiffTime where
  get = fmap Time.picosecondsToDiffTime get
  put = (put :: Fixed.Pico -> Put)  . realToFrac

instance Serialize Time.UTCTime where
  get = liftM2 Time.UTCTime get get
  put (Time.UTCTime d dt) = put d >> put dt

instance Serialize Time.NominalDiffTime where
  get = fmap realToFrac (get :: Get Fixed.Pico)
  put = (put :: Fixed.Pico -> Put)  . realToFrac

instance Serialize Time.TimeZone where
  get = liftM3 Time.TimeZone get get get
  put (Time.TimeZone m s n) = put m >> put s >> put n

instance Serialize Time.TimeOfDay where
  get = liftM3 Time.TimeOfDay get get get
  put (Time.TimeOfDay h m s) = put h >> put m >> put s

instance Serialize Time.LocalTime where
  get = liftM2 Time.LocalTime get get
  put (Time.LocalTime d tod) = put d >> put tod

-- | /Since: binary-orphans-0.1.4.0/
instance Serialize Time.AbsoluteTime where
  get = fmap (flip Time.addAbsoluteTime Time.taiEpoch) get
  put = put . flip Time.diffAbsoluteTime Time.taiEpoch

-- Monoid

-- | /Since: binary-orphans-0.1.3.0/
instance Serialize a => Serialize (NE.NonEmpty a) where
  get = fmap NE.fromList get
  put = put . NE.toList


-- | Boxed, generic vectors.
instance Serialize a => Serialize (Vector a) where
    put = genericPutVector
    get = genericGetVector
    {-# INLINE get #-}

-- | Unboxed vectors
instance (U.Unbox a, Serialize a) => Serialize (U.Vector a) where
    put = genericPutVector
    get = genericGetVector
    {-# INLINE get #-}

-- | Primitive vectors
instance (P.Prim a, Serialize a) => Serialize (P.Vector a) where
    put = genericPutVector
    get = genericGetVector
    {-# INLINE get #-}

-- | Storable vectors
instance (Storable a, Serialize a) => Serialize (S.Vector a) where
    put = genericPutVector
    get = genericGetVector
    {-# INLINE get #-}

------------------------------------------------------------------------

-- | Deserialize vector using custom parsers.
genericGetVectorWith :: (G.Vector v a, Serialize a)
    => Get Int       -- ^ Parser for vector size
    -> Get a         -- ^ Parser for vector's element
    -> Get (v a)
{-# INLINE genericGetVectorWith #-}
genericGetVectorWith getN getA = do
    n  <- getN
    G.replicateM n getA

-- | Generic put for anything in the G.Vector class which uses custom
--   encoders.
genericPutVectorWith :: (G.Vector v a, Serialize a)
    => (Int -> Put)  -- ^ Encoder for vector size
    -> (a   -> Put)  -- ^ Encoder for vector's element
    -> v a -> Put
{-# INLINE genericPutVectorWith #-}
genericPutVectorWith putN putA v = do
    putN (G.length v)
    G.mapM_ putA v

-- | Generic function for vector deserialization.
genericGetVector :: (G.Vector v a, Serialize a) => Get (v a)
{-# INLINE genericGetVector #-}
genericGetVector = genericGetVectorWith get get

-- | Generic put for anything in the G.Vector class.
genericPutVector :: (G.Vector v a, Serialize a) => v a -> Put
{-# INLINE genericPutVector #-}
genericPutVector = genericPutVectorWith put put