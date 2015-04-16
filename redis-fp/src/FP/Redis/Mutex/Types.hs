{-# LANGUAGE NoImplicitPrelude, OverloadedStrings, ScopedTypeVariables, TypeFamilies,
             DeriveDataTypeable, FlexibleContexts, FlexibleInstances, RankNTypes, GADTs,
             ConstraintKinds, NamedFieldPuns, GeneralizedNewtypeDeriving, DeriveGeneric #-}

-- | Redis mutex types.

module FP.Redis.Mutex.Types
    ( MutexToken (..)
    , MutexKey (..)
    , PeriodicPrefix (..)
    , RedisMutexException (..)
    , MutexSettings (..)
    , defaultMutexSettings )
    where

import ClassyPrelude.Conduit
import FP.Redis.Types
import Data.Data (Data)

-- | Mutex token, used to identify the owner of a mutex.
newtype MutexToken = MutexToken ByteString
    deriving (Eq, Show, Data, Typeable, Generic)

-- | Key which is known to refer to a "FP.Redis.Mutex"
newtype MutexKey = MutexKey { unMutexKey :: Key }
    deriving (Eq, Show, Ord, IsString, Data, Typeable, Generic)

-- | Exception thrown by "FP.Redis.Mutex".
data RedisMutexException
    = IncorrectRedisMutexException
    deriving (Show, Data, Typeable, Generic)
instance Exception RedisMutexException

-- | Prefix used for constructing keys for use by
-- 'FP.Redis.Mutex.periodicActionWrapped' /
-- 'FP.Redis.Mutex.periodicActionEx'.
newtype PeriodicPrefix = PeriodicPrefix { unPeriodicPrefix :: ByteString }
    deriving (Eq, Show, Ord, IsString, Data, Typeable, Generic)

-- | Settings for holding a mutex.
data MutexSettings = MutexSettings
    { mutexKey :: MutexKey
    -- ^ Mutex key.
    , mutexRefresh :: Seconds
    -- ^ Refresh interval - how often the mutex is set.
    , mutexTtl :: Seconds
    -- ^ Mutex time-to-live.  If the process exits without
    -- cleaning up, or gets disconnected from redis, this is
    -- how long other processes will need to wait until the
    -- mutex is available.
    --
    -- In order for this function to guarantee that it will
    -- either hold the mutex or throw an error, this needs to
    -- be larger than the maximum time spent retrying.
    } deriving (Eq, Show, Data, Typeable, Generic)

-- | The default settings are to refresh the mutex every 5 seconds, with
-- a TTL of 90 seconds.
defaultMutexSettings :: MutexKey -> MutexSettings
defaultMutexSettings key = MutexSettings
    { mutexKey = key
    , mutexRefresh = Seconds 5
    , mutexTtl = Seconds 90
    }
