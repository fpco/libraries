{-# LANGUAGE NoImplicitPrelude, OverloadedStrings, ScopedTypeVariables, TypeFamilies,
             DeriveDataTypeable, FlexibleContexts, FlexibleInstances, RankNTypes, GADTs,
             ConstraintKinds, NamedFieldPuns, GeneralizedNewtypeDeriving, DeriveGeneric #-}

-- | Redis mutex types.

module FP.Redis.Mutex.Types
    ( MutexToken (..)
    , MutexKey (..)
    , PeriodicPrefix (..)
    , RedisMutexException (..) )
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
