{-# LANGUAGE NoImplicitPrelude, OverloadedStrings, ScopedTypeVariables, TypeFamilies,
             DeriveDataTypeable, FlexibleContexts, FlexibleInstances, RankNTypes, GADTs,
             ConstraintKinds, NamedFieldPuns #-}

-- | Redis mutex types.

module FP.Redis.Mutex.Types
    ( MutexToken (..)
    , RedisMutexException (..) )
    where

import ClassyPrelude.Conduit

-- | Mutex token, used to identify the owner of a mutex.
newtype MutexToken = MutexToken ByteString
    deriving (Show)

-- | Exception thrown by "FP.Redis".
data RedisMutexException
    = IncorrectRedisMutexException
    deriving (Show, Typeable)
instance Exception RedisMutexException
