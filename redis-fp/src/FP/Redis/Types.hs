{-# LANGUAGE NoImplicitPrelude, OverloadedStrings, ScopedTypeVariables, TypeFamilies,
             DeriveDataTypeable, FlexibleContexts, FlexibleInstances, RankNTypes, GADTs,
             ConstraintKinds, NamedFieldPuns #-}

-- | Redis types.

module FP.Redis.Types
    ( -- * Connection
      ConnectInfo (..)
    , Connection
      -- * Commands
    , SetOption (..)
    , CommandRequest
    , Argument (..)
    , Response (..)
    , Result (..)
      -- * Newtype wrappers
    , Key (..)
    , Channel (..)
    , HashField (..)
    , TimeoutSeconds (..)
    , TimeoutMilliseconds (..)
      -- * Pub/sub subscriptions
    , SubscriptionConnection
    , SubscriptionRequest
    , Message (..)
      -- * Exceptions
    , RedisException (..)
      -- * Monads
    , MonadConnect
    , MonadCommand )
    where

import FP.Redis.Types.Internal
