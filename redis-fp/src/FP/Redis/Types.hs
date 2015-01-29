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
    , VKey (..)
    , LKey (..)
    , HKey (..)
    , SKey (..)
    , ZKey (..)
    , Channel (..)
    , HashField (..)
    , Seconds (..)
    , Milliseconds (..)
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
