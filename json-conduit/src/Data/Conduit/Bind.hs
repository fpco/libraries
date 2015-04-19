{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE InstanceSigs #-}

-- | These classes provide an interface for the ability to bind (@>>=@)
-- with Consumers and Producers.
--
-- This is particularly useful when we have newtype wrapper types
-- around conduits, but don't want to allow arbitrary concatenation of
-- these conduits.
module Data.Conduit.Bind where

import Data.Conduit (Consumer, Producer, ConduitM)

-- | 'BindConsumer' instances support binding 'Consumer's on the left
-- and right.  This allows you to have some of the power of a 'Monad'
-- instance for some newtype, but restricted to binding with
-- 'Consumer's.
--
-- This is desireable because for some types we want to disallow
-- arbitrary concatenation of the Conduit's output behavior.  For
-- example, lets say we have a newtype wrapper around 'ConduitM' which
-- is intended to declare that it only emits valid JSON.  JSON
-- appended to JSON isn't usually valid JSON, so we don't want to
-- allow general monadic binding.  However, it should be allowed to
-- bind with 'Consumer's because they don't affect output behavior.
class BindConsumer c i m | c -> i m where
    bindConsumer :: Consumer i m r -> (r -> c a) -> c a
    bindToConsumer :: c a -> (a -> Consumer i m r) -> c r

-- | 'BindProducer' instances support binding 'Producer's on the left
-- and right.  This allows you to have some of the power of a 'Monad'
-- instance for some newtype, but restricted to binding with
-- 'Consumer's.
--
-- This is desireable because for some types we want to disallow
-- arbitrary concatenation of the Conduit's input consumption.  For
-- example, lets say we have a newtype wrapper around 'ConduitM' which
-- is intended to declare that it parses valid JSON.  JSON
-- appended to JSON isn't usually valid JSON, so we don't want to
-- allow general monadic binding.  However, it should be allowed to
-- bind with 'Producer's because they don't affect input behavior.
class BindProducer c m o | c -> m o where
    bindProducer :: Producer m o -> c a -> c a
    bindToProducer :: c a -> (a -> Producer m o) -> c ()
