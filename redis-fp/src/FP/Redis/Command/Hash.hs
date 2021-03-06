{-# LANGUAGE NoImplicitPrelude, OverloadedStrings, ScopedTypeVariables, TypeFamilies,
             DeriveDataTypeable, FlexibleContexts, FlexibleInstances, RankNTypes, GADTs,
             ConstraintKinds, NamedFieldPuns #-}

-- | Redis hash commands.
-- See <http://redis.io/commands#hash>.

module FP.Redis.Command.Hash
    ( hget
    , hmget
    , hset
    , hmset
    , hdel
    , hincrby )
    where

import ClassyPrelude.Conduit
import Data.List.NonEmpty (NonEmpty)

import FP.Redis.Internal
import FP.Redis.Types.Internal

-- | Returns the value associated with field in the hash stored at key.
-- See <http://redis.io/commands/hget>.
hget :: HKey -> HashField -> CommandRequest (Maybe ByteString)
hget key field =
    makeCommand "HGET" [encodeArg key,encodeArg field]

-- | Returns the values associated with the specified fields in the hash stored at key.
-- See <http://redis.io/commands/hmget>.
hmget :: HKey -> NonEmpty HashField -> CommandRequest [Maybe ByteString]
hmget key fields =
    makeCommand "HMGET" (encodeArg key : map encodeArg (toList fields))

-- | Sets field in the hash stored at key to value.
-- See <http://redis.io/commands/hset>.
hset :: HKey -> HashField -> ByteString -> CommandRequest Bool
hset key field value =
    makeCommand "HSET" [encodeArg key,encodeArg field,encodeArg value]

-- | Sets the specified fields to their respective values in the hash stored at key.
-- See <http://redis.io/commands/hmset>.
hmset :: HKey -> NonEmpty (HashField,ByteString) -> CommandRequest ()
hmset key fieldValuePairs =
    makeCommand "HMSET" (encodeArg key : concatMap encodeFieldValue fieldValuePairs)
  where
    encodeFieldValue (field,value) = [encodeArg field,encodeArg value]

-- | Removes the specified fields from the hash stored at key.
-- See <http://redis.io/commands/hdel>.
hdel :: HKey -> NonEmpty HashField -> CommandRequest Int64
hdel key fields =
    makeCommand "HDEL" (encodeArg key : map encodeArg (toList fields))

-- | Increments the number stored at field in the hash stored at key by increment.
-- See <http://redis.io/commands/hincrby>.
hincrby :: HKey -> HashField -> Int64 -> CommandRequest Int64
hincrby key field increment =
    makeCommand "HINCRBY" [encodeArg key, encodeArg field, encodeArg increment]
