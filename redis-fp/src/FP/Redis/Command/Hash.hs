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
    , hdel )
    where

import ClassyPrelude.Conduit

import FP.Redis.Internal
import FP.Redis.Types.Internal

-- | Returns the value associated with field in the hash stored at key.
-- See <http://redis.io/commands/hget>.
hget :: HKey -> HashField -> CommandRequest (Maybe ByteString)
hget key field =
    makeCommand "HGET" [encodeArg key,encodeArg field]

-- | Returns the values associated with the specified fields in the hash stored at key.
-- See <http://redis.io/commands/hmget>.
hmget :: HKey -> [HashField] -> CommandRequest [Maybe ByteString]
hmget key fields =
    makeCommand "HMGET" (encodeArg key : map encodeArg fields)

-- | Sets field in the hash stored at key to value.
-- See <http://redis.io/commands/hset>.
hset :: HKey -> HashField -> ByteString -> CommandRequest Bool
hset key field value =
    makeCommand "HSET" [encodeArg key,encodeArg field,encodeArg value]

-- | Sets the specified fields to their respective values in the hash stored at key.
-- See <http://redis.io/commands/hmset>.
hmset :: HKey -> [(HashField,ByteString)] -> CommandRequest ()
hmset key fieldValuePairs =
    makeCommand "HMSET" (encodeArg key : concatMap encodeFieldValue fieldValuePairs)
  where
    encodeFieldValue (field,value) = [encodeArg field,encodeArg value]

-- | Removes the specified fields from the hash stored at key.
-- See <http://redis.io/commands/hdel>.
hdel :: HKey -> [HashField] -> CommandRequest Int64
hdel key fields =
    makeCommand "HDEL" (encodeArg key : map encodeArg fields)
