{-# LANGUAGE NoImplicitPrelude, OverloadedStrings, ScopedTypeVariables, TypeFamilies,
             DeriveDataTypeable, FlexibleContexts, FlexibleInstances, RankNTypes, GADTs,
             ConstraintKinds, NamedFieldPuns #-}

-- | Redis set commands.
-- See <http://redis.io/commands#set>.

module FP.Redis.Command.Set
    ( smembers
    , sadd
    , srem
    , sismember )
    where

import ClassyPrelude.Conduit

import FP.Redis.Internal
import FP.Redis.Types.Internal

-- | Returns all the members of the set value stored at key.
-- See <http://redis.io/commands/smembers>.
smembers :: Key -> CommandRequest [ByteString]
smembers key =
    makeCommand "SMEMBERS" [encodeArg key]

-- | Add the specified members to the set stored at key.
-- See <http://redis.io/commands/sadd>.
sadd :: Key -> [ByteString] -> CommandRequest Int64
sadd key members =
    makeCommand "SADD" (encodeArg key : map encodeArg members)

-- | Remove the specified members from the set stored at key.
-- See <http://redis.io/commands/srem>.
srem :: Key -> [ByteString] -> CommandRequest Int64
srem key members =
    makeCommand "SREM" (encodeArg key : map encodeArg members)

-- | Returns if member is a member of the set stored at key.
-- See <http://redis.io/commands/sismember>.
sismember :: Key -> ByteString -> CommandRequest Bool
sismember key member =
    makeCommand "SISMEMBER" [encodeArg key, encodeArg member]
