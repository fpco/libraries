{-# LANGUAGE NoImplicitPrelude, OverloadedStrings, ScopedTypeVariables, TypeFamilies,
             DeriveDataTypeable, FlexibleContexts, FlexibleInstances, RankNTypes, GADTs,
             ConstraintKinds, NamedFieldPuns #-}

-- | Redis list commands.
--See <http://redis.io/commands#list>.

module FP.Redis.Command.List
    ( rpush
    , lpush
    , lrange
    , lrem
    , blpop
    , brpop
    , brpoplpush )
    where

import ClassyPrelude.Conduit

import FP.Redis.Internal
import FP.Redis.Types.Internal

-- | Insert all the specified values at the head of the list stored at key.
-- See <http://redis.io/commands/lpush>.
lpush :: LKey -> ByteString -> CommandRequest Int64
lpush key val = makeCommand "LPUSH" [encodeArg key,encodeArg val]

-- | Insert all the specified values at the tail of the list stored at key.
-- See <http://redis.io/commands/rpush>
rpush :: LKey -> ByteString -> CommandRequest Int64
rpush key val = makeCommand "RPUSH" [encodeArg key,encodeArg val]

-- | BRPOP is a blocking list pop primitive.
-- See <http://redis.io/commands/brpop>.
brpop :: [LKey] -> Seconds -> CommandRequest (Maybe (Key,ByteString))
brpop keys timeout =
    makeCommand "BRPOP" (map encodeArg keys ++ [encodeArg timeout])

-- | BLPOP is a blocking list pop primitive.
-- See <http://redis.io/commands/blpop>.
blpop :: [LKey] -> Seconds -> CommandRequest (Maybe (Key,ByteString))
blpop keys timeout =
    makeCommand "BLPOP" (map encodeArg keys ++ [encodeArg timeout])

-- | BRPOPLPUSH is the blocking variant of RPOPLPUSH.
-- See <http://redis.io/commands/brpoplpush>.
brpoplpush :: LKey -> LKey -> Seconds -> CommandRequest (Maybe ByteString)
brpoplpush source destination timeout =
    makeCommand "BRPOPLPUSH"
                [encodeArg source
                ,encodeArg destination
                ,encodeArg timeout]

-- | Returns the specified elements of the list stored at key.
-- See <http://redis.io/commands/lrange>.
lrange :: LKey -> Int64 -> Int64 -> CommandRequest [ByteString]
lrange key start stop =
    makeCommand "LRANGE" [encodeArg key,encodeArg start,encodeArg stop]

-- | Removes the first count occurrences of elements equal to value from the list stored at key.
-- See <http://redis.io/commands/lrem>.
lrem :: LKey -> Int64 -> ByteString -> CommandRequest Int64
lrem key count_ value =
    makeCommand "LREM" [encodeArg key,encodeArg count_,encodeArg value]
