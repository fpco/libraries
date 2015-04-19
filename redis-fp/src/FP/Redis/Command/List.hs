{-# LANGUAGE NoImplicitPrelude, OverloadedStrings, ScopedTypeVariables, TypeFamilies,
             DeriveDataTypeable, FlexibleContexts, FlexibleInstances, RankNTypes, GADTs,
             ConstraintKinds, NamedFieldPuns #-}

-- | Redis list commands.
--See <http://redis.io/commands#list>.

module FP.Redis.Command.List
    ( rpush
    , lpush
    , rpop
    , lpop
    , lrange
    , lrem
    , llen
    , blpop
    , brpop
    , brpoplpush
    , rpoplpush )
    where

import ClassyPrelude.Conduit hiding (keys)
import Data.List.NonEmpty (NonEmpty)

import FP.Redis.Internal
import FP.Redis.Types.Internal

-- | Insert all the specified values at the head of the list stored at key.
-- See <http://redis.io/commands/lpush>.
lpush :: LKey -> NonEmpty ByteString -> CommandRequest Int64
lpush key vals = makeCommand "LPUSH" (encodeArg key : map encodeArg (toList vals))

-- | Insert all the specified values at the tail of the list stored at key.
-- See <http://redis.io/commands/rpush>
rpush :: LKey -> NonEmpty ByteString -> CommandRequest Int64
rpush key vals = makeCommand "RPUSH" (encodeArg key : map encodeArg (toList vals))

-- | Removes and returns the first element of the list stored at key.
-- See <http://redis.io/commands/lpop>
lpop :: LKey -> CommandRequest (Maybe ByteString)
lpop key = makeCommand "LPOP" [encodeArg key]

-- | Removes and returns the last element of the list stored at key.
-- See <http://redis.io/commands/rpop>
rpop :: LKey -> CommandRequest (Maybe ByteString)
rpop key = makeCommand "RPOP" [encodeArg key]

-- | BRPOP is a blocking list pop primitive.
-- See <http://redis.io/commands/brpop>.
brpop :: NonEmpty LKey -> Seconds -> CommandRequest (Maybe (Key,ByteString))
brpop keys timeout =
    makeCommand "BRPOP" (map encodeArg (toList keys) ++ [encodeArg timeout])

-- | BLPOP is a blocking list pop primitive.
-- See <http://redis.io/commands/blpop>.
blpop :: NonEmpty LKey -> Seconds -> CommandRequest (Maybe (Key,ByteString))
blpop keys timeout =
    makeCommand "BLPOP" (map encodeArg (toList keys) ++ [encodeArg timeout])

-- | BRPOPLPUSH is the blocking variant of RPOPLPUSH.
-- See <http://redis.io/commands/brpoplpush>.
brpoplpush :: LKey -> LKey -> Seconds -> CommandRequest (Maybe ByteString)
brpoplpush source destination timeout =
    makeCommand "BRPOPLPUSH"
                [encodeArg source
                ,encodeArg destination
                ,encodeArg timeout]

-- | Atmoically returns and removes the last element (tail) of the list stored at source, and pushes the element at the first element (head) of the list stored at destination.
--  See <http://redis.io/commands/rpoplpush>.
rpoplpush :: LKey -> LKey -> CommandRequest (Maybe ByteString)
rpoplpush source destination =
    makeCommand "RPOPLPUSH"
                [encodeArg source
                ,encodeArg destination]

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

-- | Returns the length of the list stored at key.
-- See <http://redis.io/commands/llen>.
llen :: LKey -> CommandRequest Int64
llen key = makeCommand "LLEN" [encodeArg key]
