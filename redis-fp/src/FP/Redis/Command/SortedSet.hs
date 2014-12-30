{-# LANGUAGE NoImplicitPrelude, OverloadedStrings, ScopedTypeVariables, TypeFamilies,
             DeriveDataTypeable, FlexibleContexts, FlexibleInstances, RankNTypes, GADTs,
             ConstraintKinds, NamedFieldPuns #-}

-- | Redis sorted-set commands.
-- See <http://redis.io/commands#sorted_set>.

module FP.Redis.Command.SortedSet
    ( zadd
    , zrem
    , zrange )
    where

import ClassyPrelude.Conduit

import FP.Redis.Internal
import FP.Redis.Types.Internal

-- | Adds all the specified members with the specified scores to the sorted set stored at key.
-- See <http://redis.io/commands/zadd>.
zadd :: ByteString -> [(Double, ByteString)] -> CommandRequest Int64
zadd key scoreMembers =
    makeCommand "ZADD" (encodeArg key : concatMap encodeScoreMember scoreMembers)
  where
    encodeScoreMember (score,memb) = [encodeArg score,encodeArg memb]

-- | Removes the specified members from the sorted set stored at key.
-- See <http://redis.io/commands/zrem>.
zrem :: ByteString -> [ByteString] -> CommandRequest Int64
zrem key members = makeCommand "ZREM" (key : map encodeArg members)

-- | Returns the specified range of elements in the sorted set stored at key.
-- See <http://redis.io/commands/zrange>.
zrange :: ByteString -> Int64 -> Int64 -> Bool -> CommandRequest [ByteString]
zrange key start stop withScores =
    makeCommand "ZRANGE"
                ([encodeArg key,encodeArg start,encodeArg stop] ++
                 if withScores then [encodeArg ("WITHSCORES"::ByteString)]  else [])

