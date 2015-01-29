{-# LANGUAGE NoImplicitPrelude, OverloadedStrings, ScopedTypeVariables, TypeFamilies,
             DeriveDataTypeable, FlexibleContexts, FlexibleInstances, RankNTypes, GADTs,
             ConstraintKinds, NamedFieldPuns #-}

-- | Redis sorted-set commands.
-- See <http://redis.io/commands#sorted_set>.

module FP.Redis.Command.SortedSet
    ( zadd
    , zrem
    , zrange
    , zrangebyscore
    , zscore )
    where

import ClassyPrelude.Conduit hiding (member)

import FP.Redis.Internal
import FP.Redis.Types.Internal

-- | Adds all the specified members with the specified scores to the sorted set stored at key.
-- See <http://redis.io/commands/zadd>.
zadd :: ZKey -> [(Double, ByteString)] -> CommandRequest Int64
zadd key scoreMembers =
    makeCommand "ZADD" (encodeArg key : concatMap encodeScoreMember scoreMembers)
  where
    encodeScoreMember (score,memb) = [encodeArg score,encodeArg memb]

-- | Removes the specified members from the sorted set stored at key.
-- See <http://redis.io/commands/zrem>.
zrem :: ZKey -> [ByteString] -> CommandRequest Int64
zrem key members = makeCommand "ZREM" (encodeArg key : map encodeArg members)

-- | Returns the specified range of elements in the sorted set stored at key.
-- See <http://redis.io/commands/zrange>.
zrange :: ZKey -> Int64 -> Int64 -> Bool -> CommandRequest [ByteString]
zrange key start stop withScores =
    makeCommand "ZRANGE"
                ([encodeArg key,encodeArg start,encodeArg stop] ++
                 if withScores then [encodeArg ("WITHSCORES"::ByteString)]  else [])

-- | Returns all the elements in the sorted set at key with a score between min and max.
-- See <http://redis.io/commands/zrangebyscore>.
zrangebyscore :: ZKey -> Double -> Double -> Bool -> CommandRequest [ByteString]
zrangebyscore key start stop withScores =
    makeCommand "ZRANGEBYSCORE"
                ([encodeArg key,encodeArg start,encodeArg stop] ++
                 if withScores then [encodeArg ("WITHSCORES"::ByteString)]  else [])

-- | Returns the score of the element at key, if it is in the sorted set.
-- See <http://redis.io/commands/zscore>
zscore :: ZKey -> ByteString -> CommandRequest (Maybe Double)
zscore key member = makeCommand "ZSCORE" [encodeArg key, encodeArg member]
