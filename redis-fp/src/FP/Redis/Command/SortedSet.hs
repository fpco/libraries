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
    , zscore
    , zincrby )
    where

import ClassyPrelude.Conduit hiding (member)
import Data.List.NonEmpty (NonEmpty)

import FP.Redis.Internal
import FP.Redis.Types.Internal

-- | Adds all the specified members with the specified scores to the sorted set stored at key.
-- See <http://redis.io/commands/zadd>.
zadd :: ZKey -> NonEmpty (Double, ByteString) -> CommandRequest Int64
zadd key scoreMembers =
    makeCommand "ZADD" (encodeArg key : concatMap encodeScoreMember (toList scoreMembers))
  where
    encodeScoreMember (score,memb) = [encodeArg score,encodeArg memb]

-- | Removes the specified members from the sorted set stored at key.
-- See <http://redis.io/commands/zrem>.
zrem :: ZKey -> NonEmpty ByteString -> CommandRequest Int64
zrem key members = makeCommand "ZREM" (encodeArg key : map encodeArg (toList members))

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

zincrby ::
       ZKey
    -> Double
    -- ^ Increment
    -> ByteString
    -- ^ Member
    -> CommandRequest Double
    -- ^ New score
zincrby key incr member =
    makeCommand "ZINCRBY" [encodeArg key, encodeArg incr, encodeArg member]
