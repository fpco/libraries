{-# LANGUAGE NoImplicitPrelude, OverloadedStrings, ScopedTypeVariables, TypeFamilies,
             DeriveDataTypeable, FlexibleContexts, FlexibleInstances, RankNTypes, GADTs,
             ConstraintKinds, NamedFieldPuns #-}

-- | Redis generic commands.
--See <http://redis.io/commands#generic>.

module FP.Redis.Command.Generic
    ( del
    , expire
    , ttl
    , FP.Redis.Command.Generic.keys
    , exists)
    where

import ClassyPrelude.Conduit
import Data.List.NonEmpty (NonEmpty)

import FP.Redis.Internal
import FP.Redis.Types.Internal

-- | Removes the specified keys.
-- See <http://redis.io/commands/del>.
del :: NonEmpty Key -> CommandRequest Int64
del ks = makeCommand "DEL" (map encodeArg (toList ks))

-- | Set a timeout on key.
-- See <http://redis.io/commands/expire>.
expire :: Key -> Seconds -> CommandRequest Bool
expire key seconds = makeCommand "EXPIRE" [encodeArg key,encodeArg seconds]

-- | Returns the remaining time to live of a key that has a timeout.
-- See <http://redis.io/commands/ttl>.
ttl :: Key -> CommandRequest Seconds
ttl key = makeCommand "TTL" [encodeArg key]

-- | Returns all keys matching pattern.
-- See <http://redis.io/commands/keys>.
keys :: ByteString -> CommandRequest [Key]
keys pattern = makeCommand "KEYS" [encodeArg pattern]

-- | Returns if key exists.
-- See <http://redis.io/commands/EXISTS>
exists :: Key -> CommandRequest Bool
exists key = makeCommand "EXISTS" [encodeArg key]
