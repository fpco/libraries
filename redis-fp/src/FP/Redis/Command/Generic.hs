{-# LANGUAGE NoImplicitPrelude, OverloadedStrings, ScopedTypeVariables, TypeFamilies,
             DeriveDataTypeable, FlexibleContexts, FlexibleInstances, RankNTypes, GADTs,
             ConstraintKinds, NamedFieldPuns #-}

-- | Redis generic commands.
--See <http://redis.io/commands#generic>.

module FP.Redis.Command.Generic
    ( del
    , expire
    , ttl )
    where

import ClassyPrelude.Conduit

import FP.Redis.Internal
import FP.Redis.Types.Internal

-- | Removes the specified keys.
-- See <http://redis.io/commands/del>.
del :: [Key] -> CommandRequest Int64
del keys = makeCommand "DEL" (map encodeArg keys)

-- | Set a timeout on key.
-- See <http://redis.io/commands/expire>.
expire :: Key -> Seconds -> CommandRequest Bool
expire key seconds = makeCommand "EXPIRE" [encodeArg key,encodeArg seconds]

-- | Returns the remaining time to live of a key that has a timeout.
-- See <http://redis.io/commands/ttl>.
ttl :: Key -> CommandRequest Seconds
ttl key = makeCommand "TTL" [encodeArg key]
