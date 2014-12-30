{-# LANGUAGE NoImplicitPrelude, OverloadedStrings, ScopedTypeVariables, TypeFamilies,
             DeriveDataTypeable, FlexibleContexts, FlexibleInstances, RankNTypes, GADTs,
             ConstraintKinds, NamedFieldPuns #-}

-- | Redis set commands.
-- See <http://redis.io/commands#set>.

module FP.Redis.Command.Set
    ( smembers )
    where

import ClassyPrelude.Conduit

import FP.Redis.Internal
import FP.Redis.Types.Internal

-- | Returns all the members of the set value stored at key.
-- See <http://redis.io/commands/smembers>.
smembers :: ByteString -> CommandRequest [ByteString]
smembers key =
    makeCommand "SMEMBERS" [encodeArg key]
