{-# LANGUAGE NoImplicitPrelude, OverloadedStrings, ScopedTypeVariables, TypeFamilies,
             DeriveDataTypeable, FlexibleContexts, FlexibleInstances, RankNTypes, GADTs,
             ConstraintKinds, NamedFieldPuns #-}

-- | Redis pub/sub commands.
-- See <http://redis.io/commands#pubsub>.
-- This module only exposes the PUBLISH command.
-- 'FP.Redis.PubSub' contains the subscription support.

module FP.Redis.Command.PubSub
    ( publish )
    where

import ClassyPrelude.Conduit

import FP.Redis.Internal
import FP.Redis.Types.Internal

-- | Posts a message to the given channel.
-- See <http://redis.io/commands/publish>
publish :: ByteString -- ^ Channel
        -> ByteString -- ^ Message
        -> CommandRequest Int64
publish channel message =
    makeCommand "PUBLISH" [encodeArg channel,encodeArg message]

