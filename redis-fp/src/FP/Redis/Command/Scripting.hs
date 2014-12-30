{-# LANGUAGE NoImplicitPrelude, OverloadedStrings, ScopedTypeVariables, TypeFamilies,
             DeriveDataTypeable, FlexibleContexts, FlexibleInstances, RankNTypes, GADTs,
             ConstraintKinds, NamedFieldPuns #-}

-- | Redis scripting commands.
-- See <http://redis.io/commands#scripting>.

module FP.Redis.Command.Scripting
    ( eval )
    where

import ClassyPrelude.Conduit

import FP.Redis.Internal
import FP.Redis.Types.Internal

-- | EVAL and EVALSHA are used to evaluate scripts using the Lua interpreter built into Redis starting from version 2.6.0.
-- Response type depends on the script being evauated.
-- See <http://redis.io/commands/eval>.
-- TODO SHOULD: use EVALSHA for previously sent commands
-- TODO OPTIONAL: Track EVALs and resubmit after reconnection so that EVALSHA works reliably.
-- TODO OPTIONAL: a DSL to generate Lua code in a typesafe manner
eval :: (Result a) => ByteString -> [ByteString] -> [ByteString] -> CommandRequest a
eval script keys argv =
    makeCommand "EVAL"
                (concat [[encodeArg script
                         ,encodeArg (fromIntegral (length keys) :: Int64)]
                        ,map encodeArg keys
                        ,map encodeArg argv])
