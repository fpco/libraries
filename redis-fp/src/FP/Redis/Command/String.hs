{-# LANGUAGE NoImplicitPrelude, OverloadedStrings, ScopedTypeVariables, TypeFamilies,
             DeriveDataTypeable, FlexibleContexts, FlexibleInstances, RankNTypes, GADTs,
             ConstraintKinds, NamedFieldPuns #-}

-- | Redis string commands.
-- See <http://redis.io/commands#string>.

module FP.Redis.Command.String
    ( set
    , setEx
    , get
    , incr )
    where

import ClassyPrelude.Conduit

import FP.Redis.Internal
import FP.Redis.Types.Internal

-- | Set key to hold the string.
-- See <http://redis.io/commands/set>.
set :: ByteString -- ^ Key
    -> ByteString -- ^ Value
    -> [SetOption] -- ^ Zero or more extra options
    -> CommandRequest Bool
set key val options =
    makeCommand "SET"
                (concat ([encodeArg key,encodeArg val] : map renderOption options))
  where
    renderOption :: SetOption -> [ByteString]
    renderOption (EX seconds) = ["EX",encodeArg seconds]
    renderOption (PX milliseconds) = ["PX",encodeArg milliseconds]
    renderOption NX = ["NX"]
    renderOption XX = ["XX"]

-- | Set key to hold the string value and set key to timeout after a given number of seconds.
-- See <http://redis.io/commands/setex>.
setEx :: ByteString -> Int64 -> ByteString -> CommandRequest ()
setEx key ttl_ val =
    makeCommand "SETEX" [encodeArg key,encodeArg ttl_,encodeArg val]

-- | Get the value of key.
-- See <http://redis.io/commands/get>.
get :: ByteString -> CommandRequest (Maybe ByteString)
get key = makeCommand "GET" [encodeArg key]

-- | Increments the number stored at key by one.
-- See <http://redis.io/commands/incr>.
incr :: ByteString -> CommandRequest Int64
incr key = makeCommand "INCR" [encodeArg key]
