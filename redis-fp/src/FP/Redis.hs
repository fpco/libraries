{-# LANGUAGE NoImplicitPrelude, OverloadedStrings, ScopedTypeVariables, TypeFamilies,
             DeriveDataTypeable, FlexibleContexts, FlexibleInstances, RankNTypes, GADTs,
             ConstraintKinds, NamedFieldPuns #-}

-- | Alternative Redis client.  Some differences from hedis:
--
--     * No connection pool.  Instead, can multiplex a single connection to the server between
--       multiple threads.
--     * Uses conduit for streaming and async for threading.
--     * Does /not/ support transactions.  Using @EVAL*@ to run a Lua script on the Redis server
--       is supported and is more powerful than transactions.

module FP.Redis
    ( module FP.Redis.Types
    , module FP.Redis.Connection
    , module FP.Redis.Command
    , module FP.Redis.PubSub )
    where

import FP.Redis.Types
import FP.Redis.Connection
import FP.Redis.Command
import FP.Redis.PubSub
