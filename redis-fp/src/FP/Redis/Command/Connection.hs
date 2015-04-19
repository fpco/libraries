{-# LANGUAGE NoImplicitPrelude, OverloadedStrings, ScopedTypeVariables, TypeFamilies,
             DeriveDataTypeable, FlexibleContexts, FlexibleInstances, RankNTypes, GADTs,
             ConstraintKinds, NamedFieldPuns #-}

-- | Redis connection commands.
--See <http://redis.io/commands#connection>.

module FP.Redis.Command.Connection
    ( auth
    , select
    , quit )
    where

import ClassyPrelude.Conduit

import FP.Redis.Internal
import FP.Redis.Types.Internal

-- | Request for authentication in a password-protected Redis server.
-- See <http://redis.io/commands/auth>.
auth :: ByteString -- ^ Password
     -> CommandRequest ()
auth password = makeCommand "AUTH" [encodeArg password]

-- | Select the DB with having the specified zero-based numeric index.
-- See <http://redis.io/commands/select>.
select :: Int64 -- ^ Database index
       -> CommandRequest ()
select database = makeCommand "SELECT" [encodeArg database]

-- | Ask the server to close the connection.
-- In most cases, it is preferable to use 'disconnect', which will ensure a quick and
-- clean disconnection.
-- See <http://redis.io/commands/quit>.
quit :: CommandRequest ()
quit = makeCommand "QUIT" []
