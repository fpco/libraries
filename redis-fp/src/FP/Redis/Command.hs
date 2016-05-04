{-# LANGUAGE NoImplicitPrelude, OverloadedStrings, ScopedTypeVariables, TypeFamilies,
             DeriveDataTypeable, FlexibleContexts, FlexibleInstances, RankNTypes, GADTs,
             ConstraintKinds, NamedFieldPuns #-}

-- | Redis commands.

module FP.Redis.Command
    ( module FP.Redis.Command.Connection
    , module FP.Redis.Command.Generic
    , module FP.Redis.Command.Hash
    , module FP.Redis.Command.List
    , module FP.Redis.Command.PubSub
    , module FP.Redis.Command.Scripting
    , module FP.Redis.Command.Set
    , module FP.Redis.Command.SortedSet
    , module FP.Redis.Command.String
    , runCommand
    , runCommand_
    -- , runCommands
    -- , runCommands_
    , sendCommand
    , makeCommand
    ) where

-- TODO SHOULD: Improve blocking operations support (use connection pool, special BlockingCommand type?)
-- TODO OPTIONAL: Track EVALs and resubmit after reconnection so that EVALSHA works reliably.

import ClassyPrelude.Conduit hiding (sequence)

import FP.Redis.Command.Connection
import FP.Redis.Command.Generic
import FP.Redis.Command.Hash
import FP.Redis.Command.List
import FP.Redis.Command.PubSub
import FP.Redis.Command.Scripting
import FP.Redis.Command.Set
import FP.Redis.Command.SortedSet
import FP.Redis.Command.String
import FP.Redis.Internal
import FP.Redis.Types.Internal

-- | Send a command and discard the response.
runCommand_ :: (MonadCommand m)
           => Connection -- ^ Connection
           -> CommandRequest a -- ^ Command to run
           -> m ()
runCommand_ conn request = void (runCommand conn request)

-- | Send a command and return the response.
runCommand :: (MonadCommand m)
           => Connection -- ^ Connection
           -> CommandRequest a -- ^ Command to run
           -> m a
runCommand connection request = sendCommand connection request

{-
-- | Send multiple commands and discard the responses.  The commands will be pipelined.
runCommands_ :: (Traversable t, MonadCommand m)
            => Connection -- ^ Connection
            -> t (CommandRequest a) -- ^ Commands to run
            -> m () -- ^ The commands' responses
runCommands_ conn cmds = do
    _ <- runCommands conn (map ignoreResult cmds)
    return ()

-- | Send multiple commands and return the responses.  The commands will be pipelined.
runCommands :: (Traversable t, MonadCommand m)
            => Connection -- ^ Connection
            -> t (CommandRequest a) -- ^ Commands to run
            -> m (t a) -- ^ The commands' responses
runCommands connection commands = sequence =<< mapM (sendCommand connection) commands

-- | Send commands but do not await the responses.
sendCommand :: (MonadCommand m, MonadIO n)
            => Connection -- ^ Connection
            -> CommandRequest a -- ^ Commands to send
            -> m (n a) -- ^ An action which will return the command's response
sendCommand connection command = do
    (reqs, respAction) <- commandToRequestPair command
    mapM_ (sendRequest connection) reqs
    return respAction
-}