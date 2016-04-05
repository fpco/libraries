module Distributed.JobQueue.Client
    ( JobClient
    , newJobClient
    , submitRequest
    , waitForResponse
    , checkForResponse
    , submitRequestAndWaitForResponse
    , cancelRequest
    -- * Convenience utilities for blocking on responses
    , atomicallyFromJust
    , atomicallyReturnOrThrow
    ) where

import Distributed.JobQueue.Client.Internal
