-- | The job-queue uses "Distributed.RedisQueue" atop
-- "Distributed.WorkQueue" to implement robust distribution of work
-- among many slaves.
--
-- Because it has heartbeats, it adds the guarantee that enqueued work
-- will not be lost until it's completed, even in the presence of
-- server failure.  A failure in Redis persistence can invalidate
-- this.
--
-- SIDENOTE: We may need a high reliability redis configuration
-- for this guarantee as well. The redis wikipedia article
-- mentions that the default config can lose changes received
-- during the 2 seconds before failure.
module Distributed.JobQueue
    ( -- * Client API
      module Distributed.JobQueue.Client
      -- * Worker API
    , module Distributed.JobQueue.Worker
    ) where

import Distributed.JobQueue.Client
import Distributed.JobQueue.Worker