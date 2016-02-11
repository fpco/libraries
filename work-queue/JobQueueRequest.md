This document describes the current implementation of the compute tier, in terms
of the lifecycle of a request. See also the documentation in the HMST repo,
"doc/google-docs-import/hpc-library.adoc", which describes the goals and
architecture of the HPC library.

There are two main layers to this:

- **Job-Queue** handles communication between the client tier and compute tier.
  It provides guarantees that requests won't be lost until there is a response,
  and distributes work to the workers.

- **Work-Queue** handles communication between the master compute nodes and
  slaves. This allows the the larger requests to be broken up into smaller work
  items which are distributed among workers.

# Beginning of a request: the job-queue client

The API used for clients of the job-queue is provided by
`Distributed.JobQueue.Client`. While it's making requests / waiting for
responses, the client of the job queue must run `jobQueueClient` (or
use `withJobQueueClient`), which does the following:

* Periodically checks the heartbeats of workers (see "Checking job-queue
  heartbeats" below).

* Subscribes to a channel which notifies when responses are ready.  This allows
  the client to use `jobQueueRequest :: ... -> request -> m response` to make a
  request and block on receiving the response.  A lower level API,
  `registerResponseCallback`, allows the client to specify what happens upon
  receiving a response for a particular request.

There is also an API which allows for requests to be sent and later polled for
response:

```
sendRequest :: _ => ClientConfig -> RedisInfo  -> request -> m (RequestId, Maybe response)
checkForResponse :: _ => RedisInfo -> RequestId -> m (Maybe (Either DistributedJobQueueException response))
```

## Initial request

When a request is initially made, the following occurs:

(0) The request's Serialize representation is hashed to get its `RequestID`.

(1) It checks if there is already a cached response, and returns it if it finds
one (computations must be deterministic to enable this caching!)

  - If an exception was stored in the cached response, it's assumed that we're
    attempting to retry the computation.  The cache gets cleared, and we
    continue on to (2).

(2) The request gets sent to Redis:

  - The key `request:<RequestId>` gets set with the request data.

  - The list at `requests` gets `RequestId` pushed to it.

  - The channel at `requests-channel` gets an empty message sent to it,
    notifying any idle workers that there is work to do.

# Processing of a request: job-queue worker

The API used for job-queue workers is provided by `Distributed.JobQueue.Worker`.
The function invoked by workers is

```
jobQueueWorker
  :: _
  => WorkerConfig
  -> (payload -> IO result)
    -- ^ Run by slaves
  -> (RedisInfo -> MasterConnectInfo -> request -> WorkQueue payload result ->
  IO response)
    -- ^ Run by master
  -> m ()
```

This starts a worker which will become a master or slave depending on the
requests it receives.

## Master receives initial request

Continuing on following a request through the job-queue, all of the idle workers
will be subscribed to `requests-channel`.  As soon as a message is received on
this channel, the following happens:

(0) The worker checks for a slave request, by popping `slave-requests`, and
attempts to become a slave if it receives one (see "Becoming a slave", below).
Otherwise, we carry on with the following:

(1) The worker checks for a job request, by popping the `requests` list.

  - This is done with the `rpoplpush` command, which atomically moves the item
  from the `requests` list to `active:<WorkerId>`. This is a list of items that
  the worker is currently working on. With the current implementation, the list
  should never have more than one item in it.

  - If there is no work to be done, then the worker goes into idle mode and
    waits for a request.

(2) Carrying on, assuming we got a job request: The worker fetches and decodes
the request stored at `request:<RequestId>`.

## Master asks for slaves and dispatches work to them

The master uses `Distributed.WorkQueue` to communicate with its slaves.  Here's
how this works:

(0) The master function gets invoked with the decoded request and a `WorkQueue`.

(1) The master uses the `requestSlave` function to enqueue an entry on the
`slave-requests` list. The `MasterConnectInfo` data enqueued provides the host
and port to connect to. It also publishes to the `requests-channel`, to wake up
any idle workers.

(2) The master then uses functions like `Data.WorkQueue.queueItem` to enqueue
work items.  When slaves connect, these work items are sent to them, and the
results are collected.

Note: if `workerMasterLocalSlaves` is greater than 0, then the master itself
will also process work items.

## Master collects results and sends response

(0) The master function collects all of the responses from the `WorkQueue`, and
returns them.

(1) The response then gets stored in redis. If an exception occurred while
running the master, then it is sent.  Here's how this works:

  - `response:<RequestId>` gets set with the encoded response / exception.

  - `response-channel` gets notified of the response, in the form of an encoded
    `RequestId`.

  - The `RequestId` entry in `active:<WorkerId>` gets removed, since the work is
  done.

  - The `request:<RequestId>` data gets deleted, since it's no longer needed.

# Client receives response

If the client is being used with the callbacks API, then the notification on
`response-channel` lets it know that the response is ready.

Otherwise, the client is periodically checking `checkForResponse`. Either way,
reading the response simply consists of getting the value from
`response:<RequestId>`, and decoding it.

This completes our tracing of the standard lifetime of a request!

# Recovery from failure

The above tracing of the lifetime of a request doesn't deal with the failure
cases of what happens when servers randomly go down.  In this section, we'll dig
into these details:

## Checking job-queue heartbeats

The clients of the job-queue periodically check that the workers are sending
heartbeats to redis.  Here's how this works:

* There is a sorted set of currently active workers, `heartbeat:active`, where
  the 'score' associated with the `WorkerId` is its POSIX timestamp. Server
  clocks must be roughly synchronized.

* There is a `heartbeat:last-check` value, which stores the timestamp of the
  last heartbeat check.

* Loop forever, with a thread delay of the heartbeat check interval

* Read the timestamp of `heartbeat:last-check`, and if enough time has elapsed,
  perform the check (otherwise continue looping):

* Use the `zrange` redis command to fetch the list of active workers who's last
  heartbeat came before `startTime - checkIvl`.  For each of the inactive workers:

  - All of their work items re-enqueued on the `requests` list. The collected
    workers are removed from `heartbeat:active`.

  - Check if the last heartbeat timestamp is too old, and if it is, then use the
    existing heartbeat failure code to migrate its items.

* Put the start time in `heartbeat:last-check`.

As an optimization, `heartbeat:mutex` is used to make it likely that only one
client is executing the heartbeat check at once. However, this is not necessary
for correctness - the heartbeat check can be executed by multiple clients at
once.

## Master <-> slave communication failure (work-queue)

The implementation of `Data.Streaming.NetworkMessage` periodically sends
heartbeat messages on its connection.  This ensures that we quickly know when a
connection has failed, and the exception `NMHeartbeatFailure` gets thrown.

When this happens, the item that the slave was working on gets re-enqueud, for
some other slave to work on.
