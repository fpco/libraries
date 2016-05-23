# How `JobQueue` works

Each request has a `RequestId` and a request body, which is just a `ByteString`. `RequestId` is just a `ByteString` as well, and so it can be anything, but the system assumes that if we ever produced a response for a request with `RequestId` `foo`, all subsequent clients that request `foo` will get the same answer (as long as it's cached).

A client can insert a request in the system by placing its `RequestId` on the redis list `requests` and its request body on redis string `request:requestId`.

Each worker has a `WorkerId`, which too is a `ByteString` and uniquely identifies the worker. Workers wait for new values on `requests`, and when one comes they atomically pop it from `requests` and put it on `active:workerId`. `active:workerId` is a redis list, but it always has either 0 values, if the worker is not processing anything, or 1 value, if the worker is currently processing a request.

Additionally, workers periodically place a heartbeat on the sorted set `heartbeat:active`, which has the time of the heartbeat as key and the worker id as value.

Clients periodically check `heartbeat:active` and if there are workers that haven't placed a heartbeat within a specified amount of time, the worker is considered dead, and if it was processing a request (if there was a request on `active:workerId`) the request is re-enqueued. This means that if there are no clients no dead worker detection happens.

When a worker completes a request, it does:

* Place the response body on `response:requestId`;
* Remove the request from `active:workerId`;
* Remove the request id from `requests`;
* Remove the request body from `request:requestId`.
