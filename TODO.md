# TODO

## Publisher performance

Variable-size single-frame and multipart batches are implemented through
`PublishBatchWriter`; see [the API contract](MULTIPART.md). Continue profiling
filtered routing, payload copies and saturated queue behavior with both fixed
200-byte messages and variable sizes averaging 200 bytes.

The latest recorded broadcast regression run does not meet the 25 GB/s publisher
input target or loaded processing p99.9 < 1 ms. See [measurements](BATCH_BENCHMARKS.md).
Add subscription notification drop metrics and benchmarks for connection-directed
sends and subscription callback capacity.

## Implemented alternative: independent SubClient shards

ShardedSubClient composes independent single-loop clients behind a serialized
control loop. Subscription operations are broadcast in FIFO order to every child;
each child owns its registry and READY/reconnect restoration. Callbacks may run
concurrently across shards. See SUBCLIENT_OPTIMIZATION.md for the design and tests.
This does not redistribute the original SubClient's shared session state.

## Allow publisher sessions within one SubClient to use different EventLoops

Keep all sessions of one `SubClient` on its single EventLoop for now. Revisit this
only if profiling shows that the EventLoop is a bottleneck.

Moving sessions onto different EventLoops requires an explicit synchronization
design; changing `group.next()` alone is unsafe. Before enabling it:

- Keep the session registry and subscription changes on a control EventLoop.
- Publish subscriptions as immutable, versioned snapshots.
- Let each session reconcile the latest snapshot on its own EventLoop before it
  becomes active after READY.
- Send later subscription changes to each session with their snapshot version and
  ignore stale updates.
- Track each session's applied subscription set to prevent duplicate SUBSCRIBE and
  CANCEL commands around the READY race.
- Decide whether public listener callbacks remain serialized on the control
  EventLoop or may run concurrently on session EventLoops. If frames cross loops,
  retain and release their `ByteBuf` ownership explicitly.
- Keep connect, disconnect, close, ready-count and terminal ERROR removal ordered
  through the control EventLoop.

Required race coverage:

- subscribe/unsubscribe immediately before, during and after READY restoration;
- concurrent subscription changes from multiple caller threads;
- reconnect while subscription updates are pending;
- one session closing while an update is broadcast to other sessions;
- client close racing with session READY and session termination.
