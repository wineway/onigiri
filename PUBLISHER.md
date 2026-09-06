# Publisher API contract


- `bind(address)` permits one attempt, including a failed attempt. Repeated attempts
  and calls after close return an exceptional future. Create a new publisher to
  retry binding after a failure. A successful bind means the listening socket is
  active, not that subscribers are ready.
- `publish(ByteBuf)` accepts one non-empty single-frame message.
  `publishMultipart(topic, payload)` accepts a complete two-part message.
  `newBatchWriter()` creates a reusable `PublishBatchWriter`: `add(message)` appends
  a non-empty single frame, `addMultipart(topic, payload)` appends two parts, and
  `sendMore/sendLast` constructs additional multipart entries. Payload lengths may
  differ within one batch. `flush()` submits complete entries; message-count and
  encoded-byte limits also trigger submission. Close discards unflushed entries.
  The fixed-size `PublishBatch(content, messageSize)` API has been removed.
- Passing a buffer/batch to `publish` transfers one owned reference in every
  outcome, including invalid input, no subscribers, closed publisher and queue
  rejection. Do not mutate or release that reference after handing it off.
  Batch add/send calls also consume their references, including failures. Entries
  copy the current readable region immediately, without changing input indices.
  A writer is thread-confined, including flush/abort/close. Signal producers to stop,
  let them close their own writers, then await their exit before closing the server;
  see [batch contracts](MULTIPART.md#sending).
- Publishing may be called from multiple threads. Each lane orders accepted
  batches through its bounded queue; there is no global ordering guarantee across
  concurrent producers and different lanes.
- Delivery is best effort, without replay or acknowledgements. No subscribers
  means immediate discard. A full lane queue drops the new batch for that lane.
  Unwritable subscribers skip current delivery and do not block writable peers.
  Existing socket output can still drain. Batches do not provide atomic delivery
  across subscribers, and subscription changes take effect when processed on
  the subscriber's lane.
- Subscription matching compares binary prefixes. Empty prefixes match all
  messages, and overlapping prefixes never duplicate a delivery. Repeated wire
  SUBSCRIBE commands are reference-counted: each needs a CANCEL. Canceling an
  absent prefix does nothing. Disconnect removes all that session's subscriptions.
- `PubServerConfig` bounds lanes, batches, subscribers, distinct subscriptions per
  session (default `Integer.MAX_VALUE`), and prefix bytes (default 4096). Exceeding
  subscription limits closes the offending connection. The default subscription
  count is effectively unrestricted; configure an explicit limit for your workload.
  The full constructor also exposes multipart and encoded-batch byte limits and
  subscription notification capacity (default 65,536). See
  [multipart limits](MULTIPART.md#limits-and-delivery).
- Listener callbacks are serialized on the boss control EventLoop and must not
  block. Callback exceptions are logged and isolated. `onReady` marks completed
  handshake; subscription commands may still be pending. `onDisconnected` is
  issued once per registered session; errors are reported at most once per session.
- Stop and await producers first; every producer must close its own writers.
  The publisher never closes or releases writer-local buffers. `closeAsync()` is
  idempotent. It closes the listening socket and tracked children,
  stops lanes, discards queued lane batches and waits for lane cleanup. Late
  accepted channels are closed when they register. It is not a delivery or callback
  completion barrier. `close()` starts that process without waiting. Never block
  on its future from an EventLoop. Supplied groups remain caller-owned: normally
  await publisher close **before** shutting down groups. If a caller-owned executor
  rejects cleanup, completion waits for its termination and final queue release.

`dropCounts()` returns weakly consistent diagnostic counters. `noSubscribers`,
`closedPublisher` and `invalidBatches` count input calls/batches. `fullLaneQueues`,
`stoppedLanes` and `failedLaneBatches` count lane-batches, so a fan-out batch can
contribute more than once. `droppedBatches()` is the aggregate of those lane-batch
failure categories. These counters exclude ordinary nonmatches and unwritable
subscriber skips, and cannot establish delivery rate or total lost messages.

Protocol behavior follows [ZMTP 3.1](https://rfc.zeromq.org/spec/37/) for the
implemented NULL/PUB/SUB subset; native tests do not imply support for every
ZeroMQ socket option or protocol version.
