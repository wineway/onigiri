# Multipart PUB/SUB

A logical message consists of a topic frame followed by one or more payload frames.
The publisher matches binary subscription prefixes against the topic only. It queues,
filters and drops complete messages, never individual parts. ZMTP MORE is set on all
frames except the last. Empty topics and payload parts are supported. Ordinary
single-frame publication remains available. A unified `PublishBatchWriter` accepts
variable-size single-frame and multipart entries in the same batch.

## Sending

When both buffers are available:

```java
publisher.publishMultipart(topic, payload);
```

When the topic is available before the payload, reuse a writer owned by one producer:

```java
try (MultipartWriter writer = publisher.newMultipartWriter()) {
    writer.sendMore(topic);
    // Prepare payload here. No partial message has entered the network queue.
    writer.sendLast(payload);
    // The writer is now ready for another message.
}
```

Call `sendMore` repeatedly for additional intermediate parts. `sendLast` requires
at least one earlier `sendMore`. A writer is not thread-safe: create, append, flush,
abort and close it on its producer thread. Separate producers need separate writers.
There are no writer locks, atomic ownership claims or per-message futures, and no
runtime thread-ownership check on each part. Concurrent access, including another
thread calling `writer.close()`, is unsupported. There is no global ordering across
concurrent producers or different lanes.

Every send transfers one reference, including rejection or validation failure.
Readable bytes are copied into a contiguous pooled direct wire buffer and the input
reference is released before return, without changing its reader/writer indices.
Do not use a transferred reference. To reuse immutable input storage, pass a fresh
`retain()` reference for each call. The two-argument convenience method consumes
both references even if the first part is invalid. If both arguments share storage,
each argument must carry its own reference.

`abort()` discards the unfinished message. A failed send also aborts the unfinished
message; previously completed entries in a batch remain available for `flush()`.
`close()` discards all locally buffered entries; it does not flush. The producer
must close its own writer, normally using try-with-resources. Calls on a closed
writer reject and consume the supplied part.

Shutdown order is explicit:

1. The controller signals producers to stop using application-level coordination.
2. Each producer finishes its current operation and closes its own writer in a
   `finally` block. Flush complete entries first if submission is desired; completing
   flush still does not acknowledge delivery. Unfinished entries must be aborted.
3. The controller waits for producer termination (for example, `Thread.join()` or
   task completion), then calls `publisher.closeAsync()` and awaits it before
   shutting down the EventLoop groups. Never join/block from an owned EventLoop.

The publisher has no writer registry and never releases writer-local buffers.
Its close future waits for lane/connection cleanup, not producer or writer cleanup.
If publisher shutdown happens early, further sends observe the publisher's closing
flag and reject, and already committed batches may be dropped. Buffered entries
remain producer-owned until that producer calls `writer.close()`. The publisher
cannot free memory still being written by a producer. This replaces the earlier
cross-thread writer-close guarantee; a controller must not call `writer.close()`
to interrupt an active producer.

For throughput, reuse a batch writer:

```java
try (PublishBatchWriter writer = publisher.newBatchWriter()) {
    writer.add(singleMessage); // one non-empty frame; its prefix is used for routing
    writer.addMultipart(topic, payload); // payload length is independent of other entries
    writer.sendMore(anotherTopic);
    writer.sendMore(firstPayloadPart);
    writer.sendLast(lastPayloadPart);
    writer.flush();
}
```

`add`, `addMultipart`, and `sendLast` complete entries. The writer submits
at `maxBatchMessages`, or before a new message when there is insufficient remaining
space. A complete single frame reserves its actual encoded size; staged multipart
reserves the configured maximum message size because later parts are not yet known.
`add`/`addMultipart` during unfinished multipart construction reject and consume the
new inputs and abort that unfinished message; previously completed entries survive.
`flush()` submits a partial batch of complete messages; calling it with an unfinished message throws without discarding that
message. There is no automatic time-based flush, so callers must flush at their
latency boundary. Delays while preparing payload or filling a batch are application
latency and are not hidden by the API.

A batch uses one wire buffer and three immutable arrays of logical-message/topic
offsets and lengths. Lanes share the buffer for common routes; filtered routes copy
whole message intervals into one destination buffer. No per-part futures, lists,
slices or cross-thread queue entries are created. All entries use the same
offset/length representation; there is no fixed-size stride path. This implementation
copies payloads once into the wire batch; it is not a zero-copy publisher.

## Receiving

Use `SubClient.MultipartListener` to get one callback per complete logical message:

```java
SubClient.MultipartListener listener = new SubClient.MultipartListener() {
    @Override
    public void onMultipart(SocketAddress publisher, MultipartView message) {
        // Single-frame peers produce partCount() == 1; do not assume two parts.
        ByteBuf topic = message.buffer(0);
        int topicIndex = message.index(0);
        int topicLength = message.length(0);
        // Read buffers with absolute indices. Do not mutate or release them.
        for (int part = 1; part < message.partCount(); part++) {
            consume(message.buffer(part), message.index(part), message.length(part));
        }
    }
};
```

The view and its buffers are borrowed only until callback return. The view is reused;
never save it. For asynchronous processing, obtain an owned
`message.buffer(i).retainedSlice(message.index(i), message.length(i))` and later
release that slice. Synchronous delivery creates no per-message view or part wrappers.
Incomplete parts retain input buffers; completion, error, timeout, disconnect,
reconnect and close release those references. Closing from inside a callback does
not invalidate its view before that callback returns.

Each publisher session assembles independently. `SubClient` callbacks run serially
on its EventLoop. `ShardedSubClient` accepts the same listener and can invoke it
concurrently across shards. Callbacks must not block. `MultipartListener` takes
precedence if a listener also implements `MessageListener`.

Existing `onFrame` and `MessageListener.onMessage` remain streaming part callbacks:
`more=true` means another part is required, not that it will necessarily arrive.
These modes also validate message limits and truncation, but do not buffer complete
messages. Their consumers must handle partial delivery on disconnect. An incomplete
logical message at EOF reports a protocol error even if each received frame was
complete. Complete-message mode never delivers that partial message. Listener
exceptions on the borrowed decoder path can arrive wrapped in `DecoderException`.

## Limits and delivery

`PubServerConfig` adds defaults of 64 parts/message, 16 MiB total frame-body bytes
per logical message and 32 MiB encoded bytes/batch. A frame is also bounded by the
existing 16 MiB decoder frame limit. The batch limit must accommodate the message
body limit plus nine header bytes per allowed part. The new message/batch byte
limits also apply to ordinary single-frame publishing and batch entries. Existing 7-, 8- and 10-argument
constructors keep default values for the additional limits.

`SubClientConfig` adds 64 parts/message, 16 MiB total body bytes and a 5000 ms
multipart assembly timeout. Its existing four-argument constructor remains valid.
Zero disables that timeout. One periodic task per active session checks the age of
an incomplete message every half-timeout (minimum 1 ms); the deadline starts at the
first complete MORE frame and later parts do not extend it. Detection is periodic,
not an exact real-time deadline. A partially received first frame is handled by the
existing frame limit and transport/heartbeat timeouts. Message-limit violations and
assembly timeout close the connection and normal reconnect policy applies.

Memory is bounded by configured parts, message bytes, batch bytes and lane queue
capacity. Retained input views may pin larger backing input buffers than the sum of
part lengths. Limits on total body bytes therefore do not specify exact physical
memory usage. Writers can keep an unfinished message until abort/close; there is no
publisher-side assembly timer.

A lane rejects an entire encoded batch on queue overflow. Unwritable subscribers
skip complete message delivery; accepted socket output may still drain. Subscription
changes take effect when the lane routes a completed batch, not when `sendMore`
was called. A network failure can truncate bytes already in flight: the receiving
complete-message listener drops that incomplete message. No delivery acknowledgement,
replay or cross-subscriber atomicity is promised.

Existing drop counters count submitted input/lane batches, not parts. Writer-local
aborts, validation failures and close-discarded local entries are not counted there.

The old `PublishBatch(content, messageSize)`, `publish(PublishBatch)` and
`Encoder.messages(bodies, messageSize)` interfaces have been removed. Migrate
callers to `newBatchWriter()` and append each logical message with its own length.
The earlier `newMultipartBatchWriter()` is replaced by `newBatchWriter()` as well.
This is a source-breaking API change; ordinary publish and receive APIs remain.

See [variable-size batch measurements](BATCH_BENCHMARKS.md) for benchmark commands,
throughput, p99.9, allocation, topology and target gaps.
