# Subscription notifications and connection sends

Override the existing publisher listener to receive a subscription and send an
initial message to that exact connection:

```java
var listener = new PubServer.Listener() {
    @Override
    public void onSubscription(Connection connection, byte[] prefix) {
        connection.send(
            Unpooled.wrappedBuffer(prefix),
            Unpooled.copiedBuffer("initial state", StandardCharsets.UTF_8));
    }
};
```

`Connection` is `me.wineway.onigiri.publisher.Connection`. It exposes
`remoteAddress()`, `send(ByteBuf message)` and `send(ByteBuf topic, ByteBuf payload)`.
The single-frame overload requires a nonempty message; the two-frame overload
allows empty topic and payload frames.

## Notification contract

- Each accepted wire SUBSCRIBE, including repeated and empty prefixes, produces a
  notification subject to the bounded capacity below. CANCEL does not notify.
- Subscription routing is already updated when the callback is scheduled. Normal
  publications can arrive before the callback or its initialization message.
- Callbacks run on the existing boss control EventLoop with other listener events.
  Keep callbacks nonblocking. Exceptions are logged and isolated.
- The binary prefix is an independent copy, so retaining or changing it cannot
  alter routing. The connection may have disconnected before the callback runs.
- `PubServerConfig.subscriptionCallbackCapacity` bounds outstanding notifications
  across the publisher, including the running callback. Default: 65,536; must be
  positive. The full constructor exposes it; existing constructors retain defaults.
  When full, new notifications are dropped without undoing subscriptions. Publisher
  shutdown or control-loop rejection can also discard notifications.

## Send contract

`Connection.send` is thread-safe and can be called after returning from the
callback. A handle belongs to one session for its entire lifetime. A disconnected
handle never targets a replacement connection, even if an address or lane slot is
reused. Sending to a closed connection discards the message.

Each call copies the current readable bytes and consumes one owned reference to
each input, including invalid input, queue rejection and closed connections. Input
indices are unchanged. Null buffers, an empty single frame, or messages exceeding
frame/message/encoded-batch limits throw synchronously after releasing inputs.
Do not release the handed-off references again. As with other buffer-consuming
APIs, passing the same buffer twice requires two owned references.

Complete messages enter the destination lane's existing bounded MPSC queue and
are written only to the captured session. Publisher-side topic matching is
bypassed. Native SUB clients may still filter locally, so use a matching topic
when targeting those clients. Queue overflow, lane shutdown or an unwritable
connection drops the entire message and releases its buffers. Targeted sends
share queue capacity with broadcasts.

There is no initialization acknowledgement, retry, failure callback or ordering
barrier against normal publications. Existing transport error notifications still
apply. Lane queue rejection/failure counters include targeted entries; those
counters do not count every discarded notification or closed/unwritable send.

`Connection.send` does not change the owner-thread contract of `MultipartWriter`
or `PublishBatchWriter`. No monitor or spin lock has been added to those writers.

## Validation

The full functional suite passes 141 tests with native ZeroMQ interoperability
enabled and paranoid Netty leak detection. Coverage includes same-topic connection
isolation, stale handles after reconnect, concurrent complete multipart sends,
duplicate subscriptions, prefix-copy ownership, notification overload/recovery,
and buffer release on invalid input, queue overflow, unwritable targets and close.
Native libzmq peers also verify connection-specific initialization.

See [broadcast regression measurements](BATCH_BENCHMARKS.md#connection-send-regression-run)
for the 200-byte workload after adding targeted routing. This measures the shared
broadcast hot path; it does not measure subscription callback or directed-send
capacity, nor establish a latency guarantee for initialization over TCP.
