# Onigiri

Onigiri is a Java 17+ publish/subscribe library built on Netty. It implements the
ZMTP 3.1 NULL/PUB/SUB subset over TCP and can communicate with native ZeroMQ peers.

The project is under active development (`0.1.0`). APIs may change, and the
performance targets below are engineering goals, not guaranteed capacity.

## Features

- **Publisher:** binary prefix subscriptions, single-frame and multipart messages,
  variable-size batches, and independent publishing lanes with bounded queues.
- **Subscriber:** multiple publisher connections, reconnect with subscription
  restoration, heartbeats, and borrowed-buffer receive callbacks.
- **Sharding:** `ShardedSubClient` distributes publisher connections across
  independent EventLoops.
- **Connection-specific delivery:** subscription notifications and
  `Connection.send(...)` for initializing an individual subscriber.
- **Native interoperability:** tests against libzmq / pyzmq in both directions.
  Recorded runs used libzmq 4.3.5 and pyzmq 27.1.0.

Delivery is best effort: messages can be dropped before subscriptions arrive, when
queues fill, or when subscribers are unwritable. There is no persistence, replay,
or delivery acknowledgement. Older ZMTP negotiation and PLAIN/CURVE authentication
are not implemented.

## Build

Requires **JDK 17 or later** and **Maven**. Run commands from the repository root:

```sh
mvn -pl transport test
mvn install
```

`mvn install` installs the parent POM and transport module into your local Maven
repository. A Java application can then depend on:

```xml
<dependency>
  <groupId>me.wineway</groupId>
  <artifactId>onigiri-all</artifactId>
  <version>0.1.0</version>
</dependency>
```

## Quick start: Java publisher → ZeroMQ subscriber

The included [PublisherExample](transport/src/main/java/me/wineway/onigiri/examples/PublisherExample.java)
publishes each non-empty terminal line as a UTF-8 message on loopback TCP.
The following commands use a POSIX shell:

```sh
mvn -pl transport compile dependency:build-classpath -Dmdep.outputFile=target/classpath.txt
java -cp "transport/target/classes:$(cat transport/target/classpath.txt)" \
  me.wineway.onigiri.examples.PublisherExample 5555
```

In another terminal, install the Python peer in a virtual environment:

```sh
python3 -m venv .venv
.venv/bin/python -m pip install pyzmq
```

Run the following with `.venv/bin/python`:

```python
import zmq

with zmq.Context() as context:
    with context.socket(zmq.SUB) as subscriber:
        subscriber.setsockopt(zmq.SUBSCRIBE, b"")  # Subscribe to every prefix.
        subscriber.connect("tcp://127.0.0.1:5555")
        while True:
            print(subscriber.recv())
```

Type messages in the Java terminal after the subscriber connects. The handshake
does not acknowledge subscriptions, so early messages may be dropped. Send EOF
(Ctrl-D on Unix) to stop the publisher and shut down its EventLoop group.

## API and buffer ownership

Use `PubServer.publish(ByteBuf)` for a single non-empty frame,
`publishMultipart(topic, payload)` for two frames, or `newBatchWriter()` to batch
single-frame and multipart messages with different lengths.

Publishing and writer add/send operations consume one owned reference to each
input buffer, including rejection and failure paths. Do not release that reference
again. Writers copy the readable bytes without changing input indices and belong
to one producer thread. Stop producers and close their writers before awaiting
`PubServer.closeAsync()`, then shut down caller-owned EventLoop groups.

`SubClient.MessageListener` receives borrowed buffers valid until the callback
returns. Do not release or mutate them; use `retainedSlice(index, length)` and
release that slice later if you need to keep data. Callbacks must not block.
One `SubClient` serializes callbacks on its EventLoop; callbacks from
`ShardedSubClient` can run concurrently across shards.

Detailed contracts and examples:

| Guide | Contents |
| --- | --- |
| [Publisher](PUBLISHER.md) | Ordering, delivery, limits, counters and shutdown |
| [Multipart and batching](MULTIPART.md) | Sending, receiving and buffer ownership |
| [Subscriptions and directed sends](SUBSCRIPTION.md) | Callbacks and connection-specific initialization |
| [Subscriber APIs](SUBCLIENT_OPTIMIZATION.md) | Borrowed buffers, sharding and concurrency |
| [Development notes](TODO.md) | Remaining performance and concurrency work |

## Verification

The default test suite runs Java functional tests. Native interoperability tests
are opt-in and require Python with pyzmq backed by libzmq:

```sh
mvn -pl transport -Donigiri.zmq=true \
  -Donigiri.python="$PWD/.venv/bin/python" \
  -Dio.netty.leakDetection.level=paranoid test
```

When enabled, missing native dependencies fail the tests. Coverage includes binary
and overlapping subscriptions, reconnect and restoration, multipart boundaries,
variable-size batches, connection-specific sends, malformed input, lifecycle races,
and buffer release. Throughput benchmarks are selected separately.

## Performance

Targets use **200-byte payloads** and decimal GB/s:

| Metric | Target |
| --- | --- |
| Publisher input throughput | 25 GB/s (125 million messages/s) |
| Subscriber delivered throughput | 5 GB/s (25 million messages/s) |
| Processing p99.9, excluding network transfer | < 1 ms |

The recorded publisher regression run does **not** meet the input-throughput or
loaded-latency targets. Local subscriber processing has exceeded 5 GB/s with
256-message input chunks, but the recorded multi-publisher network runs remain
below 5 GB/s. Local processing and TCP delivery results are different measurements.

Run the publisher benchmark without native dependencies:

```sh
mvn -pl transport -Dtest=PublisherDataPathBenchmark test
```

Run the subscriber benchmark with the Python environment from above:

```sh
mvn -pl transport -Dtest=SubClientBenchmark \
  -Donigiri.python="$PWD/.venv/bin/python" test
```

Reports include topology, payload distribution, throughput, sampled p99.9 latency,
Java allocation rate and measurement limitations. Batch/chunk latency percentiles
must not be interpreted as per-message percentiles.

- [Publisher variable-size and 200-byte benchmark](BATCH_BENCHMARKS.md)
- [Subscriber baseline](SUBCLIENT_BENCHMARKS.md)
- [Borrowed-buffer and multi-publisher comparison](SUBCLIENT_OPTIMIZATION.md)
- Historical results: [publisher](BENCHMARKS.md), [multipart](MULTIPART_BENCHMARKS.md)
