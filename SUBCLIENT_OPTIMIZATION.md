# SubClient: borrowed delivery and multi-publisher shards

200 bytes is the **mean payload size**, not a fixed production message size.
Publisher batch API changes are recorded in `TODO.md` and deferred. The new receive
path accepts variable lengths, empty frames and multipart flags; its optimization
does not depend on a 200-byte message size.

## Receive API

Existing `SubClient.Listener.onFrame` behavior and ownership remain unchanged.
For low-allocation synchronous processing, implement `SubClient.MessageListener`:

```java
SubClient.MessageListener listener = new SubClient.MessageListener() {
  @Override
  public void onMessage(SocketAddress publisher, ByteBuf data,
      int index, int length, boolean more) {
    // Process exactly data[index .. index + length) before returning.
    // Do not mutate data or its indices, and do not release this borrowed buffer.
  }
};
```

Data invokes `onMessage` instead of `onFrame`. The decoder processes adjacent data
frames in a loop and invokes the session callback directly, eliminating the
per-message `Decoder.Frame`, retained slice and generic pipeline output-list
handoff. Greeting and command delivery still follows the existing pipeline,
including ordering before later data. This adds no payload copy. Fragmented input
may still require Netty's normal cumulation work.

To keep a message beyond callback return, acquire `data.retainedSlice(index,
length)` and release that owned slice later. This reintroduces an allocation and
may keep an entire input buffer alive. The synchronous callback is the intended
fast path. A normal SubClient still serializes callbacks from all its publishers
on one EventLoop.

## Explicit multi-publisher parallelism

Use `ShardedSubClient` when publisher connections should be distributed across
EventLoops:

```java
var group = new MultiThreadIoEventLoopGroup(4, NioIoHandler.newFactory());
var client = new ShardedSubClient(group, 4, new ClientChannelConfig(),
    new SubClientConfig(), listener);
client.subscribe(new byte[0]);
client.connect(new InetSocketAddress("publisher-a", 5555));
client.connect(new InetSocketAddress("publisher-b", 5555));
// Connect more publishers as needed; connect() reports initial TCP attempt only.
// Shutdown from a non-EventLoop thread:
client.closeAsync().get();
group.shutdownGracefully().sync();
```

**Callbacks from different shards can run concurrently.** Listener state must be
thread-safe. No per-message cross-thread queue or future is introduced; data is
processed on its socket's EventLoop. The shard count must not exceed the supplied
group's EventLoop count. A single publisher connection stays on one shard, so
adding shards does not parallelize a single TCP stream. Assignment is round-robin,
not adaptive load balancing.

Concurrency design:

- A control EventLoop owns publisher-to-shard assignment and orders all public
  subscription/connect/disconnect operations.
- Each shard is a complete independent SubClient with its own session registry,
  subscription intent and existing READY/reconnect restoration logic. Mutable
  session state is not redistributed between EventLoops.
- A single producer (the control loop) broadcasts subscription operations in FIFO
  order to every shard. Connect operations are ordered through the same producer.
  A shard either applies an update while active or restores its own current intent
  at READY; there is no shared mutable subscription collection on worker loops.
- A reconnect queued while disconnect is still completing reuses the old shard,
  preventing old/new callbacks for that address from overlapping on different
  loops. After disconnect completes, later connections may receive a new shard.
- Close prevents further control work, closes all child clients and waits for
  registry cleanup. Supplied EventLoop groups remain caller-owned. If a shard
  rejects a subscription broadcast, the composite closes rather than remaining
  partially updated. Such an unexpected rejection reports `onError(null, error)`
  on the control loop; expected owner-group shutdown is handled by cleanup hooks.

The versioned shared-session snapshot design in `TODO.md` remains relevant if the
original SubClient's individual sessions are ever moved between loops. This
composition preserves each child's existing single-loop invariant instead.

## Reproduce the native multi-publisher benchmark

Requires a C++17 compiler and libzmq development headers/library (Debian/Ubuntu:
`libzmq3-dev`). Native source is `transport/src/test/native/subscriber_load.cpp`.
There is no Python per-message sender loop in this benchmark.

```sh
mvn -pl transport test-compile
c++ -O3 -std=c++17 transport/src/test/native/subscriber_load.cpp -lzmq \
  -o transport/target/subscriber_load

# Compatibility onFrame path, one EventLoop:
mvn -pl transport -Dtest=MultiPublisherBenchmark \
  -Donigiri.publishers=8 -Donigiri.rate=2500000 test

# Borrowed callbacks, one EventLoop:
mvn -pl transport -Dtest=MultiPublisherBenchmark \
  -Donigiri.publishers=8 -Donigiri.rate=2500000 -Donigiri.borrowed=true test

# Borrowed callbacks, four independent receive shards:
mvn -pl transport -Dtest=MultiPublisherBenchmark \
  -Donigiri.publishers=8 -Donigiri.rate=2500000 \
  -Donigiri.borrowed=true -Donigiri.shards=4 test
```

`onigiri.rate` is offered messages/second **per publisher**; zero means unpaced.
`onigiri.nativePublisher` can override the executable path. `onigiri.shards`
defaults to one. Use `-Donigiri.rate=1000000` to reproduce the lower offered-rate
measurement below. The separate local-pipeline benchmark now supports
`mvn -pl transport -Dtest=SubClientBenchmark -Donigiri.borrowed=true test` and still
requires pyzmq for handshake/network phases.

Topology and instrumentation:

- Eight independent native libzmq PUB processes on the same host, each with its
  own libzmq I/O thread, connected over loopback TCP to one or four receive loops.
  With four shards, two publisher connections are assigned to each shard.
- Empty subscription, heartbeat disabled, native PUB SNDHWM 8192 messages. The
  callback checks size, sequence order, phase and a payload byte, with bounded
  latency storage and no payload copy.
- Both fixed 200-byte control messages and a repeating **64/128/200/408-byte**
  distribution (mean 200 bytes) are measured. Byte throughput counts actual
  received payload lengths, including when drops alter the received distribution.
- Each distribution has two seconds warmup and three seconds production.
  Per-publisher markers settle earlier delivered data before counters are read;
  elapsed time includes phase-control/marker settlement. Publisher send counts
  and received counts are reported separately, including per-publisher delivery.
- Processing p99.9 measures arrival of the final input chunk to the data callback,
  sampling every 256th delivered message per publisher (up to 65,536 samples per
  publisher). It includes processing earlier messages in the chunk and any
  preemption within that interval, but excludes network transfer, socket waiting
  before the read, and waiting for earlier fragments. It is not end-to-end latency.
- Java allocation is summed across the receive EventLoops using ThreadMXBean;
  direct/native memory, native publisher allocation and other JVM threads are
  excluded. CPU percentage sums EventLoop thread CPU time: 100% means one core.
- The harness caches publisher-counter lookup per thread and separates writable
  per-publisher counters by 128 bytes. Preliminary runs that incurred per-message
  address lookup or cross-shard false sharing are not used in the comparison below.
- Counts, ordering, payload checks and markers are correctness assertions.
  Throughput targets and zero loss are reported, not asserted: a successful test
  exit is not a performance-target certification.

## Recorded comparison — 2026-09-06

Intel Core i7-11700T, 8 cores / 16 logical CPUs; OpenJDK 21.0.12, libzmq 4.3.5.
Separate fresh-JVM runs, sequential benchmark jobs, default JVM/Netty settings,
no CPU affinity. These are single-run observations with host scheduling noise.

The same eight publishers each offer 2.5 million messages/s, approximately
**4 GB/s combined payload**. The following rows use the variable-size workload:

| Receive mode | Delivered GB/s | Processing p99.9 | Java B/message | Java MB/s | Message loss |
| --- | ---: | ---: | ---: | ---: | ---: |
| Existing onFrame, 1 loop | 1.425 | 907.917 µs | 72.679 | 517.811 | 64.107% |
| Borrowed onMessage, 1 loop | 1.736 | 977.607 µs | 0.250 | 2.173 | 56.347% |
| Borrowed onMessage, 4 shards | 2.479 | 634.605 µs | 1.114 | 13.810 | 37.860% |

Four shards delivered 37,272,953 messages / 7,454,589,008 payload bytes in
3.006761 seconds. Per-publisher payload rates were 0.310, 0.308, 0.308, 0.306,
0.313, 0.309, 0.312 and 0.313 GB/s. Aggregate receive-loop CPU was 237.8%.
Throughput improved about 74% over the existing callback/one-loop configuration;
allocation rate fell about 97%. The one-loop borrowed path reduced allocation
substantially but did not improve this run's tail latency at saturation.

At a lower offered rate of one million messages/s per publisher, the four-shard
variable-size run received **all 24,000,256 messages**, delivering **1.597 GB/s**
with p99.9 **38.226 µs**, **1.594 Java B/message** and **12.727 MB/s** allocation.
This is one observed no-loss point, not a proven maximum sustainable capacity.
The fixed-200-byte phase of that same lower-rate run lost 0.602% of messages, so
it does not establish a general zero-loss guarantee.

For local processing only (one loop, pre-encoded pooled input, no socket read or
network cost), the borrowed 256-message/chunk run measured **18.783 GB/s**, p99.9
**4.500 µs/chunk**, and Java allocation below the displayed 0.001 B/message
precision. The earlier same-topology onFrame baseline was 5.563 GB/s,
20.735 µs/chunk and 32 B/message. See `SUBCLIENT_BENCHMARKS.md` for local measurement
scope. This synthetic local result is not a TCP delivery capacity claim.

**The real multi-publisher 5 GB/s delivery target remains unmet.** Saturated runs
still drop messages, and this host also executes all native publishers. Next
capacity work should isolate publishers onto another host, measure kernel/socket
and native-sender CPU costs, and establish repeated zero-loss load points before
claiming capacity. The four-size distribution is a reproducible approximation;
production size distribution and publisher count should replace it when available.
