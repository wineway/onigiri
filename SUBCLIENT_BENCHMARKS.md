# SubClient performance baseline

This file records the original onFrame baseline. See [the newer optimization
comparison](SUBCLIENT_OPTIMIZATION.md) for borrowed-buffer delivery, multi-publisher
sharding, and variable-size workloads whose mean is 200 bytes.

Run explicitly (requires Python with pyzmq backed by native libzmq):

```sh
mvn -pl transport -Dtest=SubClientBenchmark test
```

Use `-Donigiri.python=/path/to/python` to select another interpreter. Missing
pyzmq fails this benchmark rather than silently skipping it. The benchmark name
excludes it from the normal Surefire functional suite. It does not modify
production code or use an Onigiri publisher as its network peer.

## Measurement topology

One actual `SubClient`, one NIO EventLoop, one TCP connection to a Python process
running a native ZeroMQ PUB socket. Heartbeats are disabled during measurement.
The subscriber installs an empty subscription and waits for a delivered marker
before measurement, so handshake readiness is not mistaken for subscription
readiness. Every application message is a single 200-byte frame (202 wire bytes).
The callback validates frame length/boundary and consumes a payload byte into a
checksum; it does not copy payloads, enqueue application work, or retain frames.

The same client is exercised in three sequential modes, each with two seconds of
warmup and three seconds of throughput measurement:

1. **LOCAL, one message per input chunk.** After the native handshake, inject a
   retained duplicate of a pre-encoded pooled direct buffer into the live client
   pipeline on its own EventLoop, followed by `fireChannelReadComplete`.
2. **LOCAL, 256 messages per input chunk.** The same injection with 51,712 wire
   bytes per chunk. Each message is decoded and dispatched individually; this
   is not a batch callback API.
3. **NETWORK.** Native libzmq PUB sends single messages through loopback TCP.
   Python reuses a 200-byte source buffer, writes a sequence number into it and
   calls `send(copy=True)` once per message. SNDHWM is 8192. The source checks its
   deadline every 64 sends. This includes Python, libzmq, TCP, Netty reads, decoding
   and callback costs, and is not an isolated measurement of subscriber capacity.

Local injection covers `InboundTimeoutHandler`, `Decoder`, `ClientChannel`,
`Session`, `SubClient`, application callback, frame release and read-completion
processing. It excludes sender encoding, network transfer, socket reads, inbound
buffer allocation and EventLoop scheduling delay. Reflection locates the live
channel during setup only; no reflection occurs in the measured loop. The source
buffer is reused and fully consumed each iteration, so this is a favorable hot
buffer case, not fragmented or arbitrary TCP input. Both local modes validate
message count, checksum and final source-buffer reference count.

Network send count is reported separately from receive count. Sequence numbers
check order and duplicates. After production ends, the source sends a marker on
the same PUB connection; observing it proves earlier delivered frames have passed
through the callback. Markers are retried if PUB drops them. Throughput time
includes phase-control overhead and waiting for this final marker. The difference
between offered and received data messages is reported as loss; marker messages
and warmup are excluded. Counts and checksums must agree.

## Latency and allocation definitions

- Local p99.9 samples **whole input-chunk processing time** during the throughput
  phase: before retained-duplicate creation through callback, reference release
  and read completion. Sample every 1024 chunks in single-message mode and every
  16 chunks in 256-message mode. The latter percentile is for all 256 messages
  together and must not be divided by 256 to infer per-message p99.9.
- Network p99.9 samples **Java input-chunk arrival to callback**, every 64th data
  message. It includes processing earlier frames from that chunk, but excludes
  network transfer, socket waiting, scheduling before the read, and waiting for
  earlier fragments. It is not publisher-to-subscriber latency. A timestamp is
  taken for each inbound ByteBuf, which adds instrumentation cost.
- Each mode stores at most the first 131,072 sampled durations in a preallocated
  array. Percentiles use nearest rank. Sampling and timing overhead are included;
  sorting and reporting are outside throughput measurement.
- Allocation uses `ThreadMXBean` for the subscriber EventLoop thread only, reported
  as bytes per delivered message and decimal MB/s. It includes Java receive-path
  and instrumentation allocations, but excludes native/direct memory, the Python
  process, other JVM threads and application payload storage. Unsupported
  allocation counters print NaN.
- Throughput is payload bytes in decimal GB/s, with no fan-out multiplier. Target
  comparisons are informational; failing 5 GB/s does not fail the benchmark.
  Malformed frames, ordering errors, count/checksum mismatches, missing markers,
  leaked injected references and peer errors fail it.

## Recorded result — 2026-09-06

Intel Core i7-11700T, 8 cores / 16 logical CPUs; OpenJDK 64-Bit Server VM 21.0.12;
pyzmq 27.1.0 / libzmq 4.3.5. One fresh-JVM run with default JVM/Netty settings,
no pinned CPUs and no simultaneous benchmark jobs. This is a baseline, not a
statistical capacity guarantee.

| Mode | Delivered GB/s | Million messages/s | Processing p99.9 | Samples | Java B/message | Java MB/s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Local, 1 message/chunk | 1.498 | 7.491 | 0.213 µs | 21,947 | 32.000 | 239.718 |
| Local, 256 messages/chunk | 5.563 | 27.815 | 20.735 µs/chunk | 20,373 | 32.000 | 890.078 |
| Native PUB → loopback → SubClient | 0.267 | 1.333 | 7.627 µs | 62,525 | 88.593 | 118.113 |

Network measurement offered and delivered **4,001,600 messages**, with **zero
observed loss**, over 3.001488 seconds including marker settlement. The source
reported 1.334 million sends/s. The receiver observed 848,638 data-bearing input
chunks, averaging 4.72 messages/chunk.

The 256-message local topology exceeded the 5 GB/s processing target in this run;
that result does not establish 5 GB/s TCP receive capacity. Network throughput
includes the Python source's per-message work and cannot identify the bottleneck
by itself. Both local allocation rate and sensitivity to input chunk size merit
profiling before optimization. Network tail latency and fragmented-frame workloads
remain separate measurements to add if needed.
