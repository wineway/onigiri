# Publisher performance checkpoint

> Historical measurements before the unified variable-size batch API. Some benchmark
> classes/commands below were retired with the fixed-size path. Current benchmark:
> `mvn -pl transport -Dtest=PublisherDataPathBenchmark test`.
> See [current batch measurements](BATCH_BENCHMARKS.md); old results are not current
> throughput claims and are not directly comparable to the new filtered topology.

## Delivered-throughput follow-up

Three sequential fresh-JVM runs on the same host and topology below now record
each sink's counter before and after the throughput phase, excluding warmup and
latency samples. Both the sum of per-lane delivery counts and the conservation
identity `delivered + dropped = offered * 5` passed in every run. All three runs
still failed the zero-drop assertion.

| Run | Aggregate actual delivery (GB/s) | Lane 0 | Lane 1 | Lane 2 | Lane 3 | Lane 4 | Delivered / offered lane-batches |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 19.358 | 3.509 | 2.864 | 3.976 | 4.201 | 4.808 | 80.528% |
| 2 | 24.892 | 5.175 | 5.175 | 5.175 | 4.190 | 5.175 | 96.192% |
| 3 | 24.481 | 5.070 | 5.070 | 5.070 | 5.070 | 4.202 | 96.574% |

All lane rates are decimal payload GB/s. Aggregate delivery has a median of
24.481 GB/s (122.405 million deliveries/s), counting the five fan-out copies
separately. Rates vary appreciably between runs; these are saturated, lossy
delivery rates, not lossless capacity measurements or real network delivery.

| Run | Single-in-flight p99.9 (microseconds) | Java allocation (B/input-message) | Java allocation (MB/s) |
| --- | ---: | ---: | ---: |
| 1 | 80.979 | 1.095 | 26.330 |
| 2 | 71.993 | 1.287 | 33.306 |
| 3 | 51.680 | 1.321 | 33.485 |

## Original checkpoint

Measured on 2026-09-06, Intel Core i7-11700T (8 cores / 16 logical CPUs),
OpenJDK 64-Bit Server VM 21.0.12. This is a single local run, without CPU
affinity or a controlled host load; it is not a production capacity guarantee.

Reproduce from the repository root, separately from other tests:

```sh
mvn -pl transport test
mvn -pl transport -Dtest=PublisherDataPathBenchmark test
```

The regular suite passed all 80 tests. The explicitly selected benchmark failed
its zero-drop assertion; the performance target is still unmet.

## Topology and measurement

- One producer encodes a reused source containing 256 messages of 200 payload
  bytes each. Wire framing adds two bytes per message.
- Five `DefaultEventLoop` lanes, each with one writable, always-matching sink;
  each input batch is offered to every lane. Each lane supports 4096 subscriber
  slots, has an 8192-batch MPSC queue, and drains up to 256 batches per task.
- Sinks inspect one payload byte, update a shared volatile blackhole and a
  per-sink completion counter, and release the wire buffer. There are no sockets,
  network transfers, subscriber decoder, or application callbacks. Harness
  counter contention is included in the measured cost.
- Two seconds of warmup, three seconds of unrestricted production, then queue
  settlement. Throughput timing includes settlement. Rates use payload bytes
  and decimal GB/s; aggregate delivery counts each fan-out copy separately.
- Latency is measured separately using 20,000 samples with only one batch in
  flight, from before encoding until all sinks acknowledge it. It includes
  cross-thread scheduling and harness observation, but does not characterize
  queueing latency during the saturated throughput phase.
- Java allocations are summed over the producer and all five lane threads using
  `ThreadMXBean`, divided by input messages and elapsed time. Native direct-buffer
  memory and other JVM threads are excluded.

| Metric | Result |
| --- | ---: |
| Offered input throughput | 5.265 GB/s (26.327 million messages/s) |
| Aggregate sink delivery | 25.463 GB/s (127.316 million deliveries/s) |
| Dropped lane-batches during measurement | 51,499 |
| Single-in-flight p99.9 latency | 82.803 microseconds |
| Producer + lane Java allocation | 1.361 bytes/input-message |
| Producer + lane Java allocation rate | 35.838 MB/s |

The input rate remains below the 25 GB/s publisher target and includes batches
that were dropped by some lanes. The aggregate fan-out rate is not unique input
throughput. This benchmark does not establish either subscriber client throughput
or p99.9 latency under sustained load. Further work should measure loaded latency,
profile queue saturation and fan-out contention, and cover selective subscriptions
and the real subscriber decoding path before claiming the performance targets.
