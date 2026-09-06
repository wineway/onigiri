# Unified variable-size batch measurements — 2026-09-06

The original run below predates connection-directed sending. See the
[latest verification run](#github-preparation-verification) for current results and
the observed performance limitations.

The fixed-size `PublishBatch` API, fixed-stride internal representation and dedicated
fixed-size encoder benchmarks have been removed. `PublishBatchWriter` uses the same
message offsets and lengths for single-frame and multipart entries. These results
measure the new API after removing writer monitors and the publisher's writer
registry. Writers, including close, now belong to their producer thread; see
[shutdown order](MULTIPART.md#sending). No CAS or spin lock replaces the monitors.
The measurements cover filtered routing; the historical always-match measurements
in `BENCHMARKS.md` and `MULTIPART_BENCHMARKS.md` are not directly comparable.

## Verification and commands

```sh
mvn -pl transport clean test -Donigiri.zmq=true -Dio.netty.leakDetection.level=paranoid
mvn -pl transport -Dtest=PublisherDataPathBenchmark test
```

The functional suite passed **133 tests**, with no failures or skips, including
native libzmq variable-size mixed single-frame/multipart batch interoperability.
Paranoid leak detection was enabled; no Netty leak reports were observed. The
benchmark is selected separately and runs two parameterized workloads. Performance
target comparisons are informational; batch accounting, malformed output, lane
failures and lost references fail its correctness checks.

## Topology and workload

- Intel Core i7-11700T, 8 physical cores / 16 logical CPUs; OpenJDK 64-Bit Server VM
  21.0.12; Netty 4.2.17.Final. Default JVM/Netty settings, no CPU pinning, no
  simultaneous benchmark jobs. Paranoid leak detection is off for benchmarks.
- One producer, five `DefaultEventLoopGroup` lane threads. Actual
  `newBatchWriter()` -> `add` / `addMultipart` -> encoding -> `PubServer` -> bounded
  MPSC queues -> topic matching -> whole-message filtered copies -> blackhole sinks.
- One sink per lane, subscribed to A/B/A/B/A respectively. Every batch includes
  both topics, so each sink receives half of its messages. This measures filtered
  routing and its extra destination copy, not shared-buffer-only fan-out.
- Each batch contains 256 logical messages. Lane queues hold 8192 batches and drain
  up to 256 per turn. Wire and source buffers use the pooled direct allocator.
  Sources are reused through retained references, with indices unchanged; the
  writer copies each input before releasing it. Source reference counts are checked.
- The four-entry cycle is A single-frame, A multipart, B single-frame, B multipart.
  Single-frame bodies contain an 8-byte topic prefix plus payload. Multipart entries
  contain separate 8-byte topic and payload frames. Both formats count as one logical
  message. Half the entries use each format.
- The primary payload cycle is **128, 192, 256, 224 bytes**, averaging **200 bytes**.
  There are 54,592 ZMTP wire bytes/batch, or 213.25 bytes/message including topic and
  headers. Short and long frame headers both occur.
- The reference cycle uses **200 payload bytes for every entry** through the same
  general API, with the same topic/format distribution: 54,016 ZMTP bytes/batch, or
  211 bytes/message. There is no fixed-size implementation branch to optimize.
- Each workload has 2 seconds warmup and 3 seconds throughput measurement, including
  settlement of outstanding lane batches. Variable runs first and the 200-byte
  reference follows in the same JVM. Reflection only installs sinks before timing.
- There are **no sockets or network transfer**. Sink work checks wire size and first
  prefix, reads one byte and updates counters. Native network interoperability is
  covered by functional tests, not this throughput measurement.

## Recorded run

Throughput uses payload bytes only, excluding topic and headers. Input throughput
is offered work; aggregate delivery counts matching copies actually delivered to
all sinks and must not be reported as publisher input throughput.

| Metric | Variable 128/192/256/224 | 200-byte reference |
| --- | ---: | ---: |
| Offered input GB/s | 2.766 | 2.864 |
| Offered million messages/s | 13.830 | 14.322 |
| Actual aggregate delivery GB/s | 6.639 | 7.161 |
| Actual aggregate million messages/s | 34.576 | 35.804 |
| Producer + lane Java B/input message | 14.510 | 14.496 |
| Producer + lane Java MB/s | 200.685 | 207.599 |
| Loaded processing p99.9 | 204.243 µs | 490.067 µs |
| Loaded latency samples | 12,665 | 13,110 |
| Single-in-flight processing p99.9 | 103.504 µs | 109.539 µs |
| Offered batches | 162,077 | 167,832 |
| Delivered lane-batches | 810,385 | 839,160 |
| Dropped lane-batches | 0 | 0 |
| Lane-batch delivery ratio | 100% | 100% |
| Elapsed, including settlement | 3.000041 s | 3.000023 s |

Variable workload A sinks each delivered 1.106 GB/s and B sinks each 1.660 GB/s.
Their message rates are the same (6.915 million/s), but their mean payloads differ:
160 bytes for A, 240 for B. Consequently, aggregate bytes use per-sink payload totals,
not aggregate message count multiplied by the overall 200-byte mean. With three A
sinks and two B sinks, variable-workload aggregate payload is 2.4 times input when
no drops occur. The reference workload has a 2.5 multiplier.

Before removing writer monitors, the recorded run was 2.611 GB/s / 121,402.822 µs
loaded p99.9 for variable messages and 2.314 GB/s / 91.097 µs for the 200-byte
reference. These are single-run observations in a scheduling-sensitive saturated
workload, not a controlled statistical comparison. The improved variable-run tail
must not be attributed entirely to monitor removal, nor treated as a tail guarantee.

## Latency, allocations and limits

Loaded latency timestamps the start of batch construction, before the first add,
and samples arrival at each individual sink every 64th lane-batch. It includes
construction/encoding of 256 messages, queue wait, scheduling, filtering and copying.
The combined percentile uses nearest rank, with bounded arrays of at most 131,072
samples per sink. It is not a per-message percentile or a slowest-sink completion
percentile. Network is absent; application time before entering the batch writer
is not measured. Do not divide batch percentiles by 256.

Single-in-flight latency is a separate 20,000-sample phase. It waits for all five
sinks before publishing the next batch. The small result in this phase does not
establish sub-millisecond latency under saturated load.

Java allocation uses ThreadMXBean across producer and five lanes during throughput,
including metadata, buffer wrappers, scheduling and instrumentation. It excludes
native/direct memory, other JVM threads, setup, sorting and reporting. B/message is
per offered input logical message and MB/s is decimal. Unsupported counters print
NaN. The native-memory copy/retention cost is not represented by Java B/message.

An earlier run, before writer monitor removal, of this same workload offered 2.566 GB/s, delivered 99.522% of lane
batches and dropped 3,595 lane-batches; loaded p99.9 was 197,987.844 µs. The old
benchmark's unconditional no-drop assertion failed under this intentional saturated
PUB workload. The revised benchmark reports drops and verifies
`delivered + dropped == offered * lanes`, while still rejecting lane failures and
requiring lossless single-in-flight samples. The zero-drop result in the recorded
rerun is not a guarantee of lossless sustained capacity; observed tail variability
and overflow are material limitations, not hidden or counted as useful delivery.

Neither workload reaches the **25 GB/s publisher input target**. Both recorded
workloads are below **processing p99.9 < 1 ms** in their stated latency scopes for
this run; earlier runs show substantial tail variability. Functional completion of
this change is not full performance acceptance. No fixed-size-specific optimization
was added. Subscriber code is unchanged in this change; this benchmark
does not re-establish the subscriber 5 GB/s target.

## Connection-send regression run

After adding `Connection.send` and subscription notifications, the full functional
suite passed **141 tests**, including native interoperability, with paranoid leak
detection and no failures or skips. The benchmark command and topology above are
unchanged: one producer, five lane threads, one filtered blackhole sink per lane,
256 messages per batch, no sockets or network. This run exercises normal broadcasts
through the shared lane routing code, not directed-send or callback throughput.

| Metric | Variable 128/192/256/224 | 200-byte reference |
| --- | ---: | ---: |
| Offered input GB/s | 3.412 | 2.758 |
| Offered million messages/s | 17.058 | 13.792 |
| Actual aggregate delivery GB/s | 7.579 | 6.896 |
| Actual aggregate million messages/s | 39.456 | 34.480 |
| Producer + lane Java B/input message | 13.166 | 14.510 |
| Producer + lane Java MB/s | 224.582 | 200.123 |
| Loaded processing p99.9 | 281,111.674 µs | 2,251.551 µs |
| Loaded latency samples | 15,002 | 12,625 |
| Single-in-flight processing p99.9 | 159.905 µs | 137.025 µs |
| Offered batches | 207,528 | 161,626 |
| Delivered lane-batches | 960,047 | 808,130 |
| Dropped lane-batches | 77,593 | 0 |
| Lane-batch delivery ratio | 92.522% | 100% |
| Elapsed, including settlement | 3.114511 s | 3.000034 s |

Neither workload meets the 25 GB/s input target or the loaded p99.9 < 1 ms
target. Latencies are batch processing samples as defined above, not per-message
percentiles. Higher offered variable throughput includes work subsequently dropped
and must not be interpreted as increased useful capacity. Its lower allocation
per offered message also includes rejected work. These single runs show substantial
tail variability; they do not establish either a performance improvement or the
absence of a regression. No subscriber throughput claim is made by this benchmark.

## GitHub preparation verification

Re-run on 2026-09-06 after raising the default subscription count to
`Integer.MAX_VALUE` and notification capacity to 65,536, with routing regression
tests included. The native functional suite passed **151 tests**, with zero
failures, errors or skips, under paranoid leak detection; no leak reports were
observed. The benchmark's two workloads also passed their correctness checks.

Commands are the same as above. Hardware: Intel Core i7-11700T, 8 cores / 16
logical CPUs; OpenJDK 21.0.12; Netty 4.2.17.Final. One producer, five EventLoop
lanes, one A/B prefix-filtered blackhole sink per lane, 256 messages per batch,
8192-batch lane queues and a 256-batch drain limit; no sockets or network transfer.
Half the entries are single-frame and half multipart, with an 8-byte topic and
either fixed 200-byte payloads or 128/192/256/224-byte payloads averaging 200 bytes.
Each workload uses 2 seconds warmup and 3 seconds throughput measurement plus
settlement. Variable runs first. No concurrent benchmark or test jobs, CPU pinning,
or paranoid leak detection during measurement.

| Metric | Variable 128/192/256/224 | 200-byte reference |
| --- | ---: | ---: |
| Offered input GB/s | 3.412 | 2.790 |
| Offered million messages/s | 17.058 | 13.949 |
| Actual aggregate delivery GB/s | 7.795 | 6.974 |
| Actual aggregate million messages/s | 40.585 | 34.872 |
| Producer + lane Java B/input message | 13.174 | 14.491 |
| Producer + lane Java MB/s | 224.729 | 202.126 |
| Loaded processing p99.9 | 156,822.265 µs | 1,701.841 µs |
| Loaded latency samples | 15,466 | 12,776 |
| Single-in-flight processing p99.9 | 154.438 µs | 131.768 µs |
| Offered batches | 208,023 | 163,533 |
| Delivered lane-batches | 989,861 | 817,665 |
| Dropped lane-batches | 50,254 | 0 |
| Lane-batch delivery ratio | 95.168% | 100% |
| Elapsed, including settlement | 3.121883 s | 3.001302 s |

Latency and allocation scopes are defined above: loaded p99.9 measures batch
construction through arrival at an individual sink, not per-message latency;
allocation covers producer and lane Java allocations and excludes native memory.
Aggregate delivery includes fan-out. Variable offered throughput includes dropped
work. Neither workload meets the 25 GB/s input or loaded p99.9 < 1 ms targets.
This single run does not establish a regression or an improvement, and does not
measure notification capacity, directed-send throughput or subscriber performance.
