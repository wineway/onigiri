# Multipart measurements — 2026-09-06

> Historical measurements before the unified variable-size batch API. Some benchmark
> classes/commands below were retired with the fixed-size path. Current benchmark:
> `mvn -pl transport -Dtest=PublisherDataPathBenchmark test`.
> See [current batch measurements](BATCH_BENCHMARKS.md); old results are not current
> throughput claims and are not directly comparable to the new filtered topology.

The multipart feature passes functional/interop verification, but these measurements
**do not meet the repository's 25 GB/s publisher and 5 GB/s subscriber targets**.
The publisher's under-load processing p99.9 also exceeds 1 ms. Passing benchmark
correctness checks does not mean performance targets passed.

## Reproduce

Run benchmark jobs sequentially, separately from functional tests:

```sh
mvn -pl transport -Dtest=MultipartPublisherBenchmark test
mvn -pl transport -Dtest=SubClientBenchmark -Donigiri.multipart=true test
```

The subscriber benchmark needs Python with pyzmq/native libzmq. Select an interpreter
with `-Donigiri.python=/path/to/python`. Benchmarks use normal JVM/Netty defaults;
do not enable paranoid leak detection when comparing throughput. Functional checks:

```sh
mvn -pl transport -Donigiri.zmq=true -Dio.netty.leakDetection.level=paranoid test
```

Environment: Intel Core i7-11700T, 8 physical cores / 16 logical CPUs; OpenJDK
64-Bit Server VM 21.0.12; Netty 4.2.17.Final; pyzmq 27.1.0 / libzmq 4.3.5.
Fresh JVM per benchmark command, no CPU pinning and no simultaneous benchmark
jobs. Results are single-run observations, not statistical capacity guarantees.

Every logical message has an 8-byte topic and a 200-byte payload, in two frames:
212 encoded bytes. Throughput counts the 200 payload bytes, excluding topic and
headers; multiply by 1.06 for corresponding ZMTP byte throughput, before TCP/IP.
One multipart message counts as one message, not two frames.

## Publisher

Actual `MultipartBatchWriter.sendMore/sendLast` -> `PubServer.publishEncoded` ->
5 bounded MPSC publish lanes -> subscription matching -> shared buffer fan-out ->
1 always-match blackhole sink per lane. One producer thread; five
`DefaultEventLoopGroup` lane threads. Batches contain 256 messages, queue capacity
8192 batches/lane, drain limit 256. Source buffers are immutable pooled direct
buffers reused through retained references. The writer copies parts into a pooled
direct wire batch. Reflection only installs sinks and marks subscribers active
before timing; no reflection occurs in the measured publication path.

There are no sockets or network transfer, and no application work except reading
one wire byte in each sink. This topology exercises common routes, not the extra
payload copies required by filtered routes. Two seconds warmup, three seconds
throughput measurement, including settlement of outstanding lane batches.

| Metric | Result |
| --- | ---: |
| Input payload throughput | 2.092 GB/s |
| Input messages | 10.462 million/s |
| Aggregate delivered payload, all 5 sinks | 10.462 GB/s |
| Aggregate delivered messages, all 5 sinks | 52.309 million/s |
| Java allocation, producer + five lanes | 14.481 B/input message |
| Java allocation rate | 151.493 MB/s |
| Under-load processing p99.9 | 65,804.638 µs |
| Under-load latency samples | 9,575 |
| Single-in-flight processing p99.9 | 96.685 µs |
| Input batches | 122,601 |
| Delivered lane-batches | 613,005 |
| Dropped lane-batches | 0 |
| Measurement including settlement | 3.000039 s |

Input throughput is the publisher capacity comparison; aggregate delivery includes
five copies of each logical message and must not be compared with 25 GB/s as if it
were input throughput.

Under-load latency timestamps the first `sendMore` in each batch and samples arrival
at each individual sink, every 64th lane-batch, during throughput measurement. It
includes construction of all 256 messages, encoding, queue waiting and lane
scheduling; excludes network. Samples from five sinks are combined, with nearest-rank
p99.9. It is not a per-part latency or the completion time at the slowest sink for
each batch. Preallocated storage holds at most 131,072 samples per lane.

Single-in-flight latency is measured separately over 20,000 batches, waiting until
all five sinks receive each batch before issuing the next. Its sub-millisecond result
does not establish the required tail latency under load. Neither batch percentile
can be divided by 256 to infer a per-message percentile.

Allocation uses ThreadMXBean across the producer and five lane threads. It includes
batch metadata, buffer wrappers, queue scheduling and measurement instrumentation;
excludes native/direct memory, unrelated JVM threads and setup/sorting. It reports
bytes per input logical message and decimal MB/s. Counting and source-reference
ownership are verified, and every offered lane-batch must be delivered or dropped.

### Allocation fix found during implementation

An initial version used Netty's default adaptive allocator for the wire buffer.
JFR allocation sampling identified `DirectByteBuffer.duplicate()` below
`AdaptiveByteBuf.setBytes` for each topic/payload copy from the pooled input. The
initial run allocated 142.939 B/input message at 1.522 GB/s. Selecting
`PooledByteBufAllocator.DEFAULT` for multipart wire construction avoids these
per-part NIO duplicates; the recorded final run above allocated 14.481 B/message.
The profiled run is diagnostic and is not the reported final throughput run.

This improvement does not resolve the observed loaded tail latency. The test's
queue capacity, producer saturation and EventLoop scheduling remain part of that
measurement. Further queue/scheduling/GC profiling and capacity work are needed;
no claim is made that the 1 ms publisher target is met.

## Subscriber

One actual SubClient and one NIO EventLoop with `MultipartListener`, connected to
a native libzmq PUB. The complete-message callback validates two parts and reads
a payload byte into a checksum; no payload copy, application queue or retained
application work. Heartbeats are disabled. The default multipart timeout remains
enabled. Each phase has two seconds warmup and three seconds measurement.

| Mode | Payload GB/s | Million messages/s | Processing p99.9 | Samples | Java B/message | Java MB/s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Local, 1 message/input chunk | 1.496 | 7.481 | 0.175 µs | 21,918 | 0.000 | 0.000 |
| Local, 256 messages/input chunk | 4.232 | 21.160 | 14.888 µs/chunk | 15,499 | 0.000 | 0.000 |
| Native PUB -> loopback TCP -> SubClient | 0.073 | 0.367 | 4.857 µs | 17,204 | 59.185 | 21.711 |

Local modes inject a retained duplicate of a reusable pre-encoded pooled direct
buffer into the live pipeline on its EventLoop after a real handshake. They include
decoding, session checks, assembly, callback, release and read completion. They
exclude network, socket reads, inbound buffer allocation, scheduling and publisher
encoding. Each injection contains complete messages; fragmented-buffer assembly is
covered functionally but not benchmarked here. Zero allocation is rounded to three
decimal places and applies only to these favorable local injection modes.

Local p99.9 samples whole-chunk processing during throughput, every 1024 chunks
for batch 1 or every 16 chunks for batch 256. The 256-message percentile is a
whole-chunk value. It cannot be divided by 256 to derive message p99.9.

Network mode uses Python `send_multipart([topic, payload], copy=True)` over native
libzmq with SNDHWM 8192. The source writes a sequence number into reusable payload
storage. It offered and delivered 1,101,056 messages, with zero observed loss,
over 3.001527 s including final-marker settlement. The receiver saw 837,621
data-bearing chunks, averaging 1.31 messages/chunk. Sequence/order, counts and
checksums are checked. Python production cost is included in network throughput;
this is not an isolated test of Java subscriber capacity.

Network p99.9 samples chunk arrival in Java to the complete-message callback every
64th message. It excludes network/socket wait, scheduling before the read and time
waiting for earlier fragments. It does not measure publisher-to-subscriber latency.
Allocation covers only the subscriber EventLoop's Java allocations, not native
memory or the Python/libzmq process. Sample storage is bounded at 131,072 durations.

Both local modes and network receive processing remained below 1 ms for their stated
latency scopes. The best local throughput, 4.232 GB/s, is below the 5 GB/s target.
