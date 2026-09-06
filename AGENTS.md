# Engineering Requirements

## Performance targets

Treat performance as a primary design constraint for publisher and subscriber
implementation work.

- Benchmark payload size: 200 bytes per message.
- Publisher throughput target: 25 GB/s, measured in bytes per second (125 million
  200-byte messages per second).
- Subscriber client throughput target: 5 GB/s, measured in bytes per second (25
  million 200-byte messages per second).
- Excluding network transfer time, p99.9 processing latency must remain below one
  millisecond and should be minimized as far as practical.

Design hot paths around these targets. Avoid per-message futures, strings, temporary
collections, unnecessary payload copies, cross-thread handoffs, lock contention and
unbounded queues. Preserve explicit `ByteBuf` ownership and release buffers on every
drop, rejection, failure and close path.

Performance-sensitive changes must include representative 200-byte-message
benchmarks. Report throughput, p99.9 latency, allocation rate and the exact benchmark
topology so results distinguish local processing cost from network transfer time.
