package me.wineway.onigiri.publisher;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.Timeout;

import com.sun.management.ThreadMXBean;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoop;
import io.netty.util.ReferenceCountUtil;

/**
 * Manually selected publisher data-path benchmark.
 * Run with: mvn -pl transport -Dtest=PublisherDataPathBenchmark test
 */
class PublisherDataPathBenchmark {
  private static final int MESSAGE_SIZE = 200;
  private static final int MESSAGES_PER_BATCH = 256;
  private static final int LANE_COUNT = 5;
  private static final int LATENCY_SAMPLES = 20_000;
  private static final long WARMUP_NANOS = TimeUnit.SECONDS.toNanos(2);
  private static final long MEASURE_NANOS = TimeUnit.SECONDS.toNanos(3);
  private static final long SETTLE_TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(10);
  private static volatile int blackhole;

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  @Timeout(45)
  void mixedVariableMessagesAcrossFiveLanes(boolean variable) throws Exception {
    var group = new DefaultEventLoopGroup(LANE_COUNT);
    var allocator = PooledByteBufAllocator.DEFAULT;

    var config = new PubServerConfig(8192, 256, MESSAGES_PER_BATCH, 0, 0, 0, 0);
    var publisher = new PubServer(group, group, config, new PubServer.Listener() {});
    var lanes = (PublishLane[]) MultipartWriterTest.field(publisher, "lanes");
    var dropped = (LongAdder) MultipartWriterTest.field(publisher, "droppedBatches");
    var writer = publisher.newBatchWriter();
    ((java.util.concurrent.atomic.AtomicInteger) MultipartWriterTest.field(publisher, "activeSubscribers")).set(LANE_COUNT);
    var inputs = new Inputs(variable);
    var sinks = new BlackholeSink[LANE_COUNT];
    long[] measuredThreads = new long[LANE_COUNT + 1];
    measuredThreads[0] = Thread.currentThread().getId();

    try {
      int laneIndex = 0;
      for (PublishLane lane : lanes) {
        EventLoop eventLoop = lane.eventLoop();
        BlackholeSink sink = new BlackholeSink(allocator, inputs, laneIndex & 1);
        lanes[laneIndex] = lane;
        sinks[laneIndex] = sink;
        int threadIndex = laneIndex + 1;
        eventLoop.submit(() -> {
          measuredThreads[threadIndex] = Thread.currentThread().getId();
          int slot = lane.register(sink);
          assertTrue(slot >= 0);
          ByteBuf prefix = Unpooled.buffer(1).writeByte(sink.prefix);
          try { lane.updateSubscription(slot, Prefix.copyOf(prefix), true); }
          finally { prefix.release(); }
          lane.activate(slot);
        }).syncUninterruptibly();
        laneIndex++;
      }
      assertEquals(LANE_COUNT, laneIndex);

      long warmupOffered = runUntil(writer, inputs, System.nanoTime() + WARMUP_NANOS);
      awaitSettled(sinks, dropped, warmupOffered * LANE_COUNT, 0, 0);

      for (int i = 0; i < lanes.length; i++) {
        BlackholeSink sink = sinks[i];
        lanes[i].eventLoop().submit(() -> { sink.sampleCount = 0; sink.measure = true; }).syncUninterruptibly();
      }
      long deliveredBefore = deliveredBatches(sinks);
      long[] laneDelivered = new long[LANE_COUNT];
      for (int i = 0; i < LANE_COUNT; i++) laneDelivered[i] = sinks[i].receivedBatches;
      long droppedBefore = dropped.sum();
      long allocatedBefore = allocatedBytes(measuredThreads);
      long start = System.nanoTime();
      long offered = runUntil(writer, inputs, start + MEASURE_NANOS);
      awaitSettled(sinks, dropped, offered * LANE_COUNT, deliveredBefore, droppedBefore);
      long elapsed = System.nanoTime() - start;
      long allocatedAfter = allocatedBytes(measuredThreads);
      long delivered = deliveredBatches(sinks) - deliveredBefore;
      long lost = dropped.sum() - droppedBefore;
      for (int i = 0; i < LANE_COUNT; i++) {
        laneDelivered[i] = sinks[i].receivedBatches - laneDelivered[i];
      }

      long[] loadedLatency = new long[sinks.length * 131072];
      int loadedSamples = 0;
      for (int i = 0; i < lanes.length; i++) {
        BlackholeSink sink = sinks[i];
        lanes[i].eventLoop().submit(() -> sink.measure = false).syncUninterruptibly();
        System.arraycopy(sink.latency, 0, loadedLatency, loadedSamples, sink.sampleCount);
        loadedSamples += sink.sampleCount;
      }
      Arrays.sort(loadedLatency, 0, loadedSamples);
      long loadedP999 = loadedLatency[Math.min(loadedSamples - 1, (int) Math.ceil(loadedSamples * .999) - 1)];
      long[] latency = latencySamples(writer, inputs, sinks, dropped);
      Arrays.sort(latency);
      long p999 = latency[(int) (latency.length * 0.999)];

      long offeredMessages = offered * MESSAGES_PER_BATCH;
      long deliveredMessages = delivered * (MESSAGES_PER_BATCH / 2);
      long deliveredBytes = 0;
      for (int i = 0; i < sinks.length; i++) deliveredBytes += laneDelivered[i] * sinks[i].payloadBytes;
      double seconds = elapsed / 1_000_000_000.0;
      double inputGbps = offeredMessages * (double) MESSAGE_SIZE / seconds / 1_000_000_000.0;
      double aggregateGbps = deliveredBytes / seconds / 1_000_000_000.0;
      double allocatedPerMessage = allocatedBefore < 0 || allocatedAfter < 0
          ? Double.NaN
          : (allocatedAfter - allocatedBefore) / (double) offeredMessages;

      System.out.println("\nUnified publisher batch benchmark: " + (variable ? "variable 128/192/256/224" : "fixed200 reference"));
      System.out.printf("  topology: 1 producer, %d event-loop lanes, 1 prefix-filtered sink/lane (A/B alternating), no network%n"
              + "  covered: encode, MPSC handoff, subscription match, whole-message filtered copies, sink handoff%n"
              + "  runtime: %s %s, processors=%d%n"
              + "  topic=8 B, mean payload: %d B, 50%% single + 50%% multipart, batch=%d messages%n"
              + "  offered input throughput: %.3f GB/s, %.3f Mmsg/s%n"
              + "  aggregate delivery: %.3f GB/s, %.3f Mmsg/s%n"
              + "  dropped lane-batches: %d%n"
              + "  p99.9 single-in-flight publish-to-all-sinks latency: %.3f us%n"
              + "  producer + lane Java allocation: %.3f B/input-message, %.3f MB/s%n",
          LANE_COUNT, System.getProperty("java.vm.name"), System.getProperty("java.version"),
          Runtime.getRuntime().availableProcessors(), MESSAGE_SIZE, MESSAGES_PER_BATCH,
          inputGbps, offeredMessages / seconds / 1_000_000.0,
          aggregateGbps, deliveredMessages / seconds / 1_000_000.0,
          lost, p999 / 1000.0, allocatedPerMessage,
          allocatedPerMessage * offeredMessages / seconds / 1_000_000.0);
      System.out.printf("  p99.9 under load, first add -> individual sink: %.3f us, samples=%d%n"
              + "  targets: offered input >=25 GB/s AND no drops: %s; loaded processing p99.9 <1 ms: %s%n",
          loadedP999 / 1000.0, loadedSamples, inputGbps >= 25 && lost == 0, loadedP999 < 1_000_000);
      System.out.printf("  measurement including settlement: %.6f s%n"
              + "  offered batches: %d, delivered lane-batches: %d, delivery ratio: %.3f%%%n",
          seconds, offered, delivered, delivered * 100.0 / (offered * LANE_COUNT));
      long laneTotal = 0;
      for (int i = 0; i < LANE_COUNT; i++) {
        laneTotal += laneDelivered[i];
        double messagesPerSecond = laneDelivered[i] * (double) (MESSAGES_PER_BATCH / 2) / seconds;
        System.out.printf("  lane %d delivery: %.3f GB/s, %.3f Mmsg/s, %d batches%n",
            i, laneDelivered[i] * (double) sinks[i].payloadBytes / seconds / 1_000_000_000.0,
            messagesPerSecond / 1_000_000.0, laneDelivered[i]);
      }

      assertTrue(offered > 0);
      assertEquals(delivered, laneTotal);
      assertEquals(offered * LANE_COUNT, delivered + lost, "Every lane-batch must be accounted for");
      // Saturated PUB queues may drop. Account for and report drops instead of
      // mistaking offered input throughput for lossless delivery capacity.
      assertTrue(delivered > 0);
      assertEquals(0, publisher.dropCounts().failedLaneBatches());
      assertEquals(0, publisher.dropCounts().stoppedLanes());
      assertTrue(loadedSamples > 0);
      for (BlackholeSink sink : sinks) assertNull(sink.failure);
    } finally {
      writer.close();
      inputs.close();
      publisher.closeAsync().get(5, TimeUnit.SECONDS);
      group.shutdownGracefully(0, 5, TimeUnit.SECONDS).syncUninterruptibly();
    }
  }

  private static long runUntil(PublishBatchWriter writer, Inputs inputs,
      long deadline) {
    long batches = 0;
    do {
      publish(writer, inputs);
      batches++;
    } while (System.nanoTime() < deadline);
    return batches;
  }

  private static long[] latencySamples(PublishBatchWriter writer, Inputs inputs,
      BlackholeSink[] sinks, LongAdder dropped) {
    long[] samples = new long[LATENCY_SAMPLES];
    for (int i = 0; i < samples.length; i++) {
      long[] expected = new long[sinks.length];
      for (int lane = 0; lane < sinks.length; lane++) {
        expected[lane] = sinks[lane].receivedBatches + 1;
      }
      long droppedBefore = dropped.sum();
      long start = System.nanoTime();
      publish(writer, inputs);
      awaitEachSink(sinks, expected);
      samples[i] = System.nanoTime() - start;
      assertEquals(droppedBefore, dropped.sum(), "latency sample was dropped");
    }
    return samples;
  }

  private static void publish(PublishBatchWriter writer, Inputs inputs) {
    long start = System.nanoTime();
    for (int i = 0; i < inputs.bodies.length; i++) inputs.bodies[i].setLong((i & 1) == 0 ? 8 : 0, start);
    for (int i = 0; i < MESSAGES_PER_BATCH; i++) {
      int entry = i & 3;
      if ((entry & 1) == 0) writer.add(inputs.bodies[entry].retain());
      else writer.addMultipart(inputs.topics[entry >>> 1].retain(), inputs.bodies[entry].retain());
    }
  }

  private static final class Inputs implements AutoCloseable {
    final int[] sizes;
    final ByteBuf[] topics = new ByteBuf[2];
    final ByteBuf[] bodies = new ByteBuf[4];
    Inputs(boolean variable) {
      sizes = variable ? new int[] {128, 192, 256, 224} : new int[] {200, 200, 200, 200};
      for (int i = 0; i < topics.length; i++) {
        topics[i] = PooledByteBufAllocator.DEFAULT.directBuffer(8).writeByte(65 + i).writeZero(7);
      }
      for (int i = 0; i < bodies.length; i++) {
        int topicSize = (i & 1) == 0 ? 8 : 0;
        bodies[i] = PooledByteBufAllocator.DEFAULT.directBuffer(sizes[i] + topicSize).writeZero(sizes[i] + topicSize);
        if (topicSize != 0) bodies[i].setByte(0, 65 + (i >>> 1));
      }
    }
    public void close() {
      for (ByteBuf body : bodies) { assertEquals(1, body.refCnt()); body.release(); }
      for (ByteBuf topic : topics) { assertEquals(1, topic.refCnt()); topic.release(); }
    }
  }

  private static void awaitSettled(BlackholeSink[] sinks, LongAdder dropped, long expected,
      long deliveredBefore, long droppedBefore) {
    long deadline = System.nanoTime() + SETTLE_TIMEOUT_NANOS;
    while (deliveredBatches(sinks) - deliveredBefore + dropped.sum() - droppedBefore < expected) {
      if (System.nanoTime() >= deadline) {
        throw new AssertionError("Publisher lanes did not settle");
      }
      Thread.onSpinWait();
    }
  }

  private static void awaitEachSink(BlackholeSink[] sinks, long[] expected) {
    long deadline = System.nanoTime() + SETTLE_TIMEOUT_NANOS;
    for (;;) {
      boolean complete = true;
      for (int i = 0; i < sinks.length; i++) {
        if (sinks[i].receivedBatches < expected[i]) {
          complete = false;
          break;
        }
      }
      if (complete) return;
      if (System.nanoTime() >= deadline) {
        throw new AssertionError("Publisher latency sample did not arrive");
      }
      Thread.onSpinWait();
    }
  }

  private static long deliveredBatches(BlackholeSink[] sinks) {
    long delivered = 0;
    for (BlackholeSink sink : sinks) delivered += sink.receivedBatches;
    return delivered;
  }

  private static long allocatedBytes(long[] threads) {
    java.lang.management.ThreadMXBean base = ManagementFactory.getThreadMXBean();
    if (!(base instanceof ThreadMXBean bean) || !bean.isThreadAllocatedMemorySupported()) return -1;
    if (!bean.isThreadAllocatedMemoryEnabled()) bean.setThreadAllocatedMemoryEnabled(true);
    long total = 0;
    for (long thread : threads) {
      long allocated = bean.getThreadAllocatedBytes(thread);
      if (allocated < 0) return -1;
      total += allocated;
    }
    return total;
  }

  private static final class BlackholeSink implements SubscriberSink {
    private final ByteBufAllocator allocator;
    private final int prefix;
    private final int payloadBytes;
    private final int wireBytes;
    private final int firstHeader;
    private final long[] latency = new long[131072];
    private boolean measure;
    private int sampleCount;
    private volatile long receivedBatches;
    private volatile Throwable failure;

    private BlackholeSink(ByteBufAllocator allocator, Inputs inputs, int prefixIndex) {
      this.allocator = allocator;
      prefix = 65 + prefixIndex;
      int first = inputs.sizes[prefixIndex * 2];
      int second = inputs.sizes[prefixIndex * 2 + 1];
      firstHeader = first + 8 > 255 ? 9 : 2;
      payloadBytes = (first + second) * (MESSAGES_PER_BATCH / 4);
      wireBytes = (first + 8 + firstHeader + 10 + second + (second > 255 ? 9 : 2))
          * (MESSAGES_PER_BATCH / 4);
    }

    @Override
    public ByteBufAllocator allocator() {
      return allocator;
    }

    @Override
    public boolean isWritable() {
      return true;
    }

    @Override
    public boolean write(ByteBuf wire) {
      try {
        assertEquals(wireBytes, wire.readableBytes());
        assertEquals(prefix, wire.getUnsignedByte(wire.readerIndex() + firstHeader));
        blackhole ^= wire.getUnsignedByte(wire.writerIndex() - 1);
        if (measure && (receivedBatches & 63) == 0 && sampleCount < latency.length) {
          latency[sampleCount++] = System.nanoTime() - wire.getLong(wire.readerIndex() + firstHeader + 8);
        }
        receivedBatches++;
        return true;
      } finally {
        ReferenceCountUtil.release(wire);
      }
    }

    @Override
    public void flush() {}

    @Override
    public void fail(Throwable failure) {
      this.failure = failure;
    }
  }
}
