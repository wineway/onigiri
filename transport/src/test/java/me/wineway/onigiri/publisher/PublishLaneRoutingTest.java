package me.wineway.onigiri.publisher;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoop;

class PublishLaneRoutingTest {
  @Test
  void changingBatchSizesAndSubscriptionsMatchNaivePrefixOracle() throws Exception {
    // Binary, overlapping and empty prefixes, across slots 63/64 and 127/128.
    byte[][] prefixes = {{}, {0}, {0, 1}, {(byte) 255}, {(byte) 255, 1}, {2}};
    var random = new Random(0x51A7E);
    try (var fixture = new Fixture(130)) {
      boolean[][] subscribed = new boolean[130][prefixes.length];
      for (int round = 0; round < 180; round++) {
        boolean[] writable = new boolean[130];
        for (int slot = 0; slot < 130; slot++) {
          writable[slot] = round % 11 != 0 && random.nextBoolean();
          for (int p = 0; p < prefixes.length; p++) {
            subscribed[slot][p] = random.nextInt(5) == 0;
          }
        }
        fixture.run(() -> {
          for (int slot = 0; slot < 130; slot++) {
            fixture.sinks[slot].reset();
            fixture.lane.updateWritable(slot, writable[slot]);
            for (int p = 0; p < prefixes.length; p++) {
              fixture.lane.updateSubscription(slot, prefix(prefixes[p]), subscribed[slot][p]);
            }
          }
        });
        int count = new int[] {8, 1, 2, 8, 3}[round % 5];
        byte[][] topics = new byte[count][];
        for (int i = 0; i < count; i++) {
          topics[i] = prefixes[random.nextInt(prefixes.length)].clone();
        }
        EncodedBatch batch = batch(topics, round);
        byte[][] messages = new byte[count][];
        for (int i = 0; i < count; i++) {
          messages[i] = new byte[batch.wireSize(i)];
          batch.content().getBytes(batch.frameOffset(i), messages[i]);
        }
        fixture.submit(batch);
        for (int slot = 0; slot < 130; slot++) {
          var expected = new ByteArrayOutputStream();
          for (int i = 0; i < count; i++) {
            boolean matches = false;
            for (int p = 0; p < prefixes.length; p++) {
              if (subscribed[slot][p] && startsWith(topics[i], prefixes[p])) matches = true;
            }
            if (writable[slot] && matches) expected.writeBytes(messages[i]);
          }
          String context = "round=" + round + ", slot=" + slot;
          assertArrayEquals(expected.toByteArray(), fixture.sinks[slot].received.toByteArray(), context);
          assertEquals(expected.size() == 0 ? 0 : 1, fixture.sinks[slot].flushes, context);
          assertTrue(fixture.sinks[slot].failures.isEmpty(), context);
        }
      }
      assertEquals(0, fixture.dropped.sum());
    }
  }

  @Test
  void equalDeliverableRoutesWithDifferentWordLengthsUseSharedWriteAfterRecovery() throws Exception {
    try (var fixture = new Fixture(130)) {
      fixture.run(() -> {
        fixture.subscribe(0, new byte[0]);
        fixture.subscribe(129, new byte[] {1});
        fixture.lane.updateWritable(129, false);
        // Shared delivery must not allocate a filtered output.
        fixture.sinks[0].allocationFailure = new IllegalStateException("Unexpected filtered path");
      });
      fixture.submit(batch(new byte[][] {{1}, {2}}, 1));
      assertEquals(1, fixture.sinks[0].flushes);
      fixture.run(() -> fixture.lane.updateWritable(0, false));
      fixture.submit(batch(new byte[][] {{2}}, 2));
      fixture.run(() -> fixture.lane.updateWritable(0, true));
      fixture.submit(batch(new byte[][] {{2}, {1}}, 3));
      assertEquals(2, fixture.sinks[0].flushes);
      assertEquals(0, fixture.sinks[129].received.size());
      assertEquals(0, fixture.dropped.sum());
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"shared", "filtered", "directed"})
  void rejectedWriteReleasesOwnershipAndDoesNotFlush(String path) throws Exception {
    try (var fixture = new Fixture(2)) {
      fixture.run(() -> {
        fixture.subscribe(0, path.equals("filtered") ? new byte[] {1} : new byte[0]);
        fixture.subscribe(1, new byte[0]);
        fixture.sinks[0].reject = true;
      });
      EncodedBatch batch = batch(new byte[][] {{1}, {2}}, 0);
      if (path.equals("directed")) batch = new EncodedBatch(batch.content(), fixture.sinks[0]);
      fixture.submit(batch);
      assertEquals(1, fixture.sinks[0].writes);
      assertEquals(0, fixture.sinks[0].flushes);
      assertEquals(0, fixture.sinks[0].received.size());
      assertEquals(path.equals("directed") ? 0 : 1, fixture.sinks[1].flushes);
      assertEquals(path.equals("directed") ? 0 : 408, fixture.sinks[1].received.size());
      assertEquals(0, fixture.dropped.sum());
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"allocation", "copy", "filteredWrite", "sharedWrite", "directedWrite"})
  void failuresReleaseBuffersAndDrainContinues(String failurePoint) throws Exception {
    try (var fixture = new Fixture(2)) {
      var failure = new IllegalStateException("injected");
      fixture.run(() -> {
        fixture.subscribe(0, failurePoint.equals("sharedWrite") ? new byte[0] : new byte[] {1});
        fixture.subscribe(1, new byte[0]);
        if (failurePoint.equals("allocation")) fixture.sinks[0].allocationFailure = failure;
        else if (failurePoint.equals("copy")) fixture.sinks[0].readOnlyOutput = true;
        else fixture.sinks[0].writeFailure = failure;
      });
      EncodedBatch batch = batch(new byte[][] {{1}, {2}}, 0);
      boolean directed = failurePoint.equals("directedWrite");
      if (directed) batch = new EncodedBatch(batch.content(), fixture.sinks[0]);
      fixture.submit(batch);
      assertEquals(1, fixture.lane.failedBatches());
      assertEquals(1, fixture.dropped.sum());
      assertEquals(1, fixture.sinks[0].failures.size());
      if (failurePoint.equals("copy")) {
        assertInstanceOf(java.nio.ReadOnlyBufferException.class, fixture.sinks[0].failures.get(0));
      } else assertSame(failure, fixture.sinks[0].failures.get(0));
      assertEquals(directed ? 0 : 1, fixture.sinks[1].failures.size());
      for (Sink sink : fixture.sinks) {
        assertEquals(0, sink.flushes);
        for (ByteBuf output : sink.allocated) assertEquals(0, output.refCnt());
        for (ByteBuf wire : sink.wires) assertEquals(0, wire.refCnt());
      }
      fixture.run(() -> {
        fixture.sinks[0].allocationFailure = null;
        fixture.sinks[0].writeFailure = null;
        fixture.sinks[0].readOnlyOutput = false;
      });
      fixture.submit(batch(new byte[][] {{1}, {2}}, 1));
      assertEquals(1, fixture.sinks[0].flushes);
      assertEquals(1, fixture.sinks[1].flushes);
      assertEquals(1, fixture.lane.failedBatches());
    }
  }

  private static boolean startsWith(byte[] topic, byte[] prefix) {
    if (prefix.length > topic.length) return false;
    for (int i = 0; i < prefix.length; i++) if (topic[i] != prefix[i]) return false;
    return true;
  }

  private static Prefix prefix(byte[] bytes) {
    ByteBuf buffer = Unpooled.wrappedBuffer(bytes);
    try { return Prefix.copyOf(buffer); }
    finally { buffer.release(); }
  }

  private static EncodedBatch batch(byte[][] topics, int sequence) {
    // Each multipart message has 200 body bytes. Nonzero readerIndex exercises offsets.
    ByteBuf wire = Unpooled.directBuffer(7 + topics.length * 204).writeZero(7);
    wire.readerIndex(7);
    int[] offsets = new int[topics.length + 1];
    int[] topicOffsets = new int[topics.length];
    int[] lengths = new int[topics.length];
    for (int i = 0; i < topics.length; i++) {
      wire.writeByte(1).writeByte(topics[i].length);
      topicOffsets[i] = wire.writerIndex() - 7;
      lengths[i] = topics[i].length;
      wire.writeBytes(topics[i]);
      int payloadLength = 200 - topics[i].length;
      wire.writeByte(0).writeByte(payloadLength);
      for (int j = 0; j < payloadLength; j++) wire.writeByte(sequence + i + j);
      offsets[i + 1] = wire.writerIndex() - 7;
    }
    return new EncodedBatch(wire, topics.length, offsets, topicOffsets, lengths);
  }

  private static final class Fixture implements AutoCloseable {
    final DefaultEventLoopGroup group = new DefaultEventLoopGroup(1);
    final EventLoop loop = group.next();
    final LongAdder dropped = new LongAdder();
    final PublishLane lane = new PublishLane(loop,
        new PubServerConfig(16, 16, 8, 256, 0, 0, 0, 0), dropped);
    final Sink[] sinks;

    Fixture(int count) throws Exception {
      sinks = new Sink[count];
      run(() -> {
        for (int i = 0; i < count; i++) {
          sinks[i] = new Sink();
          assertEquals(i, lane.register(sinks[i]));
          lane.activate(i);
        }
      });
    }

    void run(Runnable action) throws Exception { loop.submit(action).get(5, TimeUnit.SECONDS); }
    void subscribe(int slot, byte[] bytes) { lane.updateSubscription(slot, prefix(bytes), true); }
    void submit(EncodedBatch batch) throws Exception {
      ByteBuf wire = batch.content();
      lane.submit(batch);
      run(() -> {});
      assertEquals(0, wire.refCnt());
      for (Sink sink : sinks) {
        for (ByteBuf output : sink.allocated) assertEquals(0, output.refCnt());
        for (ByteBuf written : sink.wires) assertEquals(0, written.refCnt());
      }
    }
    public void close() throws Exception {
      try { lane.closeAsync().get(5, TimeUnit.SECONDS); }
      finally { group.shutdownGracefully(0, 5, TimeUnit.SECONDS).syncUninterruptibly(); }
    }
  }

  private static final class Sink implements SubscriberSink {
    final ByteArrayOutputStream received = new ByteArrayOutputStream();
    final List<Throwable> failures = new ArrayList<>();
    final List<ByteBuf> allocated = new ArrayList<>();
    final List<ByteBuf> wires = new ArrayList<>();
    RuntimeException allocationFailure;
    RuntimeException writeFailure;
    boolean readOnlyOutput;
    boolean reject;
    int writes;
    int flushes;

    public ByteBufAllocator allocator() {
      return (ByteBufAllocator) Proxy.newProxyInstance(ByteBufAllocator.class.getClassLoader(),
          new Class<?>[] {ByteBufAllocator.class}, (proxy, method, args) -> {
            if (!method.getName().equals("directBuffer") || args.length != 2) {
              throw new AssertionError("Unexpected allocation: " + method);
            }
            if (allocationFailure != null) throw allocationFailure;
            ByteBuf buffer = UnpooledByteBufAllocator.DEFAULT.directBuffer((int) args[0], (int) args[1]);
            if (readOnlyOutput) buffer = buffer.asReadOnly();
            allocated.add(buffer);
            return buffer;
          });
    }
    public boolean isWritable() { return true; }
    public boolean write(ByteBuf wire) {
      wires.add(wire);
      writes++;
      // SubscriberSink transfers ownership even when write throws.
      try {
        if (writeFailure != null) throw writeFailure;
        if (reject) return false;
        byte[] bytes = new byte[wire.readableBytes()];
        wire.getBytes(wire.readerIndex(), bytes);
        received.writeBytes(bytes);
        return true;
      } finally { wire.release(); }
    }
    public void flush() { flushes++; }
    public void fail(Throwable failure) { failures.add(failure); }
    void reset() {
      received.reset();
      failures.clear();
      allocated.clear();
      wires.clear();
      writes = flushes = 0;
    }
  }
}
