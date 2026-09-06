package me.wineway.onigiri.publisher;

import static org.junit.jupiter.api.Assertions.*;
import io.netty.buffer.*;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.embedded.EmbeddedChannel;
import me.wineway.onigiri.protocol.Decoder;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class MultipartWriterTest {
  @Test
  void batchesRouteEntireMessagesByTopicAndDoNotPublishPartialParts() throws Exception {
    try (Fixture f = new Fixture()) {
      Sink a = f.subscribe("A");
      Sink b = f.subscribe("B");
      try (var writer = f.pub.newBatchWriter()) {
        send(writer, true, "A");
        assertThrows(IllegalStateException.class, writer::flush);
        f.drain();
        assertTrue(a.messages.isEmpty());
        send(writer, false, "B-payload");
        send(writer, true, "B");
        send(writer, true, "");
        send(writer, false, "A-payload");
        send(writer, true, "A");
        send(writer, false, "x".repeat(256));
        writer.flush();
      }
      f.drain();
      assertEquals(List.of(List.of("A", "B-payload"), List.of("A", "x".repeat(256))), a.messages);
      assertEquals(List.of(List.of("B", "", "A-payload")), b.messages);
      // All messages now share the same route: exercise shared duplicate path too.
      try (var writer = f.pub.newBatchWriter()) {
        send(writer, true, "A"); send(writer, false, "1");
        send(writer, true, "A"); send(writer, false, "2"); writer.flush();
      }
      f.drain();
      assertEquals(List.of("A", "2"), a.messages.get(3));
      assertEquals(1, b.messages.size());
    }
  }

  @Test
  void abortFailureAndOwnerCloseReleasePendingBuffers() throws Exception {
    try (Fixture f = new Fixture()) {
      Sink all = f.subscribe("");
      var writer = f.pub.newMultipartWriter();
      ByteBuf last = Unpooled.buffer().writeByte(1);
      assertThrows(IllegalStateException.class, () -> writer.sendLast(last));
      assertEquals(0, last.refCnt());
      send(writer, true, "aborted");
      ByteBuf pending = (ByteBuf) field(writer, "wire");
      writer.abort();
      send(writer, true, ""); send(writer, false, "");
      f.drain();
      assertEquals(List.of(List.of("", "")), all.messages);
      assertEquals(0, pending.refCnt());
      send(writer, true, "unfinished");
      pending = (ByteBuf) field(writer, "wire");
      writer.close();
      f.pub.closeAsync().get(5, TimeUnit.SECONDS);
      assertEquals(0, pending.refCnt());
      ByteBuf rejected = Unpooled.buffer().writeByte(1);
      assertThrows(IllegalStateException.class, () -> writer.sendLast(rejected));
      assertEquals(0, rejected.refCnt());
      writer.close();
    }
  }

  @Test
  void limitsAbortCurrentMessageButPreserveCompletedBatch() throws Exception {
    try (Fixture f = new Fixture(new PubServerConfig(16, 16, 4, 16, 0, 0, 0, 0,
        16, 16, 3, 256, 1024))) {
      Sink all = f.subscribe("");
      try (var writer = f.pub.newBatchWriter()) {
        send(writer, true, "topic"); send(writer, false, "good");
        send(writer, true, "topic");
        ByteBuf tooLarge = Unpooled.buffer(256).writeZero(256);
        assertThrows(IllegalArgumentException.class, () -> writer.sendLast(tooLarge));
        assertEquals(0, tooLarge.refCnt());
        send(writer, true, "topic"); send(writer, true, "part");
        ByteBuf tooMany = Unpooled.buffer().writeByte(1);
        assertThrows(IllegalArgumentException.class, () -> writer.sendMore(tooMany));
        assertEquals(0, tooMany.refCnt());
        writer.flush();
      }
      f.drain();
      assertEquals(List.of(List.of("topic", "good")), all.messages);
    }
  }

  @Test
  void separateWritersDoNotInterleaveMessages() throws Exception {
    try (Fixture f = new Fixture()) {
      Sink all = f.subscribe("");
      try (var first = f.pub.newMultipartWriter(); var second = f.pub.newMultipartWriter()) {
        send(first, true, "A"); send(second, true, "B");
        send(second, false, "2"); send(first, false, "1");
      }
      f.drain();
      assertEquals(List.of(List.of("B", "2"), List.of("A", "1")), all.messages);
    }
  }

  @Test
  void byteBudgetFlushesOnlyCompletedMessagesBeforeStartingNext() throws Exception {
    try (Fixture f = new Fixture(new PubServerConfig(16, 16, 4, 16, 0, 0, 0, 0,
        16, 16, 3, 256, 400))) {
      Sink all = f.subscribe("");
      try (var writer = f.pub.newBatchWriter()) {
        send(writer, true, "A"); send(writer, false, "x".repeat(200));
        send(writer, true, "B"); // space reservation submits A, never partial B
        f.drain();
        assertEquals(List.of(List.of("A", "x".repeat(200))), all.messages);
        writer.abort();
        writer.flush();
      }
      f.drain();
      assertEquals(1, all.messages.size());
    }
  }

  @Test
  void fullMessageCountAutomaticallySubmitsAndWriterCanBeReused() throws Exception {
    try (Fixture f = new Fixture(new PubServerConfig(16, 16, 2, 16, 0, 0, 0, 0))) {
      Sink all = f.subscribe("");
      try (var writer = f.pub.newBatchWriter()) {
        for (int i = 0; i < 6; i++) {
          send(writer, true, "A"); send(writer, false, Integer.toString(i));
        }
        writer.flush();
      }
      f.drain();
      assertEquals(6, all.messages.size());
      for (int i = 0; i < 6; i++) assertEquals(List.of("A", Integer.toString(i)), all.messages.get(i));
    }
  }

  @Test
  void convenienceConsumesBothReferencesOnRejectionAndPreservesBorrowedIndices() throws Exception {
    try (Fixture f = new Fixture()) {
      ByteBuf topic = Unpooled.buffer().writeByte(0).writeByte(65);
      ByteBuf payload = Unpooled.buffer().writeByte(0).writeInt(42);
      topic.readByte(); payload.readByte();
      f.pub.publishMultipart(topic.retain(), payload.retain());
      assertEquals(1, topic.refCnt()); assertEquals(1, payload.refCnt());
      assertEquals(1, topic.readerIndex()); assertEquals(1, payload.readerIndex());
      assertEquals(1, f.pub.dropCounts().noSubscribers());
      topic.release(); payload.release();
      ByteBuf rejected = Unpooled.buffer().writeByte(1);
      assertThrows(NullPointerException.class, () -> f.pub.publishMultipart(null, rejected));
      assertEquals(0, rejected.refCnt());
      f.pub.closeAsync().get(5, TimeUnit.SECONDS);
      ByteBuf t = Unpooled.buffer().writeByte(1), p = Unpooled.buffer().writeByte(2);
      assertThrows(IllegalStateException.class, () -> f.pub.publishMultipart(t, p));
      assertEquals(0, t.refCnt()); assertEquals(0, p.refCnt());
    }
  }

  @Test
  void multipartQueueOverflowAndLaneStopReleaseWholeWireBatches() throws Exception {
    try (Fixture f = new Fixture(new PubServerConfig(2, 1, 2, 16, 0, 0, 0, 0))) {
      Sink all = f.subscribe("");
      var blocked = new java.util.concurrent.CountDownLatch(1);
      var resume = new java.util.concurrent.CountDownLatch(1);
      ByteBuf[] pending = new ByteBuf[3];
      f.lane.eventLoop().execute(() -> {
        blocked.countDown();
        try { resume.await(); }
        catch (InterruptedException failure) { Thread.currentThread().interrupt(); }
      });
      try {
        assertTrue(blocked.await(5, TimeUnit.SECONDS));
        try (var writer = f.pub.newMultipartWriter()) {
          for (int i = 0; i < pending.length; i++) {
            send(writer, true, "A");
            pending[i] = (ByteBuf) field(writer, "wire");
            send(writer, false, "payload");
          }
        }
        assertEquals(1, f.lane.fullQueueDrops());
        assertEquals(0, pending[2].refCnt());
        var closing = f.pub.closeAsync();
        resume.countDown();
        closing.get(5, TimeUnit.SECONDS);
        for (ByteBuf wire : pending) assertEquals(0, wire.refCnt());
        assertTrue(all.messages.isEmpty());
      } finally { resume.countDown(); }
    }
  }

  @Test
  void unifiedBatchMixesVariableSingleAndMultipartMessagesWithoutLosingBoundaries() throws Exception {
    try (Fixture f = new Fixture()) {
      Sink a = f.subscribe("A"), b = f.subscribe("B"), all = f.subscribe("");
      try (var writer = f.pub.newBatchWriter()) {
        writer.add(bytes("A"));
        writer.addMultipart(bytes("B"), bytes("A".repeat(255)));
        writer.add(bytes("B".repeat(256)));
        writer.sendMore(bytes("A"));
        writer.sendMore(bytes(""));
        writer.sendLast(bytes("B".repeat(128)));
        writer.add(bytes("A".repeat(192)));
        writer.flush();
      }
      f.drain();
      assertEquals(List.of(List.of("A"), List.of("A", "", "B".repeat(128)),
          List.of("A".repeat(192))), a.messages);
      assertEquals(List.of(List.of("B", "A".repeat(255)), List.of("B".repeat(256))), b.messages);
      assertEquals(5, all.messages.size());
      assertEquals(List.of("B", "A".repeat(255)), all.messages.get(1));
      // Common routes with mixed representations exercise whole-batch sharing too.
      try (var writer = f.pub.newBatchWriter()) {
        writer.add(bytes("A-short"));
        writer.addMultipart(bytes("A-long"), bytes("z".repeat(4096)));
        writer.flush();
      }
      f.drain();
      assertEquals(List.of("A-long", "z".repeat(4096)), a.messages.get(4));
      assertEquals(2, b.messages.size());
    }
  }

  @Test
  void addingACompleteMessageDuringStagedMultipartRejectsAndReleasesWithoutMerging() throws Exception {
    try (Fixture f = new Fixture()) {
      Sink all = f.subscribe("");
      try (var writer = f.pub.newBatchWriter()) {
        writer.add(bytes("keep"));
        writer.sendMore(bytes("abort"));
        ByteBuf invalid = bytes("must-not-be-a-payload");
        assertThrows(IllegalStateException.class, () -> writer.add(invalid));
        assertEquals(0, invalid.refCnt());
        writer.sendMore(bytes("abort"));
        ByteBuf topic = bytes("new-topic"), payload = bytes("new-payload");
        assertThrows(IllegalStateException.class, () -> writer.addMultipart(topic, payload));
        assertEquals(0, topic.refCnt()); assertEquals(0, payload.refCnt());
        writer.add(bytes("after"));
        writer.flush();
      }
      f.drain();
      assertEquals(List.of(List.of("keep"), List.of("after")), all.messages);
    }
  }

  @Test
  void singleFrameByteBudgetAndMessageCountFlushVariableEntries() throws Exception {
    try (Fixture f = new Fixture(new PubServerConfig(16, 16, 2, 16, 0, 0, 0, 0,
        16, 16, 2, 256, 280))) {
      Sink all = f.subscribe("");
      try (var writer = f.pub.newBatchWriter()) {
        ByteBuf first = bytes("x".repeat(256));
        writer.add(first);
        assertEquals(0, first.refCnt());
        writer.add(bytes("y".repeat(128))); // 265 + 130 > 280, submits first
        f.drain();
        assertEquals(List.of(List.of("x".repeat(256))), all.messages);
        writer.add(bytes("z")); // count limit submits y + z
        f.drain();
        assertEquals(3, all.messages.size());
        writer.add(bytes("discard-on-close"));
      }
      f.drain();
      assertEquals(List.of(List.of("x".repeat(256)), List.of("y".repeat(128)), List.of("z")), all.messages);
    }
  }

  @Test
  void singleFrameInvalidAndClosedAddsConsumeInputs() throws Exception {
    try (Fixture f = new Fixture(new PubServerConfig(16, 16, 2, 16, 0, 0, 0, 0,
        16, 16, 2, 256, 280))) {
      var writer = f.pub.newBatchWriter();
      ByteBuf empty = Unpooled.buffer(1), oversized = Unpooled.buffer(257).writeZero(257);
      assertThrows(IllegalArgumentException.class, () -> writer.add(empty));
      assertThrows(IllegalArgumentException.class, () -> writer.add(oversized));
      assertEquals(0, empty.refCnt()); assertEquals(0, oversized.refCnt());
      writer.add(bytes("pending"));
      ByteBuf pending = (ByteBuf) field(writer, "wire");
      writer.close();
      f.pub.closeAsync().get(5, TimeUnit.SECONDS);
      assertEquals(0, pending.refCnt());
      ByteBuf rejected = bytes("closed");
      assertThrows(IllegalStateException.class, () -> writer.add(rejected));
      assertEquals(0, rejected.refCnt());
    }
  }

  @Test
  void stopRequestLeavesCleanupToProducerBeforePublisherShutdown() throws Exception {
    producerOwnsShutdown(false);
  }

  @Test
  void earlyPublisherCloseNeverReleasesPausedProducerStorage() throws Exception {
    producerOwnsShutdown(true);
  }

  private static void producerOwnsShutdown(boolean closePublisherEarly) throws Exception {
    try (Fixture f = new Fixture()) {
      var prepared = new java.util.concurrent.CountDownLatch(1);
      var stop = new java.util.concurrent.CountDownLatch(1);
      var pending = new java.util.concurrent.atomic.AtomicReference<ByteBuf>();
      var failure = new java.util.concurrent.atomic.AtomicReference<Throwable>();
      var producer = new Thread(() -> {
        try (var writer = f.pub.newBatchWriter()) {
          writer.add(bytes("unflushed"));
          writer.sendMore(bytes("unfinished"));
          pending.set((ByteBuf) field(writer, "wire"));
          prepared.countDown();
          if (!stop.await(5, TimeUnit.SECONDS)) throw new AssertionError("No stop request");
          if (closePublisherEarly) {
            ByteBuf rejected = bytes("payload");
            assertThrows(IllegalStateException.class, () -> writer.sendLast(rejected));
            assertEquals(0, rejected.refCnt());
          }
          // try-with-resources releases complete and partial local entries on their owner.
        } catch (Throwable cause) {
          failure.set(cause);
          prepared.countDown();
        }
      }, "test-publisher-producer");
      producer.start();
      try {
        assertTrue(prepared.await(5, TimeUnit.SECONDS));
        assertNull(failure.get());
        assertNotNull(pending.get());
        if (closePublisherEarly) f.pub.closeAsync().get(5, TimeUnit.SECONDS);
        assertEquals(1, pending.get().refCnt(), "Only the producer may release its local wire");
        stop.countDown();
        producer.join(5000);
        assertFalse(producer.isAlive());
        assertNull(failure.get());
        assertEquals(0, pending.get().refCnt());
        f.pub.closeAsync().get(5, TimeUnit.SECONDS);
      } finally {
        stop.countDown();
        producer.join(5000);
      }
    }
  }

  static ByteBuf bytes(String text) {
    return Unpooled.copiedBuffer(text, java.nio.charset.StandardCharsets.UTF_8);
  }

  static void send(MultipartWriter writer, boolean more, String text) {
    ByteBuf part = Unpooled.copiedBuffer(text, java.nio.charset.StandardCharsets.UTF_8);
    if (more) writer.sendMore(part); else writer.sendLast(part);
    // Netty's shared empty buffer is unreleasable.
    if (!text.isEmpty()) assertEquals(0, part.refCnt());
  }

  static Object field(Object target, String name) throws Exception {
    Class<?> type = target.getClass();
    while (type != null) {
      try { var field = type.getDeclaredField(name); field.setAccessible(true); return field.get(target); }
      catch (NoSuchFieldException ignored) { type = type.getSuperclass(); }
    }
    throw new NoSuchFieldException(name);
  }

  static final class Fixture implements AutoCloseable {
    final DefaultEventLoopGroup group = new DefaultEventLoopGroup(1);
    final PubServer pub;
    final PublishLane lane;
    Fixture() throws Exception { this(new PubServerConfig()); }
    Fixture(PubServerConfig config) throws Exception {
      pub = new PubServer(group, group, config, new PubServer.Listener() {});
      lane = ((PublishLane[]) field(pub, "lanes"))[0];
    }
    Sink subscribe(String prefix) throws Exception {
      Sink sink = new Sink();
      lane.eventLoop().submit(() -> {
        int slot = lane.register(sink);
        ByteBuf bytes = Unpooled.copiedBuffer(prefix, java.nio.charset.StandardCharsets.UTF_8);
        try { lane.updateSubscription(slot, Prefix.copyOf(bytes), true); }
        finally { bytes.release(); }
        lane.activate(slot);
      }).syncUninterruptibly();
      ((AtomicInteger) field(pub, "activeSubscribers")).incrementAndGet();
      return sink;
    }
    void drain() { lane.eventLoop().submit(() -> {}).syncUninterruptibly(); }
    public void close() throws Exception {
      pub.closeAsync().get(5, TimeUnit.SECONDS);
      group.shutdownGracefully(0, 5, TimeUnit.SECONDS).syncUninterruptibly();
    }
  }

  static final class Sink implements SubscriberSink {
    final List<List<String>> messages = new ArrayList<>();
    public ByteBufAllocator allocator() { return UnpooledByteBufAllocator.DEFAULT; }
    public boolean isWritable() { return true; }
    public boolean write(ByteBuf wire) {
      var decoder = new EmbeddedChannel(new Decoder(10000, false));
      try {
        decoder.writeInbound(wire);
        Decoder.Frame frame;
        List<String> message = new ArrayList<>();
        while ((frame = decoder.readInbound()) != null) {
          message.add(frame.content().toString(java.nio.charset.StandardCharsets.UTF_8));
          boolean more = frame.more();
          frame.release();
          if (!more) { messages.add(message); message = new ArrayList<>(); }
        }
        assertTrue(message.isEmpty(), "Partial message reached sink");
      } finally { decoder.finishAndReleaseAll(); }
      return true;
    }
    public void flush() { }
    public void fail(Throwable failure) { throw new AssertionError(failure); }
  }
}
