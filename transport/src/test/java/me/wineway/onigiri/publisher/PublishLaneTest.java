package me.wineway.onigiri.publisher;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoop;

class PublishLaneTest {
  @Test
  void clearsSparseRoutesBetweenFilteredAndSharedBatches() throws Exception {
    var group = new DefaultEventLoopGroup(1);
    EventLoop eventLoop = (EventLoop) group.next();
    var dropped = new LongAdder();
    var lane = new PublishLane(eventLoop,
        new PubServerConfig(16, 16, 2, 256, 0, 0, 0, 0), dropped);
    var sinks = new CountingSink[130];
    ByteBuf aBytes = Unpooled.wrappedBuffer(new byte[] {0x41});
    ByteBuf bBytes = Unpooled.wrappedBuffer(new byte[] {0x42});
    Prefix a = Prefix.copyOf(aBytes);
    Prefix b = Prefix.copyOf(bBytes);
    aBytes.release();
    bBytes.release();
    try {
      eventLoop.submit(() -> {
        for (int i = 0; i < sinks.length; i++) {
          sinks[i] = new CountingSink(new CountDownLatch(0));
          lane.activate(lane.register(sinks[i]));
        }
        lane.updateSubscription(0, a, true);
        lane.updateSubscription(129, b, true);
      }).syncUninterruptibly();
      submitAndDrain(lane, 0x41, 0x42);
      eventLoop.submit(() -> {
        lane.updateSubscription(129, b, false);
        lane.updateSubscription(129, a, true);
      }).syncUninterruptibly();
      submitAndDrain(lane, 0x42, 0x41);
      submitAndDrain(lane, 0x41, 0x41);
      submitAndDrain(lane, 0x42, 0x42);
      assertEquals(List.of(0x41, 0x41, 0x41, 0x41), sinks[0].bodies);
      assertEquals(List.of(0x42, 0x41, 0x41, 0x41), sinks[129].bodies);
      for (int i = 1; i < 129; i++) assertTrue(sinks[i].bodies.isEmpty());
      assertEquals(0, dropped.sum());
    } finally {
      group.shutdownGracefully(0, 5, TimeUnit.SECONDS).syncUninterruptibly();
    }
  }

  @Test
  void fullQueueAndCloseReleaseEveryBatchWithoutStoppingCallerEventLoop() throws Exception {
    var group = new DefaultEventLoopGroup(1);
    var loop = (EventLoop) group.next();
    var dropped = new LongAdder();
    var lane = new PublishLane(loop, new PubServerConfig(2, 1, 1, 0, 0, 0, 0), dropped);
    var blocked = new CountDownLatch(1);
    var resume = new CountDownLatch(1);
    ByteBuf[] buffers = new ByteBuf[3];
    try {
      loop.execute(() -> {
        blocked.countDown();
        try { resume.await(); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); }
      });
      assertTrue(blocked.await(5, TimeUnit.SECONDS));
      for (int i = 0; i < buffers.length; i++) {
        buffers[i] = Unpooled.directBuffer(202).writeZero(202);
        lane.submit(new EncodedBatch(buffers[i], 1, new int[] {0, 202}, new int[] {2}, new int[] {200}));
      }
      assertEquals(0, buffers[2].refCnt());
      assertEquals(1, lane.fullQueueDrops());
      var close = lane.closeAsync();
      assertTrue(!close.isDone());
      resume.countDown();
      close.get(5, TimeUnit.SECONDS);
      lane.closeAsync().get(5, TimeUnit.SECONDS);
      for (ByteBuf buffer : buffers) assertEquals(0, buffer.refCnt());
      assertEquals(2, lane.stoppedDrops());
      assertEquals(3, dropped.sum());
      assertEquals(42, loop.submit(() -> 42).get(5, TimeUnit.SECONDS));
    } finally {
      resume.countDown();
      group.shutdownGracefully(0, 5, TimeUnit.SECONDS).syncUninterruptibly();
    }
  }

  @Test
  void unwritableSubscriberDoesNotBlockOthersAndReusedSlotHasNoOldPrefixes() {
    var group = new DefaultEventLoopGroup(1);
    var loop = (EventLoop) group.next();
    var lane = new PublishLane(loop, new PubServerConfig(16, 16, 2, 0, 0, 0, 0), new LongAdder());
    var first = new CountingSink(new CountDownLatch(0));
    var second = new CountingSink(new CountDownLatch(0));
    var replacement = new CountingSink(new CountDownLatch(0));
    Prefix all = Prefix.copyOf(Unpooled.EMPTY_BUFFER);
    try {
      loop.submit(() -> {
        lane.activate(lane.register(first));
        lane.activate(lane.register(second));
        lane.updateSubscription(0, all, true);
        lane.updateSubscription(1, all, true);
        lane.updateWritable(0, false);
      }).syncUninterruptibly();
      submitAndDrain(lane, 65, 66);
      assertTrue(first.bodies.isEmpty());
      assertEquals(List.of(65, 66), second.bodies);
      loop.submit(() -> {
        lane.unregister(0, List.of(all), true);
        assertEquals(0, lane.register(replacement));
        lane.activate(0);
      }).syncUninterruptibly();
      submitAndDrain(lane, 67, 68);
      assertTrue(replacement.bodies.isEmpty());
      assertEquals(List.of(65, 66, 67, 68), second.bodies);
    } finally {
      group.shutdownGracefully(0, 5, TimeUnit.SECONDS).syncUninterruptibly();
    }
  }

  @Test
  void concurrentProducersRacingCloseReleaseAllReferences() throws Exception {
    var group = new DefaultEventLoopGroup(1);
    var lane = new PublishLane((EventLoop) group.next(),
        new PubServerConfig(2, 1, 1, 0, 0, 0, 0), new LongAdder());
    var callers = java.util.concurrent.Executors.newFixedThreadPool(4);
    var start = new CountDownLatch(1);
    ByteBuf[][] buffers = new ByteBuf[4][1000];
    var tasks = new ArrayList<java.util.concurrent.Future<?>>();
    try {
      for (int thread = 0; thread < 4; thread++) {
        final int index = thread;
        tasks.add(callers.submit(() -> {
          start.await();
          for (int i = 0; i < buffers[index].length; i++) {
            ByteBuf wire = Unpooled.directBuffer(202).writeZero(202);
            buffers[index][i] = wire;
            lane.submit(new EncodedBatch(wire, 1, new int[] {0, 202}, new int[] {2}, new int[] {200}));
          }
          return null;
        }));
      }
      start.countDown();
      lane.closeAsync().get(5, TimeUnit.SECONDS);
      for (var task : tasks) task.get(5, TimeUnit.SECONDS);
      for (ByteBuf[] row : buffers) for (ByteBuf wire : row) assertEquals(0, wire.refCnt());
    } finally {
      start.countDown();
      callers.shutdownNow();
      group.shutdownGracefully(0, 5, TimeUnit.SECONDS).syncUninterruptibly();
    }
  }

  @Test
  void rejectedDrainIsReleasedWhenExecutorTerminates() throws Exception {
    var group = new DefaultEventLoopGroup(1);
    EventLoop delegate = (EventLoop) group.next();
    var loop = (EventLoop) java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),
        new Class<?>[] {EventLoop.class}, (proxy, method, args) -> {
          if (method.getName().equals("execute")) throw new java.util.concurrent.RejectedExecutionException();
          try { return method.invoke(delegate, args); }
          catch (java.lang.reflect.InvocationTargetException failure) { throw failure.getCause(); }
        });
    var lane = new PublishLane(loop, new PubServerConfig(2, 1, 1, 0, 0, 0, 0), new LongAdder());
    ByteBuf wire = Unpooled.directBuffer(202).writeZero(202);
    lane.submit(new EncodedBatch(wire, 1, new int[] {0, 202}, new int[] {2}, new int[] {200}));
    var closing = lane.closeAsync();
    group.shutdownGracefully(0, 5, TimeUnit.SECONDS).syncUninterruptibly();
    closing.get(5, TimeUnit.SECONDS);
    assertEquals(0, wire.refCnt());
    assertEquals(1, lane.stoppedDrops());
  }

  private static void submitAndDrain(PublishLane lane, int first, int second) {
    ByteBuf wire = Unpooled.directBuffer(404).writeZero(404);
    wire.setByte(2, first);
    wire.setByte(204, second);
    lane.submit(new EncodedBatch(wire, 2, new int[] {0, 202, 404}, new int[] {2, 204}, new int[] {200, 200}));
    lane.eventLoop().submit(() -> {}).syncUninterruptibly();
    assertEquals(0, wire.refCnt());
  }

  @Test
  void routesPastMultipleSixtyFourSubscriberBoundaries() throws Exception {
    int subscriberCount = 130;
    var group = new DefaultEventLoopGroup(1);
    var dropped = new LongAdder();
    EventLoop eventLoop = (EventLoop) group.next();
    var lane = new PublishLane(eventLoop,
        new PubServerConfig(16, 16, 1, 256, 0, 0, 0, 0), dropped);
    var delivered = new CountDownLatch(subscriberCount);
    Prefix all = Prefix.copyOf(Unpooled.EMPTY_BUFFER);
    try {
      eventLoop.submit(() -> {
        for (int i = 0; i < subscriberCount; i++) {
          int slot = lane.register(new CountingSink(delivered));
          lane.updateSubscription(slot, all, true);
          lane.activate(slot);
        }
      }).syncUninterruptibly();

      ByteBuf wire = Unpooled.directBuffer(202).writeZero(202);
      lane.submit(new EncodedBatch(wire, 1, new int[] {0, 202}, new int[] {2}, new int[] {200}));

      assertTrue(delivered.await(5, TimeUnit.SECONDS));
      eventLoop.submit(() -> {}).syncUninterruptibly();
      assertEquals(0, wire.refCnt());
      assertEquals(0, dropped.sum());
    } finally {
      group.shutdownGracefully(0, 5, TimeUnit.SECONDS).syncUninterruptibly();
    }
  }

  @Test
  void submissionAfterEventLoopTerminationIsDroppedAndReleased() {
    var group = new DefaultEventLoopGroup(1);
    var dropped = new LongAdder();
    EventLoop eventLoop = (EventLoop) group.next();
    var lane = new PublishLane(eventLoop,
        new PubServerConfig(2, 1, 1, 0, 0, 0, 0), dropped);
    group.shutdownGracefully(0, 5, TimeUnit.SECONDS).syncUninterruptibly();

    ByteBuf wire = Unpooled.directBuffer(202).writeZero(202);
    var batch = new EncodedBatch(wire, 1, new int[] {0, 202}, new int[] {2}, new int[] {200});
    lane.submit(batch);

    assertEquals(0, wire.refCnt());
    assertEquals(1, dropped.sum());
  }

  @Test
  void directedQueueOverflowAndShutdownReleaseAllWireReferences() throws Exception {
    var group = new DefaultEventLoopGroup(1);
    EventLoop loop = group.next();
    var lane = new PublishLane(loop, new PubServerConfig(2, 1, 1, 0, 0, 0, 0), new LongAdder());
    var sink = new CountingSink(new CountDownLatch(0));
    var blocked = new CountDownLatch(1);
    var resume = new CountDownLatch(1);
    ByteBuf[] wires = new ByteBuf[3];
    loop.execute(() -> {
      blocked.countDown();
      try { resume.await(); }
      catch (InterruptedException failure) { Thread.currentThread().interrupt(); }
    });
    try {
      assertTrue(blocked.await(5, TimeUnit.SECONDS));
      for (int i = 0; i < wires.length; i++) {
        wires[i] = Unpooled.directBuffer(202).writeZero(202);
        lane.submit(new EncodedBatch(wires[i], sink));
      }
      assertEquals(0, wires[2].refCnt());
      assertEquals(1, lane.fullQueueDrops());
      var closing = lane.closeAsync();
      resume.countDown();
      closing.get(5, TimeUnit.SECONDS);
      for (ByteBuf wire : wires) assertEquals(0, wire.refCnt());
      assertTrue(sink.bodies.isEmpty());
    } finally {
      resume.countDown();
      group.shutdownGracefully(0, 5, TimeUnit.SECONDS).syncUninterruptibly();
    }
    ByteBuf late = Unpooled.directBuffer(202).writeZero(202);
    lane.submit(new EncodedBatch(late, sink));
    assertEquals(0, late.refCnt());
  }

  private static final class CountingSink implements SubscriberSink {
    private final CountDownLatch delivered;
    private final List<Integer> bodies = new ArrayList<>();

    private CountingSink(CountDownLatch delivered) {
      this.delivered = delivered;
    }

    @Override
    public ByteBufAllocator allocator() {
      return UnpooledByteBufAllocator.DEFAULT;
    }

    @Override
    public boolean isWritable() {
      return true;
    }

    @Override
    public boolean write(ByteBuf wire) {
      for (int i = wire.readerIndex() + 2; i < wire.writerIndex(); i += 202) {
        bodies.add(wire.getUnsignedByte(i) & 0xff);
      }
      wire.release();
      delivered.countDown();
      return true;
    }

    @Override
    public void flush() {}

    @Override
    public void fail(Throwable failure) {
      throw new AssertionError(failure);
    }
  }
}
