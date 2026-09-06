package me.wineway.onigiri.publisher;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.util.concurrent.SingleThreadEventExecutor;
import me.wineway.onigiri.protocol.Decoder;
import me.wineway.onigiri.session.SubClient;
import me.wineway.onigiri.session.SubClientConfig;
import me.wineway.onigiri.transport.ClientChannelConfig;

@Timeout(20)
class PubServerTest {
  @Test
  void validatesMessagesIndependentlyOfSubscribersAndCloseIsIdempotent() throws Exception {
    var group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    var publisher = new PubServer(group, group,
        new PubServerConfig(2, 1, 1, 16, 0, 0, 0, 0, 16, 16, 2, 200, 800), new PubServer.Listener() {});
    try {
      ByteBuf tooLarge = Unpooled.buffer(201).writeZero(201);
      assertThrows(IllegalArgumentException.class, () -> publisher.publish(tooLarge));
      assertEquals(0, tooLarge.refCnt());
      ByteBuf changed = Unpooled.buffer(201).writeZero(201);
      changed.readerIndex(1);
      publisher.publish(changed); // current readable region defines the message
      assertEquals(0, changed.refCnt());
      ByteBuf empty = Unpooled.buffer(1);
      assertThrows(IllegalArgumentException.class, () -> publisher.publish(empty));
      assertEquals(0, empty.refCnt());
      ByteBuf noSubscribers = Unpooled.buffer(200).writeZero(200);
      publisher.publish(noSubscribers);
      assertEquals(0, noSubscribers.refCnt());
      assertEquals(2, publisher.dropCounts().noSubscribers());
      assertEquals(2, publisher.dropCounts().invalidBatches());
      publisher.closeAsync().get(5, TimeUnit.SECONDS);
      publisher.closeAsync().get(5, TimeUnit.SECONDS);
      assertFalse(publisher.isBound());
      assertThrows(java.util.concurrent.ExecutionException.class,
          () -> publisher.bind(new InetSocketAddress("127.0.0.1", 0)).get(5, TimeUnit.SECONDS));
      ByteBuf closed = Unpooled.buffer(200).writeZero(200);
      publisher.publish(closed);
      assertEquals(0, closed.refCnt());
      assertEquals(1, publisher.dropCounts().closedPublisher());
      assertFalse(group.isShuttingDown());
    } finally {
      publisher.closeAsync().get(5, TimeUnit.SECONDS);
      group.shutdownGracefully(0, 5, TimeUnit.SECONDS).sync();
    }
  }

  @Test
  void failedBindCanCloseAndSecondBindIsRejected() throws Exception {
    var group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    var publisher = new PubServer(group);
    try (var occupied = new java.net.ServerSocket(0, 1, java.net.InetAddress.getLoopbackAddress())) {
      assertThrows(java.util.concurrent.ExecutionException.class,
          () -> publisher.bind(occupied.getLocalSocketAddress()).get(5, TimeUnit.SECONDS));
      assertThrows(java.util.concurrent.ExecutionException.class,
          () -> publisher.bind(new InetSocketAddress("127.0.0.1", 0)).get(5, TimeUnit.SECONDS));
      publisher.closeAsync().get(5, TimeUnit.SECONDS);
      assertFalse(publisher.isBound());
    } finally {
      publisher.closeAsync().get(5, TimeUnit.SECONDS);
      group.shutdownGracefully(0, 5, TimeUnit.SECONDS).sync();
    }
  }

  @Test
  void closesAcceptedConnectionWhoseRegistrationFinishesAfterClose() throws Exception {
    var boss = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    var workers = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    var publisher = new PubServer(boss, workers,
        new PubServerConfig(16, 16, 256, 0, 0, 0, 0), new PubServer.Listener() {});
    var blocked = new CountDownLatch(1);
    var resume = new CountDownLatch(1);
    try (var socket = new Socket()) {
      publisher.bind(new InetSocketAddress("127.0.0.1", 0)).get(5, TimeUnit.SECONDS);
      var worker = (SingleThreadEventExecutor) workers.next();
      worker.execute(() -> {
        blocked.countDown();
        try {
          if (!resume.await(10, TimeUnit.SECONDS)) throw new AssertionError("Worker not resumed");
        } catch (InterruptedException failure) {
          Thread.currentThread().interrupt();
          throw new AssertionError(failure);
        }
      });
      assertTrue(blocked.await(5, TimeUnit.SECONDS));
      socket.connect(publisher.localAddress(), 5000);
      socket.setSoTimeout(5000);
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (worker.pendingTasks() == 0 && System.nanoTime() < deadline) Thread.onSpinWait();
      assertTrue(worker.pendingTasks() > 0, "Accepted channel must be awaiting registration");

      var closing = publisher.closeAsync();
      assertTrue(!closing.isDone(), "Close must wait for worker cleanup");
      resume.countDown();
      closing.get(5, TimeUnit.SECONDS);
      assertEquals(-1, socket.getInputStream().read(), "Late connection must close without a greeting");
    } finally {
      resume.countDown();
      publisher.closeAsync().get(5, TimeUnit.SECONDS);
      workers.shutdownGracefully(0, 5, TimeUnit.SECONDS).sync();
      boss.shutdownGracefully(0, 5, TimeUnit.SECONDS).sync();
    }
  }

  @Test
  void handshakesWithSubClientFiltersBatchAndAlwaysTakesOwnership() throws Exception {
    var serverGroup = new MultiThreadIoEventLoopGroup(2, NioIoHandler.newFactory());
    var clientGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    var publisherReady = new LinkedBlockingQueue<SocketAddress>();
    var received = new LinkedBlockingQueue<byte[]>();
    PubServer publisher = new PubServer(serverGroup, serverGroup,
        new PubServerConfig(64, 16, 256, 2000, 0, 0, 0), new PubServer.Listener() {
          @Override
          public void onReady(SocketAddress subscriber) { publisherReady.add(subscriber); }
        });
    SubClient subscriber = new SubClient(clientGroup,
        new ClientChannelConfig(20, 200, 1024, 8, 1000), new SubClientConfig(2000, 0, 0, 0),
        new SubClient.Listener() {
          @Override
          public void onFrame(Decoder.Frame frame) {
            byte[] copy = new byte[frame.content().readableBytes()];
            frame.content().getBytes(frame.content().readerIndex(), copy);
            received.add(copy);
          }
        });
    try {
      publisher.bind(new InetSocketAddress("127.0.0.1", 0)).get(5, TimeUnit.SECONDS);
      assertTrue(publisher.isBound());
      assertNotNull(publisher.localAddress());

      publishMessages(publisher);

      subscriber.subscribe(new byte[] {0x41});
      subscriber.connect(publisher.localAddress()).get(5, TimeUnit.SECONDS);
      assertNotNull(publisherReady.poll(5, TimeUnit.SECONDS));

      byte[] first = null;
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (first == null && System.nanoTime() < deadline) {
        publishMessages(publisher);
        first = received.poll(20, TimeUnit.MILLISECONDS);
      }
      assertNotNull(first);
      assertEquals(128, first.length);
      assertEquals(0x41, first[0]);
      byte[] extra;
      while ((extra = received.poll(100, TimeUnit.MILLISECONDS)) != null) {
        assertEquals(0x41, extra[0]);
      }

      subscriber.closeAsync().get(5, TimeUnit.SECONDS);
      publisher.closeAsync().get(5, TimeUnit.SECONDS);
      ByteBuf afterClose = Unpooled.buffer(200).writeZero(200);
      publisher.publish(afterClose);
      assertEquals(0, afterClose.refCnt());
    } finally {
      subscriber.closeAsync().get(5, TimeUnit.SECONDS);
      publisher.closeAsync().get(5, TimeUnit.SECONDS);
      clientGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).sync();
      serverGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).sync();
    }
  }

  @Test
  void routesOneEncodedBatchAcrossSubscriberLanes() throws Exception {
    var serverGroup = new MultiThreadIoEventLoopGroup(2, NioIoHandler.newFactory());
    var clientGroup = new MultiThreadIoEventLoopGroup(2, NioIoHandler.newFactory());
    var firstMessages = new LinkedBlockingQueue<Integer>();
    var secondMessages = new LinkedBlockingQueue<Integer>();
    PubServer publisher = new PubServer(serverGroup, serverGroup,
        new PubServerConfig(64, 16, 256, 2000, 0, 0, 0), new PubServer.Listener() {});
    SubClient first = subscriber(clientGroup, 0x41, firstMessages);
    SubClient second = subscriber(clientGroup, 0x42, secondMessages);
    try {
      publisher.bind(new InetSocketAddress("127.0.0.1", 0)).get(5, TimeUnit.SECONDS);
      first.connect(publisher.localAddress()).get(5, TimeUnit.SECONDS);
      second.connect(publisher.localAddress()).get(5, TimeUnit.SECONDS);

      Integer firstValue = null;
      Integer secondValue = null;
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while ((firstValue == null || secondValue == null) && System.nanoTime() < deadline) {
        publishMessages(publisher);
        if (firstValue == null) firstValue = firstMessages.poll(20, TimeUnit.MILLISECONDS);
        if (secondValue == null) secondValue = secondMessages.poll(20, TimeUnit.MILLISECONDS);
      }
      assertEquals(0x41, firstValue);
      assertEquals(0x42, secondValue);
      Integer value;
      while ((value = firstMessages.poll()) != null) assertEquals(0x41, value);
      while ((value = secondMessages.poll()) != null) assertEquals(0x42, value);
    } finally {
      first.closeAsync().get(5, TimeUnit.SECONDS);
      second.closeAsync().get(5, TimeUnit.SECONDS);
      publisher.closeAsync().get(5, TimeUnit.SECONDS);
      clientGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).sync();
      serverGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).sync();
    }
  }

  private static SubClient subscriber(MultiThreadIoEventLoopGroup group, int prefix,
      LinkedBlockingQueue<Integer> messages) {
    SubClient client = new SubClient(group,
        new ClientChannelConfig(20, 200, 1024, 8, 1000), new SubClientConfig(2000, 0, 0, 0),
        new SubClient.Listener() {
          @Override
          public void onFrame(Decoder.Frame frame) {
            messages.add((int) frame.content().getUnsignedByte(frame.content().readerIndex()));
          }
        });
    client.subscribe(new byte[] {(byte) prefix});
    return client;
  }

  private static void publishMessages(PubServer publisher) {
    ByteBuf first = Unpooled.directBuffer(128).writeByte(0x41).writeZero(127);
    ByteBuf second = Unpooled.directBuffer(256).writeByte(0x42).writeZero(255);
    try (var writer = publisher.newBatchWriter()) {
      writer.add(first);
      writer.add(second);
      writer.flush();
    }
    assertEquals(0, first.refCnt());
    assertEquals(0, second.refCnt());
  }
}
