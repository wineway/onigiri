package me.wineway.onigiri.publisher;

import static org.junit.jupiter.api.Assertions.*;
import io.netty.buffer.*;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import me.wineway.onigiri.session.*;
import me.wineway.onigiri.transport.ClientChannelConfig;

@Timeout(25)
class ConnectionTest {
  record Subscription(Connection connection, byte[] prefix, boolean onControlLoop) {}

  @Test
  void subscriptionCallbackSendsOnlyToItsConnectionAndPrefixCopyCannotChangeRouting() throws Exception {
    try (Fixture f = new Fixture(); Receiver first = f.receiver(); Receiver second = f.receiver()) {
      first.client.subscribe("A"); second.client.subscribe("A");
      first.client.connect(f.pub.localAddress()).get(5, TimeUnit.SECONDS);
      Subscription a = f.events.poll(5, TimeUnit.SECONDS);
      assertNotNull(a, f.errors.toString()); assertTrue(a.onControlLoop());
      assertArrayEquals(new byte[] {65}, a.prefix());
      assertEquals(List.of("A", "init"), first.messages.poll(5, TimeUnit.SECONDS));
      second.client.connect(f.pub.localAddress()).get(5, TimeUnit.SECONDS);
      Subscription b = f.events.poll(5, TimeUnit.SECONDS);
      assertNotNull(b); assertNotSame(a.connection(), b.connection());
      assertEquals(List.of("A", "init"), second.messages.poll(5, TimeUnit.SECONDS));
      assertNull(first.messages.poll(100, TimeUnit.MILLISECONDS));
      // Callback mutated its copy to B. Both subscriptions must still match A.
      f.pub.publish(bytes("A-broadcast"));
      assertEquals(List.of("A-broadcast"), first.messages.poll(5, TimeUnit.SECONDS));
      assertEquals(List.of("A-broadcast"), second.messages.poll(5, TimeUnit.SECONDS));
      assertNotNull(a.connection().remoteAddress());
      ByteBuf payload = bytes("private");
      a.connection().send(bytes("A"), payload); // caller thread, outside control/worker loops
      assertEquals(0, payload.refCnt());
      assertEquals(List.of("A", "private"), first.messages.poll(5, TimeUnit.SECONDS));
      assertNull(second.messages.poll(100, TimeUnit.MILLISECONDS));
      assertTrue(f.errors.isEmpty(), f.errors.toString());
    }
  }

  @Test
  void disconnectedHandleCannotAddressReplacementConnection() throws Exception {
    try (Fixture f = new Fixture(); Receiver receiver = f.receiver()) {
      receiver.client.subscribe("A");
      SocketAddress address = f.pub.localAddress();
      receiver.client.connect(address).get(5, TimeUnit.SECONDS);
      Subscription old = f.events.poll(5, TimeUnit.SECONDS);
      assertNotNull(old, f.errors.toString());
      assertEquals(List.of("A", "init"), receiver.messages.poll(5, TimeUnit.SECONDS));
      receiver.client.disconnect(address).get(5, TimeUnit.SECONDS);
      assertNotNull(f.disconnected.poll(5, TimeUnit.SECONDS));
      receiver.client.connect(address).get(5, TimeUnit.SECONDS);
      Subscription current = f.events.poll(5, TimeUnit.SECONDS);
      assertNotNull(current); assertNotSame(old.connection(), current.connection());
      assertEquals(List.of("A", "init"), receiver.messages.poll(5, TimeUnit.SECONDS));
      ByteBuf stale = bytes("A-stale"); old.connection().send(stale);
      assertEquals(0, stale.refCnt());
      current.connection().send(bytes("A-current"));
      assertEquals(List.of("A-current"), receiver.messages.poll(5, TimeUnit.SECONDS));
      assertNull(receiver.messages.poll(100, TimeUnit.MILLISECONDS));
    }
  }

  @Test
  void concurrentDirectedSendersPreserveWholeMessages() throws Exception {
    try (Fixture f = new Fixture(); Receiver receiver = f.receiver()) {
      receiver.client.subscribe("A");
      receiver.client.connect(f.pub.localAddress()).get(5, TimeUnit.SECONDS);
      Subscription event = f.events.poll(5, TimeUnit.SECONDS);
      assertNotNull(event, f.errors.toString());
      assertNotNull(receiver.messages.poll(5, TimeUnit.SECONDS));
      var senders = Executors.newFixedThreadPool(4);
      try {
        List<Future<?>> tasks = new ArrayList<>();
        for (int thread = 0; thread < 4; thread++) {
          int producer = thread;
          tasks.add(senders.submit(() -> {
            for (int i = 0; i < 25; i++) event.connection().send(bytes("A"), bytes(producer + ":" + i));
          }));
        }
        for (Future<?> task : tasks) task.get(5, TimeUnit.SECONDS);
        Set<String> received = new HashSet<>();
        for (int i = 0; i < 100; i++) {
          List<String> message = receiver.messages.poll(5, TimeUnit.SECONDS);
          assertNotNull(message); assertEquals(2, message.size()); assertEquals("A", message.get(0));
          assertTrue(received.add(message.get(1)));
        }
      } finally { senders.shutdownNow(); }
    }
  }

  @Test
  void slowSubscriptionCallbackIsBoundedWithoutBlockingSubscriptionRouting() throws Exception {
    try (Fixture f = new Fixture(1); Receiver receiver = f.receiver()) {
      receiver.client.subscribe("A");
      receiver.client.connect(f.pub.localAddress()).get(5, TimeUnit.SECONDS);
      assertNotNull(f.events.poll(5, TimeUnit.SECONDS));
      assertNotNull(receiver.messages.poll(5, TimeUnit.SECONDS));
      f.boss.next().submit(() -> {}).syncUninterruptibly();
      var blocked = new CountDownLatch(1);
      var resume = new CountDownLatch(1);
      f.boss.next().execute(() -> {
        blocked.countDown();
        try { resume.await(); }
        catch (InterruptedException failure) { Thread.currentThread().interrupt(); }
      });
      try {
        assertTrue(blocked.await(5, TimeUnit.SECONDS));
        receiver.client.subscribe("B"); receiver.client.subscribe("C"); receiver.client.subscribe("D");
        List<String> delivered = null;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (delivered == null && System.nanoTime() < deadline) {
          f.pub.publish(bytes("D"));
          delivered = receiver.messages.poll(20, TimeUnit.MILLISECONDS);
        }
        assertEquals(List.of("D"), delivered, "New routing must not wait for control callbacks");
        var pending = (java.util.concurrent.atomic.AtomicInteger) MultipartWriterTest.field(f.pub, "pendingSubscriptions");
        assertEquals(1, pending.get());
      } finally { resume.countDown(); }
      Subscription next = f.events.poll(5, TimeUnit.SECONDS);
      assertNotNull(next); assertArrayEquals(new byte[] {'B'}, next.prefix());
      f.boss.next().submit(() -> {}).syncUninterruptibly();
      assertNull(f.events.poll(100, TimeUnit.MILLISECONDS));
      receiver.client.subscribe("E");
      Subscription recovered = f.events.poll(5, TimeUnit.SECONDS);
      assertNotNull(recovered); assertArrayEquals(new byte[] {'E'}, recovered.prefix());
    }
  }

  private static ByteBuf bytes(String value) { return Unpooled.copiedBuffer(value, StandardCharsets.UTF_8); }

  static final class Receiver implements AutoCloseable {
    final LinkedBlockingQueue<List<String>> messages = new LinkedBlockingQueue<>();
    final SubClient client;
    Receiver(Fixture f) {
      client = new SubClient(f.clients, new ClientChannelConfig(), new SubClientConfig(2000, 0, 0, 0),
          new SubClient.MultipartListener() {
            public void onMultipart(SocketAddress publisher, MultipartView message) {
              List<String> parts = new ArrayList<>();
              for (int i = 0; i < message.partCount(); i++) parts.add(message.buffer(i).toString(
                  message.index(i), message.length(i), StandardCharsets.UTF_8));
              messages.add(parts);
            }
            public void onError(Throwable failure) { f.errors.add(failure); }
          });
    }
    public void close() throws Exception { client.closeAsync().get(5, TimeUnit.SECONDS); }
  }

  static final class Fixture implements AutoCloseable {
    final MultiThreadIoEventLoopGroup boss = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    final MultiThreadIoEventLoopGroup workers = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    final MultiThreadIoEventLoopGroup clients = new MultiThreadIoEventLoopGroup(2, NioIoHandler.newFactory());
    final LinkedBlockingQueue<Subscription> events = new LinkedBlockingQueue<>();
    final LinkedBlockingQueue<Throwable> errors = new LinkedBlockingQueue<>();
    final LinkedBlockingQueue<SocketAddress> disconnected = new LinkedBlockingQueue<>();
    final PubServer pub;
    Fixture() throws Exception { this(1024); }
    Fixture(int callbacks) throws Exception {
      pub = new PubServer(boss, workers, new PubServerConfig(1024, 64, 256, 16, 2000, 0, 0, 0,
          1024, 4096, 64, 16 * 1024 * 1024, 32 * 1024 * 1024, callbacks),
          new PubServer.Listener() {
            public void onSubscription(Connection connection, byte[] prefix) {
              events.add(new Subscription(connection, prefix.clone(), boss.next().inEventLoop()));
              connection.send(Unpooled.wrappedBuffer(prefix), bytes("init"));
              Arrays.fill(prefix, (byte) 'B');
            }
            public void onError(SocketAddress address, Throwable failure) { errors.add(failure); }
            public void onDisconnected(SocketAddress address) { disconnected.add(address); }
          });
      pub.bind(new InetSocketAddress("127.0.0.1", 0)).get(5, TimeUnit.SECONDS);
    }
    Receiver receiver() { return new Receiver(this); }
    public void close() throws Exception {
      pub.closeAsync().get(5, TimeUnit.SECONDS);
      clients.shutdownGracefully(0, 5, TimeUnit.SECONDS).sync();
      workers.shutdownGracefully(0, 5, TimeUnit.SECONDS).sync();
      boss.shutdownGracefully(0, 5, TimeUnit.SECONDS).sync();
    }
  }
}
