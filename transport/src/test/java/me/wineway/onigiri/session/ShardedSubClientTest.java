package me.wineway.onigiri.session;

import static org.junit.jupiter.api.Assertions.*;
import java.io.*;
import java.net.*;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import io.netty.buffer.ByteBuf;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import me.wineway.onigiri.transport.ClientChannelConfig;

class ShardedSubClientTest {
  @Test @Timeout(10)
  void validatesShardCountAndClosesWhenCallerStopsGroup() throws Exception {
    var group = new MultiThreadIoEventLoopGroup(2, NioIoHandler.newFactory());
    try {
      assertThrows(IllegalArgumentException.class, () -> new ShardedSubClient(group, 3,
          new ClientChannelConfig(), new SubClientConfig(), new SubClient.Listener() {}));
      var client = new ShardedSubClient(group, 2, new ClientChannelConfig(), new SubClientConfig(), new SubClient.Listener() {});
      client.subscribe("A");
      group.shutdownGracefully(0, 5, TimeUnit.SECONDS).sync();
      client.closeAsync().get(5, TimeUnit.SECONDS);
      assertFalse(client.isReady());
    } finally { group.shutdownGracefully(0, 5, TimeUnit.SECONDS).sync(); }
  }

  @ParameterizedTest @ValueSource(booleans = {false, true}) @Timeout(40)
  @EnabledIfSystemProperty(named = "onigiri.zmq", matches = "true")
  void nativePublishersRestoreOrderedSubscriptionsAcrossShardsAndCloseRaces(boolean borrowed) throws Exception {
    var group = new MultiThreadIoEventLoopGroup(2, NioIoHandler.newFactory());
    var queues = new ConcurrentHashMap<SocketAddress, LinkedBlockingQueue<byte[]>>();
    var threads = ConcurrentHashMap.<Long>newKeySet();
    var errors = new LinkedBlockingQueue<Throwable>();
    class Receiver implements SubClient.Listener {
      void consume(SocketAddress publisher, ByteBuf data, int index, int length, boolean more) {
        assertFalse(more);
        threads.add(Thread.currentThread().getId());
        byte[] bytes = new byte[length]; data.getBytes(index, bytes);
        queues.get(publisher).add(bytes);
      }
      @Override public void onFrame(SocketAddress publisher, me.wineway.onigiri.protocol.Decoder.Frame frame) {
        consume(publisher, frame.content(), frame.content().readerIndex(), frame.content().readableBytes(), frame.more());
      }
      @Override public void onError(Throwable failure) { errors.add(failure); }
    }
    class BorrowedReceiver extends Receiver implements SubClient.MessageListener {
      @Override public void onMessage(SocketAddress publisher, ByteBuf data, int index, int length, boolean more) {
        consume(publisher, data, index, length, more);
      }
    }
    var client = new ShardedSubClient(group, 2, new ClientChannelConfig(20, 200, 1024, 8, 1000),
        new SubClientConfig(2000, 100, 2000, 2000), borrowed ? new BorrowedReceiver() : new Receiver());
    var callers = Executors.newFixedThreadPool(2);
    try (var first = new Peer(); var second = new Peer()) {
      var peers = List.of(first, second);
      byte[] prefix = {65}; client.subscribe(prefix); prefix[0] = 66;
      for (Peer peer : peers) {
        queues.put(peer.address, new LinkedBlockingQueue<>());
        client.connect(peer.address).get(5, TimeUnit.SECONDS);
        sync(peer, queues.get(peer.address), new byte[] {65, 1});
      }
      assertEquals(2, threads.size(), "Data should execute on two independent EventLoops");
      assertThrows(ExecutionException.class, () -> client.connect(first.address).get(5, TimeUnit.SECONDS));
      // Multiple callers update distinct prefixes; a later broadcast establishes final intent.
      var updates = new ArrayList<Future<?>>();
      for (int i = 0; i < 2; i++) {
        int tag = 70 + i;
        updates.add(callers.submit(() -> {
          for (int j = 0; j < 20; j++) { client.subscribe(new byte[] {(byte) tag}); client.unsubscribe(new byte[] {(byte) tag}); }
        }));
      }
      for (var update : updates) update.get(5, TimeUnit.SECONDS);
      var initialDisconnect = client.disconnect(first.address);
      client.unsubscribe("A"); client.subscribe("Z");
      // These updates surround connection/READY restoration on a different shard loop.
      var reconnect = client.connect(first.address);
      client.subscribe("temporary"); client.unsubscribe("temporary");
      reconnect.get(5, TimeUnit.SECONDS);
      initialDisconnect.get(5, TimeUnit.SECONDS);
      for (Peer peer : peers) {
        sync(peer, queues.get(peer.address), new byte[] {90, 2});
        peer.send(new byte[] {65, 3}); peer.send(new byte[] {70, 3}); peer.send(new byte[] {71, 3});
        peer.send(new byte[] {90, 3});
        assertArrayEquals(new byte[] {90, 3}, queues.get(peer.address).poll(3, TimeUnit.SECONDS));
        assertNull(queues.get(peer.address).poll(100, TimeUnit.MILLISECONDS));
      }
      // One session is closing while another subscription update is broadcast.
      var disconnected = client.disconnect(first.address);
      client.subscribe("Q");
      disconnected.get(5, TimeUnit.SECONDS);
      sync(second, queues.get(second.address), new byte[] {81, 1});
      var reconnecting = client.connect(first.address);
      var closed = client.closeAsync();
      reconnecting.handle((ignored, failure) -> null).get(5, TimeUnit.SECONDS);
      closed.get(5, TimeUnit.SECONDS);
      client.closeAsync().get(5, TimeUnit.SECONDS);
      assertFalse(client.isReady());
      assertFalse(group.isShuttingDown());
      assertTrue(errors.isEmpty(), errors.toString());
    } finally {
      callers.shutdownNow();
      try { client.closeAsync().get(5, TimeUnit.SECONDS); }
      finally { group.shutdownGracefully(0, 5, TimeUnit.SECONDS).sync(); }
    }
  }

  static void sync(Peer peer, LinkedBlockingQueue<byte[]> received, byte[] marker) throws Exception {
    long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (System.nanoTime() < end) {
      peer.send(marker);
      byte[] next = received.poll(50, TimeUnit.MILLISECONDS);
      if (next != null) { assertArrayEquals(marker, next); return; }
    }
    fail("Subscription did not reach native peer");
  }

  static final class Peer implements AutoCloseable {
    final Process process;
    final PrintWriter input;
    final BufferedReader output;
    final InetSocketAddress address;
    Peer() throws Exception {
      process = new ProcessBuilder(System.getProperty("onigiri.python", "python3"), "-u",
          Path.of("src/test/python/subscriber_peer.py").toAbsolutePath().toString())
          .redirectError(ProcessBuilder.Redirect.INHERIT).start();
      input = new PrintWriter(process.getOutputStream(), true);
      output = new BufferedReader(new InputStreamReader(process.getInputStream()));
      String hello = output.readLine(); assertNotNull(hello);
      address = new InetSocketAddress("127.0.0.1", Integer.parseInt(hello.split(" ")[1]));
    }
    void send(byte[] data) throws Exception {
      input.println("SEND " + HexFormat.of().formatHex(data)); assertEquals("SENT", output.readLine());
    }
    @Override public void close() throws Exception {
      input.println("QUIT");
      if (!process.waitFor(3, TimeUnit.SECONDS)) { process.destroyForcibly(); process.waitFor(3, TimeUnit.SECONDS); }
      input.close(); output.close();
    }
  }
}
