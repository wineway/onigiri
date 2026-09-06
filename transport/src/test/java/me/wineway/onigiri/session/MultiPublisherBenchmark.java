package me.wineway.onigiri.session;

import static org.junit.jupiter.api.Assertions.*;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.net.*;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import com.sun.management.ThreadMXBean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioIoHandler;
import me.wineway.onigiri.protocol.Decoder;
import me.wineway.onigiri.transport.ClientChannelConfig;

/** Real independent native libzmq PUB processes, one SubClient, one serialized EventLoop. */
class MultiPublisherBenchmark {
  @Test @Timeout(90)
  void independentPublishers() throws Exception {
    int publishers = Integer.getInteger("onigiri.publishers", 4);
    assertTrue(publishers > 0 && publishers <= 16);
    int shardCount = Integer.getInteger("onigiri.shards", 1);
    var group = new MultiThreadIoEventLoopGroup(shardCount, NioIoHandler.newFactory());
    EventLoop loop = group.next();
    var receiver = Boolean.getBoolean("onigiri.borrowed") ? new BorrowedReceiver(publishers) : new Receiver(publishers);
    var client = new Client(group, shardCount, receiver);
    var peers = new ArrayList<Peer>();
    try {
      client.subscribe(new byte[0]);
      for (int i = 0; i < publishers; i++) {
        var peer = new Peer(); peers.add(peer);
        int index = i;
        loop.submit(() -> receiver.indexes.put(peer.address, index)).get(5, TimeUnit.SECONDS);
        client.connect(peer.address).get(5, TimeUnit.SECONDS);
      }
      assertTrue(receiver.ready.await(5, TimeUnit.SECONDS));
      markers(peers, receiver, 0);
      for (SubClient shard : client.shards()) {
        EventLoop ownerLoop = (EventLoop) field(shard, "eventLoop");
        ownerLoop.submit(() -> {
          Map<?, ?> sessions = (Map<?, ?>) field(shard, "sessions");
          for (var entry : sessions.entrySet()) {
            Channel channel = (Channel) field(field(entry.getValue(), "transport"), "channel");
            int index = receiver.indexes.get(entry.getKey());
            channel.pipeline().addFirst(new ChannelInboundHandlerAdapter() {
              @Override public void channelRead(ChannelHandlerContext ctx, Object message) {
                receiver.arrival[index * 16] = System.nanoTime();
                ctx.fireChannelRead(message);
              }
            });
          }
          return null;
        }).get(5, TimeUnit.SECONDS);
      }
      long[] threads = new long[shardCount];
      int threadIndex = 0;
      for (var executor : group) threads[threadIndex++] = executor.submit(() -> Thread.currentThread().getId()).get();
      System.out.printf("Topology: %d native PUB processes -> loopback -> %d SUB shards / EventLoops; JVM=%s%n",
          publishers, shardCount, System.getProperty("java.version"));
      System.out.printf("borrowed=%s, offered rate/publisher=%d msg/s (0=unlimited)%n", Boolean.getBoolean("onigiri.borrowed"), Long.getLong("onigiri.rate", 0L));
      int phase = 1;
      for (String distribution : new String[] {"fixed", "mixed"}) {
        measure(threads, loop, peers, receiver, phase++, 2, distribution, false);
        measure(threads, loop, peers, receiver, phase++, 3, distribution, true);
      }
      assertNull(receiver.failure);
    } finally {
      try { client.closeAsync().get(5, TimeUnit.SECONDS); }
      finally {
        for (Peer peer : peers) peer.close();
        group.shutdownGracefully(0, 5, TimeUnit.SECONDS).sync();
      }
    }
  }

  static void measure(long[] threads, EventLoop loop, List<Peer> peers, Receiver receiver,
      int phase, int seconds, String distribution, boolean print) throws Exception {
    loop.submit(() -> {
      Arrays.fill(receiver.messages, 0); Arrays.fill(receiver.bytes, 0);
      Arrays.fill(receiver.last, -1); Arrays.fill(receiver.sampleCounts, 0);
      receiver.mixed = distribution.equals("mixed");
      receiver.phase = phase;
      return Thread.currentThread().getId();
    }).get(5, TimeUnit.SECONDS);
    long allocation = allocation(threads), cpu = cpu(threads);
    long start = System.nanoTime();
    for (Peer peer : peers) peer.input.println("RUN " + phase + " " + seconds + " " + distribution + " " + Long.getLong("onigiri.rate", 0L));
    long offered = 0, offeredBytes = 0;
    long[] perPeer = new long[peers.size()];
    for (int i = 0; i < peers.size(); i++) {
      String line = peers.get(i).output.readLine();
      assertNotNull(line); String[] parts = line.split(" "); assertEquals("DONE", parts[0]);
      perPeer[i] = Long.parseLong(parts[1]); offered += perPeer[i]; offeredBytes += Long.parseLong(parts[2]);
    }
    markers(peers, receiver, phase);
    long elapsed = System.nanoTime() - start;
    long allocatedAfter = allocation(threads), cpuAfter = cpu(threads);
    final long totalOffered = offered, totalOfferedBytes = offeredBytes;
    loop.submit(() -> {
      assertNull(receiver.failure);
      long received = Arrays.stream(receiver.messages).sum(), bytes = Arrays.stream(receiver.bytes).sum();
      for (int i = 0; i < peers.size(); i++) {
        assertTrue(receiver.messages[i * 16] > 0 && receiver.messages[i * 16] <= perPeer[i]);
        assertTrue(receiver.last[i * 16] < perPeer[i]);
      }
      if (print) {
        long[] latency = new long[Arrays.stream(receiver.sampleCounts).sum()];
        int at = 0;
        for (int i = 0; i < peers.size(); i++) {
          System.arraycopy(receiver.samples[i], 0, latency, at, receiver.sampleCounts[i * 16]);
          at += receiver.sampleCounts[i * 16];
        }
        Arrays.sort(latency);
        assertTrue(latency.length > 0);
        long p999 = latency[(int) Math.ceil(latency.length * .999) - 1];
        System.out.printf("%s: delivered=%.3f GB/s, %.3f Mmsg/s, p99.9=%.3f us, samples=%d%n"
            + "  allocation=%.3f B/msg, %.3f MB/s; sum EventLoop CPU=%.1f%% (100%% = one core)%n"
            + "  offered=%d / %d B, received=%d / %d B, lost=%d (%.3f%%), elapsed=%.6f s%n",
            distribution, bytes / (double) elapsed, received * 1000.0 / elapsed, p999 / 1000.0, latency.length,
            allocation < 0 || allocatedAfter < 0 ? Double.NaN : (allocatedAfter - allocation) / (double) received,
            allocation < 0 || allocatedAfter < 0 ? Double.NaN : (allocatedAfter - allocation) * 1000.0 / elapsed,
            cpu < 0 || cpuAfter < 0 ? Double.NaN : (cpuAfter - cpu) * 100.0 / elapsed,
            totalOffered, totalOfferedBytes, received, bytes, totalOffered - received,
            (totalOffered - received) * 100.0 / totalOffered, elapsed / 1e9);
        for (int i = 0; i < peers.size(); i++) System.out.printf("  publisher %d: %.3f GB/s, delivered=%d/%d%n",
            i, receiver.bytes[i * 16] / (double) elapsed, receiver.messages[i * 16], perPeer[i]);
      }
    }).get(5, TimeUnit.SECONDS);
  }

  static void markers(List<Peer> peers, Receiver receiver, int phase) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(8);
    for (;;) {
      boolean complete = true;
      for (int i = 0; i < peers.size(); i++) {
        if (receiver.markers.get(i) == phase) continue;
        complete = false; Peer peer = peers.get(i);
        peer.input.println("MARK " + phase); assertEquals("MARKED", peer.output.readLine());
      }
      if (complete) return;
      if (receiver.failure != null) fail(receiver.failure);
      if (System.nanoTime() > deadline) fail("Missing native PUB marker");
      Thread.sleep(5);
    }
  }

  static Object field(Object object, String name) throws Exception {
    Field field = object.getClass().getDeclaredField(name); field.setAccessible(true); return field.get(object);
  }
  static long allocation(long[] threads) {
    long total = 0;
    for (long thread : threads) { long value = allocation(thread); if (value < 0) return -1; total += value; }
    return total;
  }
  static long cpu(long[] threads) {
    long total = 0;
    for (long thread : threads) { long value = cpu(thread); if (value < 0) return -1; total += value; }
    return total;
  }
  static long allocation(long thread) {
    var base = ManagementFactory.getThreadMXBean();
    if (!(base instanceof ThreadMXBean bean) || !bean.isThreadAllocatedMemorySupported()) return -1;
    if (!bean.isThreadAllocatedMemoryEnabled()) bean.setThreadAllocatedMemoryEnabled(true);
    return bean.getThreadAllocatedBytes(thread);
  }
  static long cpu(long thread) {
    var bean = ManagementFactory.getThreadMXBean();
    if (!bean.isThreadCpuTimeSupported()) return -1;
    if (!bean.isThreadCpuTimeEnabled()) bean.setThreadCpuTimeEnabled(true);
    return bean.getThreadCpuTime(thread);
  }

  static class Receiver implements SubClient.Listener {
    static final int[] SIZES = {64, 128, 200, 408};
    final Map<SocketAddress, Integer> indexes = new ConcurrentHashMap<>();
    final CountDownLatch ready;
    final java.util.concurrent.atomic.AtomicIntegerArray markers;
    final long[] messages, bytes, last, arrival;
    final long[][] samples;
    final int[] sampleCounts;
    volatile int phase;
    boolean mixed;
    static final class Cache { SocketAddress address; int index; }
    final ThreadLocal<Cache> cache = ThreadLocal.withInitial(Cache::new);
    volatile Throwable failure;
    Receiver(int count) {
      samples = new long[count][65536]; sampleCounts = new int[count * 16];
      ready = new CountDownLatch(count); markers = new java.util.concurrent.atomic.AtomicIntegerArray(count);
      for (int i = 0; i < count; i++) markers.set(i, -1);
      // Isolate counters written by different shard threads: no false sharing in the harness.
      messages = new long[count * 16]; bytes = new long[count * 16]; last = new long[count * 16]; arrival = new long[count * 16];
    }
    @Override public void onReady(SocketAddress address) { ready.countDown(); }
    @Override public void onError(Throwable error) { failure = error; }
    @Override public void onFrame(SocketAddress address, Decoder.Frame frame) {
      consume(address, frame.content(), frame.content().readerIndex(), frame.content().readableBytes(), frame.more());
    }
    void consume(SocketAddress address, ByteBuf body, int offset, int length, boolean more) {
      // Frames from one socket arrive consecutively; resolve the benchmark counter
      // only when the publisher changes, rather than hashing an address per message.
      Cache current = cache.get();
      if (current.address != address) {
        current.index = indexes.get(address); current.address = address;
      }
      int publisherIndex = current.index;
      int index = publisherIndex * 16;
      if (more || length < 9) { failure = new AssertionError("Malformed frame"); return; }
      int type = body.getUnsignedByte(offset);
      if (type == 127) { markers.set(publisherIndex, body.getUnsignedByte(offset + 1)); return; }
      long sequence = body.getLong(offset + 1);
      if (type != phase || sequence <= last[index] || body.getUnsignedByte(offset + length - 1) != phase
          || length != (mixed ? SIZES[(int) (sequence & 3)] : 200)) {
        failure = new AssertionError("Corrupt, reordered or duplicate data"); return;
      }
      last[index] = sequence;
      if ((messages[index] & 255) == 0 && sampleCounts[index] < samples[publisherIndex].length) samples[publisherIndex][sampleCounts[index]++] = System.nanoTime() - arrival[index];
      messages[index]++; bytes[index] += length;
    }
  }

  static final class BorrowedReceiver extends Receiver implements SubClient.MessageListener {
    BorrowedReceiver(int count) { super(count); }
    @Override public void onMessage(SocketAddress address, ByteBuf data, int index, int length, boolean more) {
      consume(address, data, index, length, more);
    }
  }

  static final class Client {
    final SubClient single;
    final ShardedSubClient sharded;
    Client(EventLoopGroup group, int shards, SubClient.Listener listener) {
      single = shards == 1 ? new SubClient(group, new ClientChannelConfig(), new SubClientConfig(5000, 0, 0, 0), listener) : null;
      sharded = shards == 1 ? null : new ShardedSubClient(group, shards, new ClientChannelConfig(), new SubClientConfig(5000, 0, 0, 0), listener);
    }
    void subscribe(byte[] prefix) { if (single != null) single.subscribe(prefix); else sharded.subscribe(prefix); }
    CompletableFuture<Void> connect(SocketAddress address) { return single != null ? single.connect(address) : sharded.connect(address); }
    CompletableFuture<Void> closeAsync() { return single != null ? single.closeAsync() : sharded.closeAsync(); }
    SubClient[] shards() throws Exception { return single != null ? new SubClient[] {single} : (SubClient[]) field(sharded, "shards"); }
  }

  static final class Peer implements AutoCloseable {
    final Process process;
    final PrintWriter input;
    final BufferedReader output;
    final InetSocketAddress address;
    Peer() throws Exception {
      process = new ProcessBuilder(Path.of(System.getProperty("onigiri.nativePublisher", "target/subscriber_load")).toAbsolutePath().toString())
          .redirectError(ProcessBuilder.Redirect.INHERIT).start();
      CompletableFuture.delayedExecutor(80, TimeUnit.SECONDS).execute(() -> { if (process.isAlive()) process.destroyForcibly(); });
      input = new PrintWriter(process.getOutputStream(), true);
      output = new BufferedReader(new InputStreamReader(process.getInputStream()));
      String greeting = output.readLine(); assertNotNull(greeting); System.out.println(greeting);
      URI uri = URI.create(greeting.split(" ")[0]); address = new InetSocketAddress(uri.getHost(), uri.getPort());
    }
    @Override public void close() throws Exception {
      input.println("QUIT");
      if (!process.waitFor(3, TimeUnit.SECONDS)) { process.destroyForcibly(); process.waitFor(3, TimeUnit.SECONDS); }
      input.close(); output.close();
    }
  }
}
