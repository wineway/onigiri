package me.wineway.onigiri.session;

import static org.junit.jupiter.api.Assertions.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import com.sun.management.ThreadMXBean;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.EventLoop;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import me.wineway.onigiri.protocol.Decoder;
import me.wineway.onigiri.transport.ClientChannelConfig;

/** Explicit benchmark: mvn -pl transport -Dtest=SubClientBenchmark test (requires pyzmq). */
class SubClientBenchmark {
  private static final int PAYLOAD = 200;
  private static final boolean MULTIPART = Boolean.getBoolean("onigiri.multipart");
  private static final long WARMUP_NS = TimeUnit.SECONDS.toNanos(2);
  private static final long MEASURE_NS = TimeUnit.SECONDS.toNanos(3);

  @Test
  @Timeout(90)
  void localPipelineAndNativePublisher() throws Exception {
    var group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    EventLoop loop = group.next();
    var receiver = MULTIPART ? new CompleteReceiver()
        : Boolean.getBoolean("onigiri.borrowed") ? new BorrowedReceiver() : new Receiver();
    var client = new SubClient(group, new ClientChannelConfig(20, 200, 1024, 8, 2000),
        new SubClientConfig(5000, 0, 0, 0), receiver);
    Process peer = null;
    try {
      peer = new ProcessBuilder(System.getProperty("onigiri.python", "python3"), "-u",
          Path.of("src/test/python/subscriber_benchmark_peer.py").toAbsolutePath().toString(),
          MULTIPART ? "multipart" : "single")
          .redirectError(ProcessBuilder.Redirect.INHERIT).start();
      // Also bound process lifetime on platforms without Python signal.alarm.
      Process child = peer;
      CompletableFuture.delayedExecutor(80, TimeUnit.SECONDS).execute(() -> {
        if (child.isAlive()) child.destroyForcibly();
      });
      try (var output = new BufferedReader(new InputStreamReader(peer.getInputStream()));
          var input = new PrintWriter(peer.getOutputStream(), true)) {
        String greeting = output.readLine();
        assertNotNull(greeting, "Native PUB failed to start; install pyzmq");
        System.out.println("\nSubClient benchmark: " + greeting);
        System.out.printf("runtime: %s %s, logical CPUs=%d, payload=%d B, heartbeat=off%n",
            System.getProperty("java.vm.name"), System.getProperty("java.version"),
            Runtime.getRuntime().availableProcessors(), PAYLOAD);
        System.out.println("multipart=" + MULTIPART + ", topic=" + (MULTIPART ? 8 : 0) + " B");
        client.subscribe(new byte[0]);
        client.connect(new InetSocketAddress("127.0.0.1", Integer.parseInt(greeting.split(" ")[1])))
            .get(5, TimeUnit.SECONDS);
        receiver.ready.get(5, TimeUnit.SECONDS);
        marker(input, output, receiver, 0);
        // Reflection is test-only setup, outside measurements: retain the actual live pipeline.
        Channel channel = loop.submit(() -> channel(client)).get(5, TimeUnit.SECONDS);
        for (int batch : new int[] {1, 256}) {
          loop.submit(() -> local(channel, receiver, batch)).get(20, TimeUnit.SECONDS);
        }
        loop.submit(() -> {
          channel.pipeline().addFirst("benchmarkArrival", new ChannelInboundHandlerAdapter() {
            @Override
            public void channelRead(ChannelHandlerContext ctx, Object message) {
              receiver.arrival = System.nanoTime();
              ctx.fireChannelRead(message);
            }
          });
        }).get(5, TimeUnit.SECONDS);
        network(input, output, receiver, loop, 1, 2, false);
        network(input, output, receiver, loop, 2, 3, true);
        assertNull(receiver.failure);
        client.closeAsync().get(5, TimeUnit.SECONDS);
        input.println("QUIT");
      }
      assertTrue(peer.waitFor(5, TimeUnit.SECONDS));
      assertEquals(0, peer.exitValue());
    } finally {
      if (peer != null && peer.isAlive()) {
        peer.destroyForcibly();
        peer.waitFor(5, TimeUnit.SECONDS);
      }
      try { client.closeAsync().get(5, TimeUnit.SECONDS); }
      finally { group.shutdownGracefully(0, 5, TimeUnit.SECONDS).sync(); }
    }
  }

  private static void local(Channel channel, Receiver receiver, int batch) {
    ByteBuf wire = PooledByteBufAllocator.DEFAULT.directBuffer((PAYLOAD + (MULTIPART ? 12 : 2)) * batch);
    long checksumPerBatch = 0;
    for (int i = 0; i < batch; i++) {
      int value = (i + 1) & 255;
      if (MULTIPART) wire.writeByte(1).writeByte(8).writeLong(0);
      wire.writeShort(PAYLOAD).writeZero(PAYLOAD - 1).writeByte(value);
      checksumPerBatch += value;
    }
    receiver.network = false;
    try {
      runLocal(channel, wire, System.nanoTime() + WARMUP_NS, null, 0);
      receiver.count = 0;
      receiver.checksum = 0;
      var samples = new Samples();
      long allocated = allocated(Thread.currentThread().getId());
      long start = System.nanoTime();
      long batches = runLocal(channel, wire, start + MEASURE_NS, samples, batch == 1 ? 1023 : 15);
      long elapsed = System.nanoTime() - start;
      long bytes = allocationDelta(allocated, allocated(Thread.currentThread().getId()));
      long delivered = receiver.count;
      assertEquals(batches * batch, delivered);
      assertEquals(batches * checksumPerBatch, receiver.checksum);
      report("LOCAL batch=" + batch, delivered, elapsed, bytes, samples.values, samples.count);
      System.out.println("  latency scope: entire input chunk, through callback, release and readComplete; sampled during throughput; no network/scheduling");
      assertEquals(1, wire.refCnt(), "Receive path must release every injected reference");
      assertNull(receiver.failure);
    } finally {
      wire.release();
    }
  }

  private static final class Samples {
    final long[] values = new long[131_072];
    int count;
  }

  private static long runLocal(Channel channel, ByteBuf wire, long deadline, Samples samples, int mask) {
    long batches = 0;
    do {
      boolean sample = samples != null && (batches & mask) == 0 && samples.count < samples.values.length;
      long start = sample ? System.nanoTime() : 0;
      channel.pipeline().fireChannelRead(wire.retainedDuplicate());
      channel.pipeline().fireChannelReadComplete();
      if (sample) samples.values[samples.count++] = System.nanoTime() - start;
      batches++;
    } while (System.nanoTime() < deadline);
    return batches;
  }

  private static void network(PrintWriter input, BufferedReader output, Receiver receiver,
      EventLoop loop, int phase, int seconds, boolean report) throws Exception {
    long thread = loop.submit(() -> {
      receiver.network = true;
      receiver.phase = phase;
      receiver.count = 0;
      receiver.checksum = 0;
      receiver.lastArrival = 0;
      receiver.lastSequence = -1;
      receiver.samples = 0;
      receiver.chunks = 0;
      return Thread.currentThread().getId();
    }).get(5, TimeUnit.SECONDS);
    long allocated = allocated(thread);
    long start = System.nanoTime();
    input.println("RUN " + phase + " " + seconds);
    String done = output.readLine();
    assertNotNull(done);
    assertTrue(done.startsWith("DONE "), done);
    long offered = Long.parseLong(done.split(" ")[1]);
    long sourceNs = Long.parseLong(done.split(" ")[2]);
    marker(input, output, receiver, phase);
    long elapsed = System.nanoTime() - start;
    long bytes = allocationDelta(allocated, allocated(thread));
    loop.submit(() -> {
      assertNull(receiver.failure);
      assertTrue(receiver.count > 0 && receiver.count <= offered);
      assertTrue(receiver.lastSequence < offered);
      assertEquals(receiver.count * phase, receiver.checksum);
      if (report) {
        report("NETWORK native PUB -> loopback TCP -> 1 SubClient / 1 EventLoop",
            receiver.count, elapsed, bytes, receiver.latency, receiver.samples);
        System.out.printf("  offered=%d, delivered=%d, lost=%d, delivered=%.3f%%, source send=%.3f Mmsg/s%n",
            offered, receiver.count, offered - receiver.count, receiver.count * 100.0 / offered,
            offered * 1000.0 / sourceNs);
        System.out.printf("  data-bearing input chunks=%d, mean messages/chunk=%.2f%n",
            receiver.chunks, receiver.count / (double) receiver.chunks);
        System.out.println("  latency scope: sampled chunk arrival -> callback, excludes network and socket wait; every 64th message");
      }
    }).get(5, TimeUnit.SECONDS);
  }

  private static void marker(PrintWriter input, BufferedReader output, Receiver receiver, int phase)
      throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    do {
      input.println("MARK " + phase);
      assertEquals("MARKED", output.readLine());
      long wait = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(20);
      while (receiver.marker != phase && System.nanoTime() < wait) {
        if (receiver.failure != null) fail(receiver.failure);
        Thread.sleep(1);
      }
      if (receiver.marker == phase) return;
    } while (System.nanoTime() < deadline);
    fail("Native PUB marker not delivered");
  }

  private static void report(String topology, long messages, long elapsed, long bytes,
      long[] latency, int samples) {
    assertTrue(samples > 0);
    long[] sorted = Arrays.copyOf(latency, samples);
    Arrays.sort(sorted);
    double duration = elapsed / 1e9;
    double gbps = messages * (double) PAYLOAD / elapsed;
    double p999 = sorted[Math.min(samples - 1, (int) Math.ceil(samples * .999) - 1)] / 1000.0;
    System.out.printf("%s%n  delivered=%.3f GB/s, %.3f Mmsg/s, messages=%d, elapsed=%.6f s%n"
            + "  p99.9=%.3f us, samples=%d%n  EventLoop Java allocation=%.3f B/message, %.3f MB/s%n"
            + "  targets (this topology only): throughput >=5 GB/s: %s, processing p99.9 <1 ms: %s%n",
        topology, gbps, messages / duration / 1e6, messages, duration, p999, samples,
        bytes < 0 ? Double.NaN : bytes / (double) messages,
        bytes < 0 ? Double.NaN : bytes / duration / 1e6, gbps >= 5, p999 < 1000);
  }

  private static long allocationDelta(long before, long after) {
    return before < 0 || after < 0 ? -1 : after - before;
  }

  private static long allocated(long thread) {
    var base = ManagementFactory.getThreadMXBean();
    if (!(base instanceof ThreadMXBean bean) || !bean.isThreadAllocatedMemorySupported()) return -1;
    if (!bean.isThreadAllocatedMemoryEnabled()) bean.setThreadAllocatedMemoryEnabled(true);
    return bean.getThreadAllocatedBytes(thread);
  }

  private static Channel channel(SubClient client) throws Exception {
    Map<?, ?> sessions = (Map<?, ?>) field(client, "sessions");
    assertEquals(1, sessions.size());
    return (Channel) field(field(sessions.values().iterator().next(), "transport"), "channel");
  }

  private static Object field(Object object, String name) throws Exception {
    Field field = object.getClass().getDeclaredField(name);
    field.setAccessible(true);
    return field.get(object);
  }

  private static class Receiver implements SubClient.Listener {
    final CompletableFuture<Void> ready = new CompletableFuture<>();
    final long[] latency = new long[131_072];
    volatile Throwable failure;
    volatile int marker = -1;
    boolean network;
    int phase;
    int samples;
    long count;
    long lastSequence;
    long arrival;
    long lastArrival;
    long chunks;
    long checksum;

    @Override
    public void onReady() { ready.complete(null); }
    @Override
    public void onError(Throwable cause) { failure = cause; }
    @Override
    public void onFrame(Decoder.Frame frame) {
      consume(frame.content(), frame.content().readerIndex(), frame.content().readableBytes(), frame.more());
    }
    void consume(ByteBuf body, int index, int length, boolean more) {
      if (length != PAYLOAD || more) {
        failure = new AssertionError("Invalid message size or multipart flag");
        return;
      }
      int type = body.getUnsignedByte(index);
      if (type == 127) {
        marker = body.getUnsignedByte(index + 1);
        return;
      }
      if (network) {
        if (type != phase) {
          failure = new AssertionError("Message crossed measurement phase");
          return;
        }
        long sequence = body.getLong(index + 1);
        if (sequence <= lastSequence) failure = new AssertionError("Duplicate or reordered message");
        lastSequence = sequence;
        if (arrival != lastArrival) { chunks++; lastArrival = arrival; }
        if ((count & 63) == 0 && samples < latency.length) latency[samples++] = System.nanoTime() - arrival;
      }
      checksum += body.getUnsignedByte(index + PAYLOAD - 1);
      count++;
    }
  }
  private static final class CompleteReceiver extends Receiver implements SubClient.MultipartListener {
    @Override public void onMultipart(java.net.SocketAddress publisher, MultipartView message) {
      if (message.partCount() != 2 || message.length(0) != 8) {
        failure = new AssertionError("Invalid multipart envelope");
        return;
      }
      consume(message.buffer(1), message.index(1), message.length(1), false);
    }
  }
  private static final class BorrowedReceiver extends Receiver implements SubClient.MessageListener {
    @Override public void onMessage(java.net.SocketAddress publisher, ByteBuf data,
        int index, int length, boolean more) { consume(data, index, length, more); }
  }

}
