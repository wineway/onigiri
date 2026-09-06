package me.wineway.onigiri.session;

import static org.junit.jupiter.api.Assertions.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.HexFormat;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import me.wineway.onigiri.protocol.Decoder;
import me.wineway.onigiri.transport.ClientChannelConfig;

@EnabledIfSystemProperty(named = "onigiri.zmq", matches = "true")
class ZeroMqSubscriberInteropTest {
  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2})
  @Timeout(30)
  void receivesFromNativeZeroMqPublisher(int mode) throws Exception {
    var group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    var received = new LinkedBlockingQueue<byte[]>();
    var errors = new LinkedBlockingQueue<Throwable>();
    class Receiver implements SubClient.Listener {
      void consume(io.netty.buffer.ByteBuf data, int index, int length, boolean more) {
        assertFalse(more);
        byte[] bytes = new byte[length];
        data.getBytes(index, bytes);
        received.add(bytes);
      }
      @Override public void onFrame(Decoder.Frame frame) {
        consume(frame.content(), frame.content().readerIndex(), frame.content().readableBytes(), frame.more());
      }
      @Override public void onError(Throwable failure) { errors.add(failure); }
    }
    class BorrowedReceiver extends Receiver implements SubClient.MessageListener {
      @Override public void onMessage(java.net.SocketAddress address, io.netty.buffer.ByteBuf data,
          int index, int length, boolean more) { consume(data, index, length, more); }
    }
    class CompleteReceiver extends Receiver implements SubClient.MultipartListener {
      @Override public void onMultipart(java.net.SocketAddress address, MultipartView message) {
        for (int i = 0; i < message.partCount(); i++) {
          consume(message.buffer(i), message.index(i), message.length(i), false);
        }
      }
    }
    var subscriber = new SubClient(group, new ClientChannelConfig(20, 200, 1024, 8, 1000),
        new SubClientConfig(2000, 100, 2000, 2000), mode == 2 ? new CompleteReceiver() : mode == 1 ? new BorrowedReceiver() : new Receiver());
    Process peer = null;
    try {
      peer = new ProcessBuilder(System.getProperty("onigiri.python", "python3"), "-u",
          Path.of("src/test/python/subscriber_peer.py").toAbsolutePath().toString())
          .redirectError(ProcessBuilder.Redirect.INHERIT).start();
      try (var output = new BufferedReader(new InputStreamReader(peer.getInputStream()));
          var input = new PrintWriter(peer.getOutputStream(), true)) {
        String greeting = output.readLine();
        assertNotNull(greeting);
        System.out.println("libzmq PUB peer: " + greeting);
        var address = new InetSocketAddress("127.0.0.1", Integer.parseInt(greeting.split(" ")[1]));
        subscriber.subscribe(new byte[] {0, (byte) 255});
        subscriber.connect(address).get(5, TimeUnit.SECONDS);
        synchronize(input, output, received, new byte[] {0, (byte) 255, 1});
        for (int size : new int[] {2, 200, 255, 256, 4096}) {
          byte[] message = new byte[size];
          message[1] = (byte) 255;
          send(input, output, new byte[] {66, 1});
          send(input, output, message);
          assertArrayEquals(message, received.poll(3, TimeUnit.SECONDS));
          assertNull(received.poll(50, TimeUnit.MILLISECONDS));
        }
        if (mode == 2) {
          for (int size : new int[] {0, 200, 255, 256, 4096}) {
            byte[] payload = new byte[size];
            input.println("MULTI 00ff,-," + (size == 0 ? "-" : HexFormat.of().formatHex(payload)));
            assertEquals("SENT", output.readLine());
            assertArrayEquals(new byte[] {0, (byte) 255}, received.poll(3, TimeUnit.SECONDS));
            assertArrayEquals(new byte[0], received.poll(3, TimeUnit.SECONDS));
            assertArrayEquals(payload, received.poll(3, TimeUnit.SECONDS));
          }
        }
        subscriber.unsubscribe(new byte[] {0, (byte) 255});
        subscriber.subscribe("Z");
        synchronize(input, output, received, new byte[] {90, 1});
        send(input, output, new byte[] {0, (byte) 255});
        assertNull(received.poll(100, TimeUnit.MILLISECONDS));
        subscriber.disconnect(address).get(5, TimeUnit.SECONDS);
        subscriber.connect(address).get(5, TimeUnit.SECONDS);
        synchronize(input, output, received, new byte[] {90, 2});
        subscriber.closeAsync().get(5, TimeUnit.SECONDS);
        input.println("QUIT");
      }
      assertTrue(peer.waitFor(5, TimeUnit.SECONDS));
      assertEquals(0, peer.exitValue());
      assertTrue(errors.isEmpty(), errors.toString());
    } finally {
      if (peer != null && peer.isAlive()) {
        peer.destroyForcibly();
        peer.waitFor(5, TimeUnit.SECONDS);
      }
      subscriber.closeAsync().get(5, TimeUnit.SECONDS);
      group.shutdownGracefully(0, 5, TimeUnit.SECONDS).sync();
    }
  }

  private static void send(PrintWriter input, BufferedReader output, byte[] message) throws Exception {
    input.println("SEND " + HexFormat.of().formatHex(message));
    assertEquals("SENT", output.readLine());
  }

  private static void synchronize(PrintWriter input, BufferedReader output,
      LinkedBlockingQueue<byte[]> received, byte[] marker) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (System.nanoTime() < deadline) {
      send(input, output, marker);
      byte[] message = received.poll(50, TimeUnit.MILLISECONDS);
      if (message != null) {
        assertArrayEquals(marker, message);
        return;
      }
    }
    fail("Subscription did not reach native PUB peer");
  }
}
