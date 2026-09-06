package me.wineway.onigiri.publisher;

import static org.junit.jupiter.api.Assertions.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.HexFormat;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import io.netty.buffer.Unpooled;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;

/** Explicit integration gate: requires Python with pyzmq backed by native libzmq. */
@EnabledIfSystemProperty(named = "onigiri.zmq", matches = "true")
class ZeroMqPublisherInteropTest {
  @Test
  @Timeout(45)
  void nativeZeroMqSubscriber() throws Exception {
    var group = new MultiThreadIoEventLoopGroup(2, NioIoHandler.newFactory());
    var errors = new LinkedBlockingQueue<Throwable>();
    var server = new PubServer(group, group,
        new PubServerConfig(64, 16, 256, 2000, 100, 2000, 2000), new PubServer.Listener() {
          @Override
          public void onSubscription(Connection connection, byte[] prefix) {
            if (java.util.Arrays.equals(prefix, new byte[] {'!', 'i', 'n', 'i', 't'})) {
              connection.send(Unpooled.wrappedBuffer(prefix),
                  Unpooled.copiedBuffer("native-init", java.nio.charset.StandardCharsets.US_ASCII));
            }
          }
          @Override
          public void onError(java.net.SocketAddress address, Throwable failure) { errors.add(failure); }
        });
    Process peer = null;
    try {
      server.bind(new InetSocketAddress("127.0.0.1", 0)).get(5, TimeUnit.SECONDS);
      int port = ((InetSocketAddress) server.localAddress()).getPort();
      peer = new ProcessBuilder(System.getProperty("onigiri.python", "python3"), "-u",
          Path.of("src/test/python/publisher_peer.py").toAbsolutePath().toString(),
          "tcp://127.0.0.1:" + port).redirectError(ProcessBuilder.Redirect.INHERIT).start();
      boolean passed = false;
      try (var output = new BufferedReader(new InputStreamReader(peer.getInputStream()));
          var input = new PrintWriter(peer.getOutputStream(), true)) {
        String line;
        while ((line = output.readLine()) != null) {
          if (line.startsWith("SEND ")) {
            try (var writer = server.newBatchWriter()) {
              for (String hex : line.substring(5).split(",")) {
                var body = Unpooled.wrappedBuffer(HexFormat.of().parseHex(hex));
                writer.add(body);
                assertEquals(0, body.refCnt());
              }
              writer.flush();
            }
            input.println("SENT");
          } else if (line.startsWith("MULTI ")) {
            try (var writer = server.newBatchWriter()) {
              for (String message : line.substring(6).split(";")) {
                String[] parts = message.split(",");
                for (int i = 0; i < parts.length; i++) {
                  var part = Unpooled.wrappedBuffer(parts[i].equals("-") ? new byte[0]
                      : HexFormat.of().parseHex(parts[i]));
                  if (parts.length == 1) writer.add(part);
                  else if (i + 1 == parts.length) writer.sendLast(part); else writer.sendMore(part);
                }
              }
              writer.flush();
            }
            input.println("SENT");
          } else if (line.equals("CLOSE")) {
            server.closeAsync().get(5, TimeUnit.SECONDS);
            input.println("CLOSED");
          } else {
            System.out.println("libzmq peer: " + line);
            if (line.equals("PASS")) passed = true;
          }
        }
      }
      assertTrue(peer.waitFor(5, TimeUnit.SECONDS));
      assertEquals(0, peer.exitValue());
      assertTrue(passed, "Native peer did not complete checks");
      server.closeAsync().get(5, TimeUnit.SECONDS);
      assertTrue(errors.isEmpty(), errors.toString());
    } finally {
      if (peer != null && peer.isAlive()) {
        peer.destroyForcibly();
        peer.waitFor(5, TimeUnit.SECONDS);
      }
      server.closeAsync().get(5, TimeUnit.SECONDS);
      group.shutdownGracefully(0, 5, TimeUnit.SECONDS).sync();
    }
  }
}
