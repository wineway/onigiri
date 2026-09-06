package me.wineway.onigiri.session;

import static org.junit.jupiter.api.Assertions.*;

import java.io.DataInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import me.wineway.onigiri.protocol.Decoder;
import me.wineway.onigiri.protocol.Encoder;
import me.wineway.onigiri.transport.ClientChannelConfig;

@Timeout(20)
class SubClientTest {
  private final Encoder encoder = new Encoder(UnpooledByteBufAllocator.DEFAULT);

  @Test
  void ignoresAllSubscriptionOverloadsDuringAndAfterClose() throws Exception {
    var group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    SubClient session = new SubClient(group);
    try {
      session.subscribe("queued");
      var closing = session.closeAsync();
      assertIgnored(session);
      closing.get(5, TimeUnit.SECONDS);
      assertIgnored(session);
      group.shutdownGracefully(0, 5, TimeUnit.SECONDS).sync();
      // No executor submission should be attempted after close, even when terminated.
      assertIgnored(session);
      assertFalse(session.isReady());
    } finally {
      session.closeAsync().get(5, TimeUnit.SECONDS);
      group.shutdownGracefully(0, 5, TimeUnit.SECONDS).sync();
    }
  }

  private static void assertIgnored(SubClient session) {
    assertDoesNotThrow(() -> {
      session.subscribe("closed");
      session.unsubscribe("closed");
      session.subscribe(new byte[] {0, (byte) 255});
      session.unsubscribe(new byte[] {0, (byte) 255});
    });
  }

  @Test
  void restoresCurrentSubscriptionsAfterHandshakeAndReconnect() throws Exception {
    var group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    var events = new LinkedBlockingQueue<String>();
    SubClient session = new SubClient(group,
        new ClientChannelConfig(1000, 1000, 1024, 8, 1000), new SubClient.Listener() {
          @Override
          public void onReady() { events.add("ready"); }
          @Override
          public void onDisconnected() { events.add("disconnected"); }
          @Override
          public void onFrame(Decoder.Frame frame) {
            events.add(frame.content().toString(StandardCharsets.UTF_8));
          }
        });
    try (ServerSocket server = new ServerSocket(0)) {
      server.setSoTimeout(5000);
      byte[] prefix = {'A'};
      session.subscribe(prefix);
      prefix[0] = 'Z'; // Session must own a copy of subscription data.
      session.subscribe("A");
      session.subscribe("B");
      session.connect(server.getLocalSocketAddress()).get(5, TimeUnit.SECONDS);
      try (Socket peer = server.accept()) {
        handshakeUntilReady(peer);
        peer.setSoTimeout(100);
        assertThrows(SocketTimeoutException.class, () -> peer.getInputStream().read());
        assertFalse(session.isReady());
        peer.setSoTimeout(5000);
        write(peer, encoder.ready(Map.of("Socket-Type", new byte[] {'P', 'U', 'B'})));
        assertEquals("ready", events.poll(5, TimeUnit.SECONDS));
        assertTrue(session.isReady());
        assertCommand(peer, "SUBSCRIBE", "A");
        assertCommand(peer, "SUBSCRIBE", "B");
        session.unsubscribe("A");
        assertCommand(peer, "CANCEL", "A");
        session.subscribe("C");
        assertCommand(peer, "SUBSCRIBE", "C");
        session.subscribe("C");
        session.unsubscribe("missing");
        peer.setSoTimeout(100);
        assertThrows(SocketTimeoutException.class, () -> peer.getInputStream().read());
      }
      assertEquals("disconnected", events.poll(5, TimeUnit.SECONDS));
      assertFalse(session.isReady());
      session.unsubscribe("B");
      session.subscribe("D");
      try (Socket peer = server.accept()) {
        handshakeUntilReady(peer);
        write(peer, encoder.ready(Map.of("Socket-Type", new byte[] {'X', 'P', 'U', 'B'})));
        assertEquals("ready", events.poll(5, TimeUnit.SECONDS));
        assertCommand(peer, "SUBSCRIBE", "C");
        assertCommand(peer, "SUBSCRIBE", "D");
        peer.getOutputStream().write(ByteBufUtil.decodeHexDump("000444617461"));
        assertEquals("Data", events.poll(5, TimeUnit.SECONDS));
        // Heartbeat context must be echoed without the TTL bytes.
        peer.getOutputStream().write(ByteBufUtil.decodeHexDump("04080450494e47000141"));
        assertCommand(peer, "PONG", "A");
        session.closeAsync().get(5, TimeUnit.SECONDS);
        assertEquals(-1, peer.getInputStream().read());
      }
      assertFalse(group.isShuttingDown());
      assertDoesNotThrow(() -> session.subscribe("closed"));
      assertDoesNotThrow(() -> session.unsubscribe("closed"));
      session.closeAsync().get(5, TimeUnit.SECONDS);
    } finally {
      session.closeAsync().get(5, TimeUnit.SECONDS);
      group.shutdownGracefully(0, 5, TimeUnit.SECONDS).sync();
    }
  }

  @Test
  void connectsMultiplePublishersAndIsolatesRejectedSession() throws Exception {
    var group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    var ready = new LinkedBlockingQueue<SocketAddress>();
    var frames = new LinkedBlockingQueue<SocketAddress>();
    var errors = new LinkedBlockingQueue<SocketAddress>();
    SubClient client = new SubClient(group,
        new ClientChannelConfig(1000, 1000, 1024, 8, 1000), new SubClient.Listener() {
          @Override
          public void onReady(SocketAddress publisher) { ready.add(publisher); }
          @Override
          public void onError(SocketAddress publisher, Throwable failure) { errors.add(publisher); }
          @Override
          public void onFrame(SocketAddress publisher, Decoder.Frame frame) { frames.add(publisher); }
        });
    try (ServerSocket firstServer = new ServerSocket(0);
         ServerSocket secondServer = new ServerSocket(0)) {
      SocketAddress firstAddress = firstServer.getLocalSocketAddress();
      SocketAddress secondAddress = secondServer.getLocalSocketAddress();
      client.subscribe("A");
      client.connect(firstAddress).get(5, TimeUnit.SECONDS);
      client.connect(secondAddress).get(5, TimeUnit.SECONDS);
      try (Socket first = firstServer.accept(); Socket second = secondServer.accept()) {
        handshakeUntilReady(first);
        handshakeUntilReady(second);
        write(first, encoder.ready(Map.of("Socket-Type", new byte[] {'P', 'U', 'B'})));
        write(second, encoder.ready(Map.of("Socket-Type", new byte[] {'P', 'U', 'B'})));
        assertEquals(Set.of(firstAddress, secondAddress),
            Set.of(ready.poll(5, TimeUnit.SECONDS), ready.poll(5, TimeUnit.SECONDS)));
        assertCommand(first, "SUBSCRIBE", "A");
        assertCommand(second, "SUBSCRIBE", "A");
        assertTrue(client.isReady());

        first.getOutputStream().write(ByteBufUtil.decodeHexDump("000166"));
        second.getOutputStream().write(ByteBufUtil.decodeHexDump("000173"));
        assertEquals(Set.of(firstAddress, secondAddress),
            Set.of(frames.poll(5, TimeUnit.SECONDS), frames.poll(5, TimeUnit.SECONDS)));

        first.getOutputStream().write(ByteBufUtil.decodeHexDump("0409054552524f52026e6f"));
        assertEquals(firstAddress, errors.poll(5, TimeUnit.SECONDS));
        assertEquals(-1, first.getInputStream().read());
        assertTrue(client.isReady());
        client.subscribe("B");
        assertCommand(second, "SUBSCRIBE", "B");

        client.connect(firstAddress).get(5, TimeUnit.SECONDS);
        try (Socket replacement = firstServer.accept()) {
          handshakeUntilReady(replacement);
          write(replacement, encoder.ready(Map.of("Socket-Type", new byte[] {'P', 'U', 'B'})));
          assertEquals(firstAddress, ready.poll(5, TimeUnit.SECONDS));
          assertCommand(replacement, "SUBSCRIBE", "A");
          assertCommand(replacement, "SUBSCRIBE", "B");
          client.disconnect(firstAddress).get(5, TimeUnit.SECONDS);
          assertEquals(-1, replacement.getInputStream().read());
        }
        assertTrue(client.isReady());
        client.closeAsync().get(5, TimeUnit.SECONDS);
        assertEquals(-1, second.getInputStream().read());
      }
    } finally {
      client.closeAsync().get(5, TimeUnit.SECONDS);
      group.shutdownGracefully(0, 5, TimeUnit.SECONDS).sync();
    }
  }

  @Test
  void invalidPeerReadyClosesConnectionWithoutReplayingSubscriptions() throws Exception {
    var group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    SubClient session = new SubClient(group);
    try (ServerSocket server = new ServerSocket(0)) {
      server.setSoTimeout(5000);
      session.subscribe("A");
      session.connect(server.getLocalSocketAddress()).get(5, TimeUnit.SECONDS);
      try (Socket peer = server.accept()) {
        handshakeUntilReady(peer);
        write(peer, encoder.ready(Map.of("Socket-Type", new byte[] {'S', 'U', 'B'})));
        assertEquals(-1, peer.getInputStream().read());
        assertFalse(session.isReady());
      }
    } finally {
      session.closeAsync().get(5, TimeUnit.SECONDS);
      group.shutdownGracefully(0, 5, TimeUnit.SECONDS).sync();
    }
  }

  private void handshakeUntilReady(Socket peer) throws Exception {
    peer.setSoTimeout(5000);
    byte[] actual = new DataInputStream(peer.getInputStream()).readNBytes(64);
    ByteBuf greeting = encoder.greeting();
    try {
      assertArrayEquals(ByteBufUtil.getBytes(greeting), actual);
    } finally {
      greeting.release();
    }
    write(peer, encoder.greeting());
    byte[] ready = readCommand(peer, "READY");
    ByteBuf metadata = io.netty.buffer.Unpooled.wrappedBuffer(ready);
    try {
      assertArrayEquals(new byte[] {'S', 'U', 'B'}, Decoder.metadata(metadata).get("socket-type"));
    } finally {
      metadata.release();
    }
  }

  private static void assertCommand(Socket peer, String name, String data) throws Exception {
    assertArrayEquals(data.getBytes(StandardCharsets.UTF_8), readCommand(peer, name));
  }

  private static byte[] readCommand(Socket peer, String expectedName) throws Exception {
    var in = new DataInputStream(peer.getInputStream());
    int flags = in.readUnsignedByte();
    assertTrue(flags == 4 || flags == 6);
    long length = flags == 4 ? in.readUnsignedByte() : in.readLong();
    assertTrue(length > 0 && length < 65536);
    int nameLength = in.readUnsignedByte();
    assertEquals(expectedName, new String(in.readNBytes(nameLength), StandardCharsets.US_ASCII));
    return in.readNBytes((int) length - nameLength - 1);
  }

  private static void write(Socket peer, ByteBuf wire) throws Exception {
    try {
      peer.getOutputStream().write(ByteBufUtil.getBytes(wire));
    } finally {
      wire.release();
    }
  }
}
