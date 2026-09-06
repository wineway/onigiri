package me.wineway.onigiri.session;

import static org.junit.jupiter.api.Assertions.*;

import java.io.DataInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.handler.codec.CorruptedFrameException;
import me.wineway.onigiri.protocol.Decoder;
import me.wineway.onigiri.protocol.Encoder;
import me.wineway.onigiri.transport.ClientChannelConfig;

@Timeout(20)
class SubClientLivenessTest {
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void handshakeDeadlineCoversGreetingAndReadyAndResetsOnReconnect(boolean sendGreeting) throws Exception {
    try (Peer test = new Peer(new SubClientConfig(400, 0, 0, 0))) {
      try (Socket first = test.accept()) {
        if (sendGreeting) {
          test.exchangeGreeting(first);
        }
        Throwable failure = test.errors.poll(5, TimeUnit.SECONDS);
        assertInstanceOf(TimeoutException.class, failure);
        assertEquals("Handshake timed out", failure.getMessage());
      }
      try (Socket second = test.accept()) {
        test.handshake(second);
        assertNull(test.errors.poll(600, TimeUnit.MILLISECONDS));
        assertTrue(test.client.isReady());
      }
    }
  }

  @Test
  void sendsPingAndTimesOutEvenWhenFurtherProbesAreSent() throws Exception {
    try (Peer test = new Peer(new SubClientConfig(2000, 50, 200, 399));
         Socket socket = test.accept()) {
      test.handshake(socket);
      assertArrayEquals(new byte[] {0, 3}, readCommand(socket, "PING"));
      Throwable failure = test.errors.poll(5, TimeUnit.SECONDS);
      assertInstanceOf(TimeoutException.class, failure);
      assertEquals("Heartbeat timed out", failure.getMessage());
      assertFalse(test.client.isReady());
    }
  }

  @Test
  void incomingPartialFrameBytesAnswerHeartbeatProbes() throws Exception {
    try (Peer test = new Peer(new SubClientConfig(2000, 40, 300, 0));
         Socket socket = test.accept()) {
      test.handshake(socket);
      readCommand(socket, "PING");
      socket.getOutputStream().write(new byte[] {0, 20});
      for (int i = 0; i < 5; i++) {
        assertNull(test.errors.poll(100, TimeUnit.MILLISECONDS));
        socket.getOutputStream().write(42); // deliberately incomplete data frame
      }
      assertTrue(test.client.isReady());
      Throwable failure = test.errors.poll(5, TimeUnit.SECONDS);
      assertInstanceOf(TimeoutException.class, failure);
      assertEquals("Heartbeat timed out", failure.getMessage());
    }
  }

  @Test
  void peerTtlIsRefreshedByTrafficEvenWithLocalHeartbeatDisabled() throws Exception {
    try (Peer test = new Peer(new SubClientConfig(2000, 0, 0, 0));
         Socket socket = test.accept()) {
      test.handshake(socket);
      socket.getOutputStream().write(ByteBufUtil.decodeHexDump("04080450494e47000341"));
      assertArrayEquals(new byte[] {'A'}, readCommand(socket, "PONG"));
      socket.getOutputStream().write(new byte[] {0, 20});
      for (int i = 0; i < 5; i++) {
        assertNull(test.errors.poll(100, TimeUnit.MILLISECONDS));
        socket.getOutputStream().write(42);
      }
      Throwable failure = test.errors.poll(5, TimeUnit.SECONDS);
      assertInstanceOf(TimeoutException.class, failure);
      assertEquals("Peer PING TTL timed out", failure.getMessage());
    }
  }

  @Test
  void zeroPeerTtlCancelsPreviousDeadlineAndCloseCancelsAllTimers() throws Exception {
    try (Peer test = new Peer(new SubClientConfig(2000, 0, 0, 0));
         Socket socket = test.accept()) {
      test.handshake(socket);
      socket.getOutputStream().write(ByteBufUtil.decodeHexDump("04070450494e470002"));
      readCommand(socket, "PONG");
      socket.getOutputStream().write(ByteBufUtil.decodeHexDump("04070450494e470000"));
      readCommand(socket, "PONG");
      assertNull(test.errors.poll(400, TimeUnit.MILLISECONDS));
      socket.getOutputStream().write(ByteBufUtil.decodeHexDump("04070450494e470002"));
      readCommand(socket, "PONG");
      test.client.closeAsync().get(5, TimeUnit.SECONDS);
      assertNull(test.errors.poll(400, TimeUnit.MILLISECONDS));
    }
  }

  @Test
  void reportsProtocolErrorsAndProtectsCleanupFromErrorCallbackFailures() throws Exception {
    try (Peer test = new Peer(new SubClientConfig(2000, 50, 200, 0));
         Socket socket = test.accept()) {
      test.throwOnError = true;
      test.exchangeGreeting(socket);
      write(socket, test.encoder.ready(Map.of("Socket-Type", new byte[] {'S', 'U', 'B'})));
      assertInstanceOf(CorruptedFrameException.class, test.errors.poll(5, TimeUnit.SECONDS));
      assertEquals(-1, socket.getInputStream().read());
      assertFalse(test.client.isReady());
    }
  }

  @Test
  void reportsListenerExceptionAndReleasesFrame() throws Exception {
    try (Peer test = new Peer(new SubClientConfig(2000, 0, 0, 0));
         Socket socket = test.accept()) {
      test.handshake(socket);
      test.throwOnFrame = true;
      socket.getOutputStream().write(ByteBufUtil.decodeHexDump("000141"));
      Throwable failure = test.errors.poll(5, TimeUnit.SECONDS);
      assertInstanceOf(IllegalStateException.class, failure);
      assertEquals("consumer failed", failure.getMessage());
      assertEquals(-1, socket.getInputStream().read());
      test.group.next().submit(() -> {}).get(5, TimeUnit.SECONDS);
      assertEquals(0, test.lastFrame.refCnt());
    }
  }

  @Test
  void reportsPeerRejectionAndDoesNotReconnect() throws Exception {
    try (Peer test = new Peer(new SubClientConfig(2000, 0, 0, 0));
         Socket socket = test.accept()) {
      test.exchangeGreeting(socket);
      socket.getOutputStream().write(ByteBufUtil.decodeHexDump("0409054552524f52026e6f"));
      Throwable failure = test.errors.poll(5, TimeUnit.SECONDS);
      assertEquals("Peer rejected handshake: no", failure.getMessage());
      assertEquals(-1, socket.getInputStream().read());
      test.server.setSoTimeout(200);
      assertThrows(java.net.SocketTimeoutException.class, test.server::accept);
    }
  }

  private static final class Peer implements AutoCloseable {
    final MultiThreadIoEventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    final ServerSocket server;
    final Encoder encoder = new Encoder(UnpooledByteBufAllocator.DEFAULT);
    final LinkedBlockingQueue<Throwable> errors = new LinkedBlockingQueue<>();
    final LinkedBlockingQueue<Boolean> ready = new LinkedBlockingQueue<>();
    final SubClient client;
    volatile boolean throwOnError;
    volatile boolean throwOnFrame;
    volatile Decoder.Frame lastFrame;

    Peer(SubClientConfig config) throws Exception {
      server = new ServerSocket(0);
      server.setSoTimeout(5000);
      client = new SubClient(group, new ClientChannelConfig(20, 200, 1024, 8, 1000), config,
          new SubClient.Listener() {
            @Override
            public void onReady() { ready.add(true); }
            @Override
            public void onError(Throwable failure) {
              errors.add(failure);
              if (throwOnError) {
                throw new IllegalStateException("error listener failed");
              }
            }
            @Override
            public void onFrame(Decoder.Frame frame) {
              lastFrame = frame;
              if (throwOnFrame) {
                throw new IllegalStateException("consumer failed");
              }
            }
          });
      client.connect(server.getLocalSocketAddress()).get(5, TimeUnit.SECONDS);
    }

    Socket accept() throws Exception {
      Socket socket = server.accept();
      socket.setSoTimeout(5000);
      return socket;
    }

    void exchangeGreeting(Socket socket) throws Exception {
      assertEquals(64, new DataInputStream(socket.getInputStream()).readNBytes(64).length);
      write(socket, encoder.greeting());
      readCommand(socket, "READY");
    }

    void handshake(Socket socket) throws Exception {
      exchangeGreeting(socket);
      write(socket, encoder.ready(Map.of("Socket-Type", new byte[] {'P', 'U', 'B'})));
      assertEquals(Boolean.TRUE, ready.poll(5, TimeUnit.SECONDS));
    }

    @Override
    public void close() throws Exception {
      client.closeAsync().get(5, TimeUnit.SECONDS);
      server.close();
      group.shutdownGracefully(0, 5, TimeUnit.SECONDS).sync();
    }
  }

  private static byte[] readCommand(Socket peer, String expectedName) throws Exception {
    var in = new DataInputStream(peer.getInputStream());
    assertEquals(4, in.readUnsignedByte());
    int length = in.readUnsignedByte();
    int nameLength = in.readUnsignedByte();
    assertEquals(expectedName, new String(in.readNBytes(nameLength), StandardCharsets.US_ASCII));
    return in.readNBytes(length - nameLength - 1);
  }

  private static void write(Socket socket, ByteBuf wire) throws Exception {
    try {
      socket.getOutputStream().write(ByteBufUtil.getBytes(wire));
    } finally {
      wire.release();
    }
  }
}
