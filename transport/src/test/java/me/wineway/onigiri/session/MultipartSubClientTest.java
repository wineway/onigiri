package me.wineway.onigiri.session;

import static org.junit.jupiter.api.Assertions.*;
import java.io.DataInputStream;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import io.netty.buffer.*;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.handler.codec.CorruptedFrameException;
import me.wineway.onigiri.protocol.Encoder;
import me.wineway.onigiri.transport.ClientChannelConfig;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@Timeout(20)
class MultipartSubClientTest {
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void publishersAssembleIndependentlyIncludingShards(boolean sharded) throws Exception {
    try (Peer peer = new Peer(sharded, 5000); ServerSocket secondServer = new ServerSocket(0)) {
      peer.connect(peer.server.getLocalSocketAddress());
      peer.connect(secondServer.getLocalSocketAddress());
      try (Socket first = peer.server.accept(); Socket second = secondServer.accept()) {
        peer.handshake(first); peer.handshake(second);
        first.getOutputStream().write(new byte[] {1, 1, 65});
        second.getOutputStream().write(new byte[] {1, 1, 66, 0, 1, 50});
        assertEquals(List.of("B", "2"), peer.messages.poll(5, TimeUnit.SECONDS));
        assertNull(peer.messages.poll(50, TimeUnit.MILLISECONDS));
        first.getOutputStream().write(new byte[] {0, 1, 49});
        assertEquals(List.of("A", "1"), peer.messages.poll(5, TimeUnit.SECONDS));
        assertTrue(peer.errors.isEmpty());
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void incompleteMessageFailsOnEofOrDeadlineAndReconnectStartsClean(boolean eof) throws Exception {
    try (Peer peer = new Peer(false, eof ? 5000 : 100)) {
      peer.connect(peer.server.getLocalSocketAddress());
      try (Socket first = peer.server.accept()) {
        peer.handshake(first);
        first.getOutputStream().write(new byte[] {1, 1, 65});
        if (eof) first.shutdownOutput();
        Throwable failure = peer.errors.poll(5, TimeUnit.SECONDS);
        if (eof) assertInstanceOf(CorruptedFrameException.class, failure);
        else assertInstanceOf(TimeoutException.class, failure);
        assertTrue(peer.messages.isEmpty());
      }
      try (Socket second = peer.server.accept()) {
        peer.handshake(second);
        second.getOutputStream().write(new byte[] {1, 1, 66, 0, 1, 50});
        assertEquals(List.of("B", "2"), peer.messages.poll(5, TimeUnit.SECONDS));
      }
    }
  }

  @Test
  void callbackFailureReleasesBorrowedBuffersAndDisconnects() throws Exception {
    try (Peer peer = new Peer(false, 5000)) {
      peer.throwOnMessage = true;
      peer.connect(peer.server.getLocalSocketAddress());
      try (Socket socket = peer.server.accept()) {
        peer.handshake(socket);
        socket.getOutputStream().write(new byte[] {1, 1, 65, 0, 1, 49});
        Throwable failure = peer.errors.poll(5, TimeUnit.SECONDS);
        assertInstanceOf(io.netty.handler.codec.DecoderException.class, failure);
        assertInstanceOf(IllegalStateException.class, failure.getCause());
        assertEquals(-1, socket.getInputStream().read());
        peer.group.next().submit(() -> {}).syncUninterruptibly();
        for (ByteBuf buffer : peer.lastBuffers) assertEquals(0, buffer.refCnt());
      }
    }
  }

  static final class Peer implements AutoCloseable {
    final MultiThreadIoEventLoopGroup group = new MultiThreadIoEventLoopGroup(2, NioIoHandler.newFactory());
    final ServerSocket server = new ServerSocket(0);
    final LinkedBlockingQueue<List<String>> messages = new LinkedBlockingQueue<>();
    final LinkedBlockingQueue<Throwable> errors = new LinkedBlockingQueue<>();
    final LinkedBlockingQueue<Boolean> ready = new LinkedBlockingQueue<>();
    final SubClient client;
    final ShardedSubClient shards;
    volatile boolean throwOnMessage;
    volatile ByteBuf[] lastBuffers;
    Peer(boolean sharded, int timeout) throws Exception {
      server.setSoTimeout(5000);
      var listener = new SubClient.MultipartListener() {
        public void onMultipart(SocketAddress address, MultipartView view) {
          if (throwOnMessage) {
            lastBuffers = new ByteBuf[view.partCount()];
            for (int i = 0; i < view.partCount(); i++) lastBuffers[i] = view.buffer(i);
            throw new IllegalStateException("consumer failed");
          }
          List<String> parts = new ArrayList<>();
          for (int i = 0; i < view.partCount(); i++) parts.add(view.buffer(i).toString(
              view.index(i), view.length(i), java.nio.charset.StandardCharsets.UTF_8));
          messages.add(parts);
        }
        public void onReady() { ready.add(true); }
        public void onError(Throwable failure) { errors.add(failure); }
      };
      var transport = new ClientChannelConfig(50, 100, 1024, 8, 1000);
      var config = new SubClientConfig(2000, 0, 0, 0, 64, 4096, timeout);
      client = sharded ? null : new SubClient(group, transport, config, listener);
      shards = sharded ? new ShardedSubClient(group, 2, transport, config, listener) : null;
    }
    void connect(SocketAddress address) throws Exception {
      (client != null ? client.connect(address) : shards.connect(address)).get(5, TimeUnit.SECONDS);
    }
    void handshake(Socket socket) throws Exception {
      socket.setSoTimeout(5000);
      var input = new DataInputStream(socket.getInputStream());
      assertEquals(64, input.readNBytes(64).length);
      var encoder = new Encoder(UnpooledByteBufAllocator.DEFAULT);
      write(socket, encoder.greeting());
      assertEquals(4, input.readUnsignedByte());
      int size = input.readUnsignedByte();
      assertEquals(size, input.readNBytes(size).length);
      write(socket, encoder.ready(Map.of("Socket-Type", new byte[] {'P', 'U', 'B'})));
      assertNotNull(ready.poll(5, TimeUnit.SECONDS));
    }
    static void write(Socket socket, ByteBuf data) throws Exception {
      try { socket.getOutputStream().write(ByteBufUtil.getBytes(data)); }
      finally { data.release(); }
    }
    public void close() throws Exception {
      if (client != null) client.closeAsync().get(5, TimeUnit.SECONDS);
      if (shards != null) shards.closeAsync().get(5, TimeUnit.SECONDS);
      server.close();
      group.shutdownGracefully(0, 5, TimeUnit.SECONDS).sync();
    }
  }
}
