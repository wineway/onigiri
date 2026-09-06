package me.wineway.onigiri.transport;

import static org.junit.jupiter.api.Assertions.*;

import java.io.DataInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.IoEventLoop;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;

import me.wineway.onigiri.protocol.Decoder;
import me.wineway.onigiri.protocol.Encoder;

@Timeout(20)
class ClientChannelTest {
  @Test
  void decodesInboundTrafficAndStartsWithFreshGreetingAfterReconnect() throws Exception {
    CountingGroup group = new CountingGroup();
    var events = new LinkedBlockingQueue<String>();
    var lastFrame = new AtomicReference<Decoder.Frame>();
    Encoder encoder = new Encoder(UnpooledByteBufAllocator.DEFAULT);
    ClientChannel client = new ClientChannel(group, new ClientChannelConfig(20, 200, 1024, 8, 1000),
        new ClientChannel.Listener() {
          @Override
          public void onGreeting(Decoder.Greeting greeting) {
            events.add("greeting:" + greeting.major() + "." + greeting.minor());
          }

          @Override
          public void onFrame(Decoder.Frame frame) {
            lastFrame.set(frame);
            events.add(frame.command() + ":" + ByteBufUtil.hexDump(frame.content()));
          }
        });
    try (ServerSocket server = new ServerSocket(0)) {
      server.setSoTimeout(5000);
      client.connect(server.getLocalSocketAddress()).get(5, TimeUnit.SECONDS);
      for (int connection = 0; connection < 2; connection++) {
        try (Socket peer = server.accept()) {
          // Fragment the greeting, then coalesce a command and data frame.
          ByteBuf wire = encoder.greeting();
          try {
            byte[] greeting = ByteBufUtil.getBytes(wire);
            peer.getOutputStream().write(greeting, 0, 11);
            peer.getOutputStream().write(greeting, 11, greeting.length - 11);
          } finally {
            wire.release();
          }
          assertEquals("greeting:3.1", events.poll(5, TimeUnit.SECONDS));
          // READY with empty metadata followed by an ordinary data frame.
          peer.getOutputStream().write(ByteBufUtil.decodeHexDump("04060552454144590003414243"));
          assertEquals("READY:", events.poll(5, TimeUnit.SECONDS));
          assertEquals("null:414243", events.poll(5, TimeUnit.SECONDS));
          group.selected.submit(() -> {}).get(5, TimeUnit.SECONDS);
          assertEquals(0, lastFrame.get().refCnt());
          ByteBuf payload = Unpooled.directBuffer(4).writeInt(connection);
          try {
            client.send(encoder.message(payload, false));
          } finally {
            payload.release();
          }
          peer.setSoTimeout(5000);
          var reply = new DataInputStream(peer.getInputStream());
          assertEquals(0, reply.readUnsignedByte());
          assertEquals(4, reply.readUnsignedByte());
          assertEquals(connection, reply.readInt());
          group.selected.submit(() -> {}).get(5, TimeUnit.SECONDS);
          assertEquals(0, payload.refCnt());
        }
      }
      assertEquals(1, group.selections.get());
    } finally {
      client.closeAsync().get(5, TimeUnit.SECONDS);
      group.shutdownGracefully(0, 5, TimeUnit.SECONDS).sync();
    }
  }

  @Test
  void releasesInboundFrameWhenListenerThrows() throws Exception {
    CountingGroup group = new CountingGroup();
    var received = new LinkedBlockingQueue<Decoder.Frame>();
    ClientChannel client = new ClientChannel(group, new ClientChannelConfig(),
        new ClientChannel.Listener() {
          @Override
          public void onFrame(Decoder.Frame frame) {
            received.add(frame);
            throw new IllegalStateException("listener failed");
          }
        });
    try (ServerSocket server = new ServerSocket(0)) {
      server.setSoTimeout(5000);
      client.connect(server.getLocalSocketAddress()).get(5, TimeUnit.SECONDS);
      try (Socket peer = server.accept()) {
        peer.setSoTimeout(5000);
        ByteBuf greeting = new Encoder(UnpooledByteBufAllocator.DEFAULT).greeting();
        try {
          peer.getOutputStream().write(ByteBufUtil.getBytes(greeting));
        } finally {
          greeting.release();
        }
        peer.getOutputStream().write(ByteBufUtil.decodeHexDump("000141"));
        Decoder.Frame frame = received.poll(5, TimeUnit.SECONDS);
        assertNotNull(frame);
        assertEquals(-1, peer.getInputStream().read());
        group.selected.submit(() -> {}).get(5, TimeUnit.SECONDS);
        assertEquals(0, frame.refCnt());
      }
    } finally {
      client.closeAsync().get(5, TimeUnit.SECONDS);
      group.shutdownGracefully(0, 5, TimeUnit.SECONDS).sync();
    }
  }

  @Test
  void dropsDisconnectedMessagesAndReusesEventLoopOnReconnect() throws Exception {
    CountingGroup group = new CountingGroup();
    ClientChannel client = new ClientChannel(group, new ClientChannelConfig(1000, 1000, 1024, 8, 1000));
    try (ServerSocket server = new ServerSocket(0)) {
      server.setSoTimeout(5000);
      ByteBuf beforeConnect = Unpooled.buffer(4).writeInt(-1);
      client.send(beforeConnect);
      assertEquals(0, beforeConnect.refCnt());
      client.connect(server.getLocalSocketAddress()).get(5, TimeUnit.SECONDS);
      // Capture the original connection so the disconnect assertion does not
      // depend on when the peer's FIN is processed.
      var channelField = ClientChannel.class.getDeclaredField("channel");
      channelField.setAccessible(true);
      Channel original = (Channel) channelField.get(client);
      try (Socket first = server.accept()) {
        first.setSoTimeout(5000);
        for (int i = 0; i < 200; i++) {
          client.send(Unpooled.buffer(4).writeInt(i));
        }
        DataInputStream input = new DataInputStream(first.getInputStream());
        for (int i = 0; i < 200; i++) {
          assertEquals(i, input.readInt());
        }
      }
      original.closeFuture().sync();
      original.eventLoop().submit(() -> {
        ByteBuf disconnected = Unpooled.buffer(4).writeInt(-2);
        client.send(disconnected);
        assertEquals(0, disconnected.refCnt());
      }).get(5, TimeUnit.SECONDS);
      try (Socket second = server.accept()) {
        second.setSoTimeout(5000);
        original.eventLoop().submit(() -> client.send(Unpooled.buffer(4).writeInt(200)))
            .get(5, TimeUnit.SECONDS);
        assertEquals(200, new DataInputStream(second.getInputStream()).readInt());
        assertEquals(1, group.selections.get(), "Reconnect must reuse the selected event loop");
        client.closeAsync().get(5, TimeUnit.SECONDS);
        assertEquals(-1, second.getInputStream().read());
        assertFalse(group.isShuttingDown(), "The supplied group belongs to the caller");
      }
    } finally {
      client.closeAsync().get(5, TimeUnit.SECONDS);
      group.shutdownGracefully(0, 5, TimeUnit.SECONDS).sync();
    }
  }

  @Test
  void concurrentSendersPreserveEachProducersOrder() throws Exception {
    CountingGroup group = new CountingGroup();
    ClientChannel client = new ClientChannel(group);
    var producers = Executors.newFixedThreadPool(4);
    try (ServerSocket server = new ServerSocket(0)) {
      server.setSoTimeout(5000);
      client.connect(server.getLocalSocketAddress()).get(5, TimeUnit.SECONDS);
      try (Socket socket = server.accept()) {
        socket.setSoTimeout(5000);
        List<java.util.concurrent.Future<?>> tasks = new ArrayList<>();
        for (int producer = 0; producer < 4; producer++) {
          int id = producer;
          tasks.add(producers.submit(() -> {
            for (int sequence = 0; sequence < 200; sequence++) {
              client.send(Unpooled.buffer(4).writeInt(id * 1000 + sequence));
            }
          }));
        }
        int[] next = new int[4];
        DataInputStream input = new DataInputStream(socket.getInputStream());
        for (int i = 0; i < 800; i++) {
          int message = input.readInt();
          assertEquals(next[message / 1000]++, message % 1000);
        }
        assertArrayEquals(new int[] {200, 200, 200, 200}, next);
        for (var task : tasks) {
          task.get(5, TimeUnit.SECONDS);
        }
      }
    } finally {
      producers.shutdown();
      assertTrue(producers.awaitTermination(5, TimeUnit.SECONDS));
      client.closeAsync().get(5, TimeUnit.SECONDS);
      group.shutdownGracefully(0, 5, TimeUnit.SECONDS).sync();
    }
  }

  @Test
  void closeAfterSendersFinishReleasesQueuedAndRejectedBuffers() throws Exception {
    CountingGroup group = new CountingGroup();
    ClientChannel client = new ClientChannel(group);
    var producers = Executors.newFixedThreadPool(4);
    List<ByteBuf> buffers = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      buffers.add(Unpooled.buffer(4).writeInt(i));
    }
    try {
      List<java.util.concurrent.Future<?>> tasks = new ArrayList<>();
      for (int producer = 0; producer < 4; producer++) {
        int offset = producer;
        tasks.add(producers.submit(() -> {
          for (int i = offset; i < buffers.size(); i += 4) {
            client.send(buffers.get(i));
          }
        }));
      }
      for (var task : tasks) {
        task.get(5, TimeUnit.SECONDS);
      }
      var closing = client.closeAsync();
      ByteBuf rejected = Unpooled.buffer(4).writeInt(1000);
      assertThrows(IllegalStateException.class, () -> client.send(rejected));
      assertEquals(0, rejected.refCnt());
      closing.get(5, TimeUnit.SECONDS);
      client.closeAsync().get(5, TimeUnit.SECONDS);
      assertTrue(buffers.stream().allMatch(buffer -> buffer.refCnt() == 0));
      assertFalse(group.isShuttingDown());
    } finally {
      producers.shutdown();
      assertTrue(producers.awaitTermination(5, TimeUnit.SECONDS));
      client.closeAsync().get(5, TimeUnit.SECONDS);
      group.shutdownGracefully(0, 5, TimeUnit.SECONDS).sync();
    }
  }

  @Test
  void externalGroupShutdownReleasesDisconnectedQueue() throws Exception {
    CountingGroup group = new CountingGroup();
    ClientChannel client = new ClientChannel(group);
    ByteBuf buffer = Unpooled.buffer(4).writeInt(1);
    try {
      client.send(buffer);
      group.shutdownGracefully(0, 5, TimeUnit.SECONDS).sync();
      client.closeAsync().get(5, TimeUnit.SECONDS);
      assertEquals(0, buffer.refCnt());
      ByteBuf rejected = Unpooled.buffer(4).writeInt(2);
      assertThrows(IllegalStateException.class, () -> client.send(rejected));
      assertEquals(0, rejected.refCnt());
    } finally {
      group.shutdownGracefully(0, 5, TimeUnit.SECONDS).sync();
    }
  }

  private static final class CountingGroup extends MultiThreadIoEventLoopGroup {
    private final AtomicInteger selections = new AtomicInteger();
    private IoEventLoop selected;

    CountingGroup() {
      super(2, NioIoHandler.newFactory());
    }

    @Override
    public IoEventLoop next() {
      selections.incrementAndGet();
      selected = super.next();
      return selected;
    }
  }
}
