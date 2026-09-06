package me.wineway.onigiri.publisher;

import static org.junit.jupiter.api.Assertions.*;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.ReferenceCountUtil;
import me.wineway.onigiri.protocol.Command;
import me.wineway.onigiri.protocol.Decoder;
import me.wineway.onigiri.protocol.Encoder;
import me.wineway.onigiri.transport.InboundTimeoutHandler;

class PublisherSessionTest {
  @Test
  void repeatedWireSubscriptionsRequireBalancedCancellationAndDisconnectClearsState() {
    try (var peer = new Peer()) {
      peer.ready("SUB");
      peer.command(Command.SUBSCRIBE, 65);
      peer.command(Command.SUBSCRIBE, 65);
      peer.command(Command.CANCEL, 65);
      peer.publish(65);
      assertEquals(202, peer.takeWire().readableBytes());
      peer.releaseTaken();
      peer.command(Command.CANCEL, 65);
      peer.command(Command.CANCEL, 65);
      peer.publish(65);
      assertNull(peer.channel.readOutbound());
      peer.command(Command.SUBSCRIBE, 66);
      peer.channel.close();
      assertFalse(peer.lane.hasActiveSessions());
      assertEquals(1, peer.disconnected);
      assertTrue(peer.errors.isEmpty());
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"early-ready", "early-subscribe", "duplicate-ready", "wrong-type",
      "data", "ping-short", "ping-long", "pong-long", "unknown", "metadata", "error"})
  void rejectsInvalidProtocolAndReportsOnlyOneError(String scenario) {
    try (var peer = new Peer()) {
      switch (scenario) {
        case "early-ready" -> peer.channel.writeInbound(peer.encoder.ready(Map.of("Socket-Type", new byte[64])));
        case "early-subscribe" -> {
          peer.channel.writeInbound(peer.encoder.greeting());
          peer.command(Command.SUBSCRIBE, 65);
        }
        case "wrong-type" -> peer.ready("PUB");
        case "metadata" -> {
          peer.channel.writeInbound(peer.encoder.greeting());
          peer.command(Command.READY, 99);
        }
        default -> {
          peer.ready("XSUB");
          switch (scenario) {
            case "duplicate-ready" -> peer.channel.writeInbound(peer.encoder.ready(Map.of()));
            case "data" -> peer.channel.writeInbound(peer.encoder.message(Unpooled.EMPTY_BUFFER, false));
            case "ping-short" -> peer.command(Command.PING, 0);
            case "ping-long" -> peer.command(Command.PING, new int[19]);
            case "pong-long" -> peer.command(Command.PONG, new int[17]);
            case "unknown" -> peer.channel.writeInbound(peer.encoder.command("EXTENSION", Unpooled.EMPTY_BUFFER));
            case "error" -> peer.command(Command.ERROR, 1, 65);
          }
        }
      }
      assertFalse(peer.channel.isActive());
      assertEquals(1, peer.errors.size());
      assertEquals(1, peer.disconnected);
      assertFalse(peer.lane.hasActiveSessions());
      peer.channel.advanceTimeBy(1, TimeUnit.DAYS);
      peer.channel.runScheduledPendingTasks();
      assertEquals(1, peer.errors.size(), "Closed session timers must be cancelled");
    }
  }

  @Test
  void handshakeAndHeartbeatTimeoutsCloseAndReleaseSession() {
    try (var peer = new Peer()) {
      peer.channel.advanceTimeBy(3, TimeUnit.SECONDS);
      peer.channel.runScheduledPendingTasks();
      assertFalse(peer.channel.isActive());
      assertEquals(1, peer.errors.size());
    }
    try (var peer = new Peer()) {
      peer.ready("SUB");
      peer.channel.advanceTimeBy(100, TimeUnit.MILLISECONDS);
      peer.channel.runScheduledPendingTasks();
      peer.channel.advanceTimeBy(3, TimeUnit.SECONDS);
      peer.channel.runScheduledPendingTasks();
      assertFalse(peer.channel.isActive());
      assertEquals(1, peer.errors.size());
    }
  }

  @Test
  void pingEchoesBinaryContext() {
    try (var peer = new Peer()) {
      peer.ready("SUB");
      peer.command(Command.PING, 0, 1, 0, 255, 65);
      var decoded = new EmbeddedChannel(new Decoder(1024, false));
      try {
        decoded.writeInbound((Object) peer.channel.readOutbound());
        Decoder.Frame pong = decoded.readInbound();
        assertEquals(Command.PONG, pong.commandType());
        byte[] context = new byte[3];
        pong.content().readBytes(context);
        assertArrayEquals(new byte[] {0, (byte) 255, 65}, context);
        pong.release();
      } finally {
        decoded.finishAndReleaseAll();
      }
      // InboundTimeoutHandler uses real monotonic time for TTL; heartbeat timeout
      // coverage above uses EmbeddedEventLoop's deterministic scheduler.
      assertTrue(peer.channel.isActive());
    }
  }

  @Test
  void subscriptionLimitsRejectPeer() {
    try (var peer = new Peer()) {
      peer.ready("SUB");
      peer.command(Command.SUBSCRIBE, new int[5]);
      assertFalse(peer.channel.isActive());
      assertEquals(1, peer.errors.size());
    }
    try (var peer = new Peer()) {
      peer.ready("SUB");
      peer.command(Command.SUBSCRIBE, 65);
      peer.command(Command.SUBSCRIBE, 66);
      peer.command(Command.SUBSCRIBE, 67);
      assertFalse(peer.channel.isActive());
      assertEquals(1, peer.errors.size());
    }
  }

  @Test
  void notifiesEveryValidSubscribeButNotCancelOrRejectedCommand() {
    try (var peer = new Peer()) {
      peer.ready("SUB");
      peer.command(Command.SUBSCRIBE, 65);
      peer.command(Command.SUBSCRIBE, 65);
      peer.command(Command.CANCEL, 65);
      peer.command(Command.CANCEL, 65);
      peer.command(Command.SUBSCRIBE);
      assertEquals(3, peer.notices.size());
      assertArrayEquals(new byte[] {65}, peer.notices.get(0));
      assertArrayEquals(new byte[] {65}, peer.notices.get(1));
      assertArrayEquals(new byte[0], peer.notices.get(2));
      peer.command(Command.SUBSCRIBE, new int[5]);
      assertEquals(3, peer.notices.size());
    }
  }

  @Test
  void directedMessagesDoNotRequireMatchingPublisherRouteAndReleaseInvalidInput() {
    try (var peer = new Peer()) {
      peer.ready("SUB");
      ByteBuf topic = Unpooled.buffer().writeByte(65);
      ByteBuf payload = Unpooled.buffer(256).writeZero(256);
      peer.session.send(topic, payload);
      assertEquals(0, topic.refCnt()); assertEquals(0, payload.refCnt());
      peer.channel.runPendingTasks();
      ByteBuf wire = peer.takeWire();
      assertEquals(268, wire.readableBytes());
      assertEquals(1, wire.getByte(0)); assertEquals(1, wire.getByte(1));
      assertEquals(65, wire.getByte(2)); assertEquals(2, wire.getByte(3));
      assertEquals(256, wire.getLong(4));
      peer.releaseTaken();
      ByteBuf invalid = Unpooled.buffer().writeByte(1);
      assertThrows(NullPointerException.class, () -> peer.session.send(null, invalid));
      assertEquals(0, invalid.refCnt());
      ByteBuf empty = Unpooled.buffer(1);
      assertThrows(IllegalArgumentException.class, () -> peer.session.send(empty));
      assertEquals(0, empty.refCnt());
    }
  }

  @Test
  void queuedDirectedMessageDropsWhenTargetClosesAndUnwritableDoesNotQueueRetry() {
    try (var peer = new Peer()) {
      peer.ready("SUB");
      peer.channel.unsafe().outboundBuffer().setUserDefinedWritability(1, false);
      peer.session.send(Unpooled.buffer().writeByte(65));
      peer.channel.runPendingTasks();
      assertNull(peer.channel.readOutbound());
      peer.channel.unsafe().outboundBuffer().setUserDefinedWritability(1, true);
      peer.session.send(Unpooled.buffer().writeByte(66));
      // EmbeddedChannel.close() runs pending tasks before closing; deliver the
      // disconnect lifecycle first to test a message still queued at disconnect.
      peer.channel.pipeline().fireChannelInactive();
      peer.channel.close();
      peer.channel.runPendingTasks();
      assertNull(peer.channel.readOutbound());
      ByteBuf closed = Unpooled.buffer().writeByte(67);
      peer.session.send(closed);
      assertEquals(0, closed.refCnt());
    }
  }

  private static final class Peer implements AutoCloseable, Session.Listener {
    final EmbeddedChannel channel = new EmbeddedChannel();
    final Encoder encoder = new Encoder(UnpooledByteBufAllocator.DEFAULT);
    final List<Throwable> errors = new ArrayList<>();
    final PublishLane lane;
    final Session session;
    final List<byte[]> notices = new ArrayList<>();
    int disconnected;
    ByteBuf taken;

    Peer() {
      var config = new PubServerConfig(2, 1, 2, 1, 2000, 100, 2000, 2000, 2, 4);
      // EmbeddedEventLoop has no terminationFuture; supply one for the lane lifecycle hook.
      var termination = new io.netty.util.concurrent.DefaultPromise<Void>(
          io.netty.util.concurrent.GlobalEventExecutor.INSTANCE);
      var loop = (io.netty.channel.EventLoop) java.lang.reflect.Proxy.newProxyInstance(
          getClass().getClassLoader(), new Class<?>[] {io.netty.channel.EventLoop.class},
          (proxy, method, args) -> {
            if (method.getName().equals("terminationFuture")) return termination;
            try { return method.invoke(channel.eventLoop(), args); }
            catch (java.lang.reflect.InvocationTargetException failure) { throw failure.getCause(); }
          });
      lane = new PublishLane(loop, config, new LongAdder());
      session = new Session(lane, config, this);
      channel.pipeline().addLast(new InboundTimeoutHandler(), new Decoder(), session);
      channel.pipeline().fireChannelActive();
      discardOutbound();
    }

    void ready(String type) {
      channel.writeInbound(encoder.greeting());
      channel.writeInbound(encoder.ready(Map.of("Socket-Type", type.getBytes(java.nio.charset.StandardCharsets.US_ASCII))));
      discardOutbound();
    }

    void command(Command command, int... bytes) {
      ByteBuf data = Unpooled.buffer(bytes.length);
      try {
        for (int b : bytes) data.writeByte(b);
        channel.writeInbound(encoder.command(command, data));
      } finally {
        data.release();
      }
    }

    void publish(int first) {
      ByteBuf wire = Unpooled.directBuffer(202).writeZero(202);
      wire.setByte(2, first);
      lane.submit(new EncodedBatch(wire, 1, new int[] {0, 202}, new int[] {2}, new int[] {200}));
      channel.runPendingTasks();
    }

    ByteBuf takeWire() { return taken = channel.readOutbound(); }
    void releaseTaken() { taken.release(); taken = null; }
    void discardOutbound() {
      Object output;
      while ((output = channel.readOutbound()) != null) ReferenceCountUtil.release(output);
    }
    public void onReady(Session session) {}
    public void onSubscription(Session session, Prefix prefix) { notices.add(prefix.copyBytes()); }
    public void onDisconnected(Session session, boolean active) { disconnected++; }
    public void onError(Session session, Throwable failure) { errors.add(failure); }
    public void close() {
      ReferenceCountUtil.release(taken);
      lane.closeAsync();
      channel.runPendingTasks();
      channel.finishAndReleaseAll();
    }
  }
}
