package me.wineway.onigiri.publisher;

import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.ScheduledFuture;
import me.wineway.onigiri.protocol.Command;
import me.wineway.onigiri.protocol.Decoder;
import me.wineway.onigiri.protocol.Encoder;
import me.wineway.onigiri.transport.InboundTimeoutHandler;

/** Protocol and subscription state for one accepted subscriber connection. */
final class Session extends ChannelDuplexHandler implements SubscriberSink, Connection {
  interface Listener {
    void onReady(Session session);
    default void onSubscription(Session session, Prefix prefix) {}
    void onDisconnected(Session session, boolean wasActive);
    void onError(Session session, Throwable failure);
  }

  private enum State { Greeting, Ready, Active, Closed }

  private final PublishLane lane;
  private final PubServerConfig config;
  private final Listener listener;
  private final Encoder encoder = new Encoder(ByteBufAllocator.DEFAULT);
  private final Map<Prefix, Integer> subscriptions = new LinkedHashMap<>();
  private State state = State.Greeting;
  private volatile boolean directOpen;
  private Channel channel;
  private int slot = -1;
  private boolean errorReported;
  private boolean disconnectedReported;
  private ScheduledFuture<?> handshakeTimer;
  private ScheduledFuture<?> heartbeatTimer;

  Session(PublishLane lane, PubServerConfig config, Listener listener) {
    this.lane = lane;
    this.config = config;
    this.listener = listener;
  }

  Channel channel() {
    return channel;
  }

  @Override
  public SocketAddress remoteAddress() {
    return channel.remoteAddress();
  }

  @Override
  public void send(ByteBuf message) { sendMessage(message, null, false); }

  @Override
  public void send(ByteBuf topic, ByteBuf payload) { sendMessage(topic, payload, true); }

  private void sendMessage(ByteBuf first, ByteBuf second, boolean multipart) {
    ByteBuf wire = null;
    try {
      java.util.Objects.requireNonNull(first, "message/topic");
      if (multipart) java.util.Objects.requireNonNull(second, "payload");
      int firstSize = first.readableBytes();
      int secondSize = multipart ? second.readableBytes() : 0;
      long size = (long) firstSize + secondSize;
      long wireSize = size + (firstSize > 255 ? 9 : 2)
          + (multipart ? (secondSize > 255 ? 9 : 2) : 0);
      if ((!multipart && firstSize == 0) || size > config.maxMessageBytes()
          || firstSize > Decoder.DEFAULT_MAX_FRAME_SIZE || secondSize > Decoder.DEFAULT_MAX_FRAME_SIZE
          || wireSize > config.maxBatchBytes()) {
        throw new IllegalArgumentException("Message exceeds limits or single frame is empty");
      }
      if (!directOpen) return;
      wire = io.netty.buffer.PooledByteBufAllocator.DEFAULT.directBuffer((int) wireSize, (int) wireSize);
      Encoder.appendMessage(wire, first, multipart);
      if (multipart) Encoder.appendMessage(wire, second, false);
      EncodedBatch batch = new EncodedBatch(wire, this);
      wire = null;
      lane.submit(batch);
    } finally {
      ReferenceCountUtil.release(wire);
      ReferenceCountUtil.release(first);
      if (multipart) ReferenceCountUtil.release(second);
    }
  }

  @Override
  public ByteBufAllocator allocator() {
    return channel.alloc();
  }

  @Override
  public boolean isWritable() {
    return state == State.Active && channel.isActive() && channel.isWritable();
  }

  @Override
  public boolean write(ByteBuf wire) {
    if (!isWritable()) {
      wire.release();
      return false;
    }
    try {
      channel.write(wire, channel.voidPromise());
      return true;
    } catch (RuntimeException failure) {
      ReferenceCountUtil.safeRelease(wire);
      channel.pipeline().fireExceptionCaught(failure);
      return false;
    }
  }

  @Override
  public void flush() {
    if (channel.isActive()) channel.flush();
  }

  @Override
  public void handlerAdded(ChannelHandlerContext ctx) {
    channel = ctx.channel();
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) {
    slot = lane.register(this);
    if (slot < 0) {
      fail(new IllegalStateException("Publisher lane subscriber limit reached"));
      return;
    }
    if (config.handshakeTimeoutMillis() > 0) {
      handshakeTimer = ctx.executor().schedule(() -> fail(new TimeoutException("Handshake timed out")),
          config.handshakeTimeoutMillis(), TimeUnit.MILLISECONDS);
    }
    ctx.writeAndFlush(encoder.greeting(), ctx.voidPromise());
    ctx.fireChannelActive();
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object message) {
    try {
      if (message instanceof Decoder.Greeting greeting) receiveGreeting(greeting);
      else if (message instanceof Decoder.Frame frame) receive(frame);
      else throw new CorruptedFrameException("Unexpected inbound message");
    } finally {
      ReferenceCountUtil.release(message);
    }
  }

  private void receiveGreeting(Decoder.Greeting greeting) {
    require(State.Greeting);
    state = State.Ready;
    channel.writeAndFlush(
        encoder.ready(Map.of("Socket-Type", new byte[] {'P', 'U', 'B'})), channel.voidPromise());
  }

  private void receive(Decoder.Frame frame) {
    Command command = frame.commandType();
    if (command == Command.ERROR) {
      fail(new CorruptedFrameException("Subscriber rejected handshake: " + errorReason(frame.content())));
      return;
    }
    if (command == Command.READY) {
      require(State.Ready);
      byte[] type = Decoder.metadata(frame.content()).get("socket-type");
      String peerType = type == null ? "" : new String(type, StandardCharsets.US_ASCII);
      if (!peerType.equals("SUB") && !peerType.equals("XSUB")) {
        throw new CorruptedFrameException("Expected SUB or XSUB peer");
      }
      state = State.Active;
      directOpen = true;
      cancel(handshakeTimer);
      handshakeTimer = null;
      lane.activate(slot);
      if (config.heartbeatIntervalMillis() > 0) {
        heartbeatTimer = channel.eventLoop().scheduleWithFixedDelay(this::heartbeat,
            config.heartbeatIntervalMillis(), config.heartbeatIntervalMillis(), TimeUnit.MILLISECONDS);
      }
      listener.onReady(this);
      return;
    }
    require(State.Active);
    if (command == Command.SUBSCRIBE || command == Command.CANCEL) {
      if (frame.content().readableBytes() > config.maxSubscriptionPrefixBytes()) {
        throw new CorruptedFrameException("Subscription prefix limit exceeded");
      }
      Prefix prefix = Prefix.copyOf(frame.content());
      int count = subscriptions.getOrDefault(prefix, 0);
      if (command == Command.SUBSCRIBE) {
        if (count == Integer.MAX_VALUE || (count == 0
            && subscriptions.size() >= config.maxSubscriptionsPerSession())) {
          throw new CorruptedFrameException("Subscription count limit exceeded");
        }
        subscriptions.put(prefix, count + 1);
        if (count == 0) lane.updateSubscription(slot, prefix, true);
        listener.onSubscription(this, prefix);
      } else if (count == 1) {
        subscriptions.remove(prefix);
        lane.updateSubscription(slot, prefix, false);
      } else if (count > 1) {
        subscriptions.put(prefix, count - 1);
      }
    } else if (command == Command.PING) {
      ByteBuf data = frame.content();
      if (data.readableBytes() < 2 || data.readableBytes() > 18) {
        throw new CorruptedFrameException("Invalid PING");
      }
      inboundTimeout().setIdleTimeout(data.getUnsignedShort(data.readerIndex()) * 100);
      channel.writeAndFlush(encoder.command(Command.PONG,
          data.slice(data.readerIndex() + 2, data.readableBytes() - 2)), channel.voidPromise());
    } else if (command != Command.PONG || frame.content().readableBytes() > 16) {
      throw new CorruptedFrameException("Unexpected command: " + command);
    }
  }

  private String errorReason(ByteBuf content) {
    if (!content.isReadable()
        || content.getUnsignedByte(content.readerIndex()) != content.readableBytes() - 1) {
      return "Malformed ERROR";
    }
    return content.toString(content.readerIndex() + 1, content.readableBytes() - 1,
        StandardCharsets.US_ASCII);
  }

  private void heartbeat() {
    if (state != State.Active) return;
    try {
      ByteBuf data = Unpooled.buffer(2).writeShort(config.heartbeatTtlMillis() / 100);
      try {
        channel.writeAndFlush(encoder.command(Command.PING, data), channel.voidPromise());
      } finally {
        data.release();
      }
      inboundTimeout().expectInbound(config.heartbeatTimeoutMillis());
    } catch (RuntimeException failure) {
      fail(failure);
    }
  }

  private InboundTimeoutHandler inboundTimeout() {
    return channel.pipeline().get(InboundTimeoutHandler.class);
  }

  private void require(State expected) {
    if (state != expected) {
      throw new CorruptedFrameException("Expected " + expected + ", got " + state);
    }
  }

  @Override
  public void fail(Throwable failure) {
    directOpen = false;
    try {
      if (!errorReported) {
        errorReported = true;
        listener.onError(this, failure);
      }
    } finally {
      channel.close();
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable failure) {
    fail(failure);
  }

  @Override
  public void channelWritabilityChanged(ChannelHandlerContext ctx) {
    if (slot >= 0) lane.updateWritable(slot, isWritable());
    ctx.fireChannelWritabilityChanged();
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) {
    boolean active = state == State.Active;
    state = State.Closed;
    directOpen = false;
    cancel(handshakeTimer);
    cancel(heartbeatTimer);
    handshakeTimer = heartbeatTimer = null;
    lane.unregister(slot, subscriptions.keySet(), active);
    subscriptions.clear();
    if (!disconnectedReported) {
      disconnectedReported = true;
      listener.onDisconnected(this, active);
    }
    ctx.fireChannelInactive();
  }

  private static void cancel(ScheduledFuture<?> timer) {
    if (timer != null) timer.cancel(false);
  }
}
