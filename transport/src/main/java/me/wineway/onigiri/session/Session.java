package me.wineway.onigiri.session;

import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.EventLoop;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.util.concurrent.ScheduledFuture;
import me.wineway.onigiri.protocol.Command;
import me.wineway.onigiri.protocol.Decoder;
import me.wineway.onigiri.protocol.Encoder;
import me.wineway.onigiri.transport.ClientChannel;
import me.wineway.onigiri.transport.ClientChannelConfig;

/** Protocol state for one publisher endpoint across TCP reconnects. */
final class Session implements AutoCloseable {
  interface Listener {
    void restoreSubscriptions(Session session);
    void onReady(Session session);
    void onDisconnected(Session session);
    void onError(Session session, Throwable failure);
    void onFrame(Session session, Decoder.Frame frame);
    void onClosed(Session session);
    default boolean completeMessages() { return false; }
    default void onMultipart(Session session, MultipartView message) { throw new UnsupportedOperationException(); }
    default boolean borrowedMessages() { return false; }
    default void onMessage(Session session, ByteBuf data, int index, int length, boolean more) {
      throw new UnsupportedOperationException();
    }
  }

  private enum State { Disconnected, Greeting, Ready, Active, Closed }

  private final ClientChannel transport;
  private final Encoder encoder = new Encoder(ByteBufAllocator.DEFAULT);
  private final SubClientConfig config;
  private final Listener listener;
  private final AtomicBoolean closing = new AtomicBoolean();
  private final AtomicBoolean closedNotified = new AtomicBoolean();
  private volatile State state = State.Disconnected;
  private final MultipartAssembler multipart;
  private ScheduledFuture<?> multipartTimer;
  private ScheduledFuture<?> handshakeTimer;
  private ScheduledFuture<?> heartbeatTimer;

  Session(EventLoop eventLoop, ClientChannelConfig transportConfig,
      SubClientConfig config, Listener listener) {
    this.config = Objects.requireNonNull(config, "config");
    this.listener = Objects.requireNonNull(listener, "listener");
    multipart = new MultipartAssembler(config, listener.completeMessages());
    transport = new ClientChannel(eventLoop, transportConfig, new ClientChannel.Listener() {
      @Override
      public void onError(Throwable failure) {
        cancelTimers();
        multipart.reset();
        state = closing.get() ? State.Closed : State.Disconnected;
        Session.this.listener.onError(Session.this, failure);
      }

      @Override
      public void onConnected() {
        if (closing.get()) return;
        cancelTimers();
        state = State.Greeting;
        if (config.handshakeTimeoutMillis() > 0) {
          handshakeTimer = transport.eventLoop().schedule(
              () -> timeout("Handshake"), config.handshakeTimeoutMillis(), TimeUnit.MILLISECONDS);
        }
        transport.send(encoder.greeting());
      }

      @Override
      public void onGreeting(Decoder.Greeting greeting) {
        if (closing.get()) return;
        require(State.Greeting);
        state = State.Ready;
        transport.send(encoder.ready(Map.of("Socket-Type", new byte[] {'S', 'U', 'B'})));
      }

      @Override
      public void onFrame(Decoder.Frame frame) {
        if (!closing.get()) receive(frame);
      }

      @Override
      public void onDisconnected() {
        cancelTimers();
        state = closing.get() ? State.Closed : State.Disconnected;
        try {
          if (!closing.get()) multipart.endOfInput();
          else multipart.reset();
        } catch (CorruptedFrameException failure) {
          Session.this.listener.onError(Session.this, failure);
        } finally {
          Session.this.listener.onDisconnected(Session.this);
        }
      }
    }, listener.borrowedMessages() ? this::receiveMessage : null);
    transport.eventLoop().terminationFuture().addListener(ignored -> {
      closing.set(true);
      cancelTimers();
      multipart.reset();
      state = State.Closed;
      notifyClosed();
    });
  }

  CompletableFuture<Void> connect(SocketAddress address) {
    if (closing.get()) {
      return CompletableFuture.failedFuture(new IllegalStateException("Session is closed"));
    }
    return transport.connect(address);
  }

  boolean isReady() {
    return !closing.get() && state == State.Active;
  }

  EventLoop eventLoop() {
    return transport.eventLoop();
  }

  void fail(Throwable failure) {
    transport.disconnect(failure);
  }

  void sendSubscription(ByteBuffer key, boolean subscribe) {
    if (!eventLoop().inEventLoop()) {
      throw new IllegalStateException("Subscription send must run on the event loop");
    }
    if (state != State.Active && state != State.Ready) return;
    ByteBuf prefix = Unpooled.wrappedBuffer(key.duplicate());
    try {
      transport.send(subscribe ? encoder.subscribe(prefix) : encoder.cancel(prefix));
    } finally {
      prefix.release();
    }
  }

  private void receiveMessage(ByteBuf data, int index, int length, boolean more) {
    if (closing.get()) return;
    require(State.Active);
    MultipartView complete = multipart.accept(data, index, length, more);
    if (listener.completeMessages()) {
      if (complete != null) {
        try { listener.onMultipart(this, complete); }
        finally { multipart.delivered(); }
      }
    } else listener.onMessage(this, data, index, length, more);
  }

  private void receive(Decoder.Frame frame) {
    Command command = frame.commandType();
    if (command == Command.ERROR) {
      ByteBuf reason = frame.content();
      String detail = "Malformed ERROR";
      if (reason.isReadable() && reason.getUnsignedByte(reason.readerIndex()) == reason.readableBytes() - 1) {
        detail = reason.toString(reason.readerIndex() + 1, reason.readableBytes() - 1,
            StandardCharsets.US_ASCII);
      }
      closeAsync();
      listener.onError(this, new CorruptedFrameException("Peer rejected handshake: " + detail));
      return;
    }
    if (command == Command.READY) {
      require(State.Ready);
      byte[] type = Decoder.metadata(frame.content()).get("socket-type");
      String peerType = type == null ? "" : new String(type, StandardCharsets.US_ASCII);
      if (!peerType.equals("PUB") && !peerType.equals("XPUB")) {
        throw new CorruptedFrameException("Expected PUB or XPUB peer");
      }
      listener.restoreSubscriptions(this);
      state = State.Active;
      cancel(handshakeTimer);
      handshakeTimer = null;
      if (config.heartbeatIntervalMillis() > 0) {
        heartbeatTimer = eventLoop().scheduleWithFixedDelay(this::heartbeat,
            config.heartbeatIntervalMillis(), config.heartbeatIntervalMillis(), TimeUnit.MILLISECONDS);
      }
      if (config.multipartTimeoutMillis() > 0) {
        multipartTimer = eventLoop().scheduleWithFixedDelay(() -> {
          if (multipart.timedOut(System.nanoTime())) {
            multipart.reset();
            transport.disconnect(new TimeoutException("Multipart message timed out"));
          }
        }, config.multipartTimeoutMillis(), Math.max(1, config.multipartTimeoutMillis() / 2),
            TimeUnit.MILLISECONDS);
      }
      listener.onReady(this);
      return;
    }
    require(State.Active);
    if (command == null) {
      multipart.accept(frame.content(), frame.content().readerIndex(),
          frame.content().readableBytes(), frame.more());
      listener.onFrame(this, frame);
    } else if (command == Command.PING) {
      ByteBuf data = frame.content();
      if (data.readableBytes() < 2 || data.readableBytes() > 18) {
        throw new CorruptedFrameException("Invalid PING");
      }
      transport.setInboundIdleTimeout(data.getUnsignedShort(data.readerIndex()) * 100);
      transport.send(encoder.command(Command.PONG,
          data.slice(data.readerIndex() + 2, data.readableBytes() - 2)));
    } else if (command != Command.PONG || frame.content().readableBytes() > 16) {
      throw new CorruptedFrameException("Unexpected command: " + command);
    }
  }

  private void heartbeat() {
    if (closing.get() || state != State.Active) return;
    try {
      ByteBuf data = Unpooled.buffer(2).writeShort(config.heartbeatTtlMillis() / 100);
      try {
        transport.send(encoder.command(Command.PING, data));
      } finally {
        data.release();
      }
      transport.expectInbound(config.heartbeatTimeoutMillis());
    } catch (RuntimeException failure) {
      transport.disconnect(failure);
    }
  }

  private void timeout(String phase) {
    if (!closing.get() && state != State.Disconnected && state != State.Closed) {
      transport.disconnect(new TimeoutException(phase + " timed out"));
    }
  }

  private void require(State expected) {
    if (state != expected) {
      throw new CorruptedFrameException("Expected " + expected + ", got " + state);
    }
  }

  private void cancelTimers() {
    cancel(multipartTimer);
    multipartTimer = null;
    cancel(handshakeTimer);
    cancel(heartbeatTimer);
    handshakeTimer = heartbeatTimer = null;
  }

  private static void cancel(ScheduledFuture<?> timer) {
    if (timer != null) timer.cancel(false);
  }

  CompletableFuture<Void> closeAsync() {
    if (closing.compareAndSet(false, true)) {
      if (eventLoop().inEventLoop()) {
        cancelTimers();
        multipart.reset();
        state = State.Closed;
      } else {
        try {
          eventLoop().execute(() -> {
            cancelTimers();
            multipart.reset();
            state = State.Closed;
          });
        } catch (RuntimeException ignored) {
          // ClientChannel's termination hook completes transport cleanup.
        }
      }
    }
    CompletableFuture<Void> result = transport.closeAsync();
    result.whenComplete((ignored, failure) -> notifyClosed());
    return result;
  }

  private void notifyClosed() {
    if (closedNotified.compareAndSet(false, true)) listener.onClosed(this);
  }

  @Override
  public void close() {
    closeAsync();
  }
}
