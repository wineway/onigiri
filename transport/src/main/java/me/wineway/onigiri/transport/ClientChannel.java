package me.wineway.onigiri.transport;

import java.net.SocketAddress;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jctools.queues.MpscLinkedQueue;

import me.wineway.onigiri.protocol.Decoder;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.ScheduledFuture;
import io.netty.util.internal.shaded.org.jctools.queues.MpscArrayQueue;

/**
 * A reconnecting TCP client. The caller owns the supplied NIO event loop group.
 * Stop producers and wait for all send() calls to return before closing this
 * client
 * or shutting down its event loop group. Concurrent send and close are
 * unsupported.
 */
public class ClientChannel implements AutoCloseable {
  /**
   * Inbound protocol callbacks run on the client's event loop and must not block.
   * Frames are borrowed until onFrame returns (also if it throws). Retain a frame
   * before keeping it for asynchronous processing, and release that reference later.
   * Handshake policy and multipart assembly belong to the listener/session.
   */
  public interface Listener {
    /** Errors are reported on the event loop. Throwing here is logged, not retried. */
    default void onError(Throwable failure) {
    }

    default void onConnected() {
    }

    default void onDisconnected() {
    }

    default void onGreeting(Decoder.Greeting greeting) {
    }

    default void onFrame(Decoder.Frame frame) {
    }
  }

  private final ClientChannelConfig config;
  private final Listener listener;
  private final EventLoop eventLoop;
  private final Bootstrap bootstrap;
  private final MpscArrayQueue<ByteBuf> outboundBuffer;
  private final AtomicBoolean drainRequested = new AtomicBoolean();
  private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();

  private volatile boolean closeRequested;
  private volatile Channel channel;

  // Access only on eventLoop.
  private ChannelState channelState = ChannelState.Inactive;
  private long reconnectBackoffMillis;
  private SocketAddress peerAddress;
  private ScheduledFuture<?> reconnectTask;

  public ClientChannel(EventLoopGroup eventLoopGroup) {
    this(eventLoopGroup, new ClientChannelConfig());
  }

  public ClientChannel(EventLoopGroup eventLoopGroup, ClientChannelConfig config) {
    this(eventLoopGroup, config, new Listener() {});
  }

  public ClientChannel(EventLoopGroup eventLoopGroup, ClientChannelConfig config, Listener listener) {
    this(eventLoopGroup, config, listener, null);
  }

  /** Data consumer runs synchronously on the receive loop under Decoder's borrowing contract. */
  public ClientChannel(EventLoopGroup eventLoopGroup, ClientChannelConfig config, Listener listener,
      Decoder.DataConsumer dataConsumer) {
    this.config = Objects.requireNonNull(config, "config");
    this.listener = Objects.requireNonNull(listener, "listener");
    this.eventLoop = Objects.requireNonNull(eventLoopGroup, "eventLoopGroup").next();
    this.bootstrap = new Bootstrap()
        .group(eventLoop)
        .channel(NioSocketChannel.class)
        .option(ChannelOption.TCP_NODELAY, true)
        .option(ChannelOption.SO_KEEPALIVE, true)
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.connectTimeoutMillis())
        .handler(new ChannelInitializer<SocketChannel>() {
          @Override
          protected void initChannel(SocketChannel socketChannel) {
            socketChannel.pipeline().addLast("inboundTimeout", new InboundTimeoutHandler());
            socketChannel.pipeline().addLast("decoder", new Decoder(Decoder.DEFAULT_MAX_FRAME_SIZE, true, dataConsumer));
            socketChannel.pipeline().addLast("client", new ClientHandler());
          }
        });
    this.outboundBuffer = new MpscArrayQueue<ByteBuf>(config.outboundBufferMaxLength());
    // If the owner shuts down the group first, cleanup waits until the consumer has
    // stopped.
    eventLoop.terminationFuture().addListener(future -> {
      closeRequested = true;
      releaseOutbound();
      closeFuture.complete(null);
    });
  }

  enum ChannelState {
    Inactive,
    Connecting,
    Establish,
    Closed,
  }

  /** Executor for session state and callbacks. The caller still owns its group. */
  public EventLoop eventLoop() {
    return eventLoop;
  }

  /** Fails only the current connection; normal reconnect policy remains in effect. */
  public void disconnect(Throwable failure) {
    Objects.requireNonNull(failure, "failure");
    if (!eventLoop.inEventLoop()) {
      throw new IllegalStateException("disconnect must run on the event loop");
    }
    if (!closeRequested && channel != null && channel.isActive()) {
      channel.pipeline().fireExceptionCaught(failure);
    }
  }

  /** Arms a one-shot inbound response deadline; additional probes do not extend it. */
  public void expectInbound(int timeoutMillis) {
    InboundTimeoutHandler handler = inboundTimeoutHandler(timeoutMillis);
    if (handler != null) {
      handler.expectInbound(timeoutMillis);
    }
  }

  /** Sets a recurring inbound idle limit; any bytes refresh it. Zero disables it. */
  public void setInboundIdleTimeout(int timeoutMillis) {
    InboundTimeoutHandler handler = inboundTimeoutHandler(timeoutMillis);
    if (handler != null) {
      handler.setIdleTimeout(timeoutMillis);
    }
  }

  private InboundTimeoutHandler inboundTimeoutHandler(int timeoutMillis) {
    if (!eventLoop.inEventLoop()) {
      throw new IllegalStateException("Timeout configuration must run on the event loop");
    }
    if (timeoutMillis < 0) {
      throw new IllegalArgumentException("Timeout must be non-negative");
    }
    return closeRequested || channel == null || !channel.isActive()
        ? null : channel.pipeline().get(InboundTimeoutHandler.class);
  }

  private void reportError(Throwable failure) {
    try {
      listener.onError(failure);
    } catch (Throwable callbackFailure) {
      System.getLogger(ClientChannel.class.getName()).log(System.Logger.Level.WARNING,
          "Error callback failed", callbackFailure);
    }
  }

  /**
   * Starts connecting once. The future reports the first attempt; failures keep
   * retrying
   * in the background until closeAsync() is called.
   */
  public CompletableFuture<Void> connect(SocketAddress address) {
    Objects.requireNonNull(address, "address");
    CompletableFuture<Void> result = new CompletableFuture<>();
    if (closeRequested) {
      return CompletableFuture.failedFuture(new IllegalStateException("Client is closed"));
    }
    try {
      eventLoop.execute(() -> {
        if (peerAddress != null || closeRequested) {
          result.completeExceptionally(new IllegalStateException("Client already started or closed"));
          return;
        }
        peerAddress = address;
        connectOnEventLoop().addListener(future -> {
          if (future.isSuccess()) {
            result.complete(null);
          } else {
            result.completeExceptionally(future.cause());
          }
        });
      });
    } catch (RejectedExecutionException failure) {
      result.completeExceptionally(failure);
    }
    return result;
  }

  /**
   * Sends already encoded wire bytes and transfers ownership of buf, including
   * when rejected. Supports concurrent
   * senders,
   * but all send() calls must return before client close or event loop group
   * shutdown.
   * Messages sent without an active connection are discarded and released.
   * A send racing with reconnect may be delivered on the new connection.
   * This does not acknowledge remote delivery.
   */
  public void send(ByteBuf buf) {
    Objects.requireNonNull(buf, "buf");
    if (closeRequested) {
      buf.release();
      throw new IllegalStateException("Client is closed");
    }
    Channel target = channel;
    if (target == null || !target.isActive()) {
      buf.release();
      return;
    }
    if (!outboundBuffer.offer(buf)) {
      buf.release();
      throw new IllegalStateException("too many requests");
    }
    requestDrain();
  }

  private void requestDrain() {
    if (drainRequested.compareAndSet(false, true)) {
      try {
        eventLoop.execute(() -> {
          drainRequested.set(false);
          drainOutbound();
        });
      } catch (RejectedExecutionException failure) {
        drainRequested.set(false);
        // The termination listener releases queued buffers after the consumer stops.
        throw failure;
      }
    }
  }

  private void drainOutbound() {
    ByteBuf buf;
    if (channelState != ChannelState.Establish) {
      releaseOutbound();
      return;
    }
    int written = 0;
    while (written < config.writeBatchSize() && channel.isActive() && channel.isWritable()
        && (buf = outboundBuffer.poll()) != null) {
      Channel target = channel;
      target.write(buf).addListener(future -> {
        if (!future.isSuccess() && !closeRequested) {
          target.pipeline().fireExceptionCaught(future.cause());
        }
      });
      written++;
    }
    if (written > 0) {
      channel.flush();
    }
    if (channel.isActive() && channel.isWritable() && !outboundBuffer.isEmpty()) {
      requestDrain();
    }
  }

  private ChannelFuture connectOnEventLoop() {
    channelState = ChannelState.Connecting;
    ChannelFuture attempt = bootstrap.connect(peerAddress);
    channel = attempt.channel();
    attempt.addListener(future -> {
      if (!future.isSuccess() && channelState != ChannelState.Closed) {
        channelState = ChannelState.Inactive;
        scheduleReconnect();
        reportError(future.cause());
      }
    });
    return attempt;
  }

  private void scheduleReconnect() {
    if (channelState == ChannelState.Closed || reconnectTask != null) {
      return;
    }
    reconnectBackoffMillis = reconnectBackoffMillis == 0
        ? config.initialReconnectBackoffMillis()
        : reconnectBackoffMillis
            + Math.min(reconnectBackoffMillis, config.maxReconnectBackoffMillis() - reconnectBackoffMillis);
    reconnectTask = eventLoop.schedule(() -> {
      reconnectTask = null;
      if (channelState != ChannelState.Closed) {
        connectOnEventLoop();
      }
    }, reconnectBackoffMillis, TimeUnit.MILLISECONDS);
  }

  /**
   * Stops retries, releases queued messages and closes the connection, but not
   * the group.
   * The caller must stop producers and wait for all send() calls to return first.
   * Calls to closeAsync() and close() must not overlap; sequential repeats are
   * supported.
   * Queued messages are released, not guaranteed to be delivered before close.
   */
  public CompletableFuture<Void> closeAsync() {
    if (!closeRequested) {
      closeRequested = true;
      try {
        eventLoop.execute(this::closeOnEventLoop);
      } catch (RejectedExecutionException ignored) {
        // The event loop's termination listener completes cleanup.
      }
    }
    return closeFuture.copy();
  }

  private void closeOnEventLoop() {
    channelState = ChannelState.Closed;
    if (reconnectTask != null) {
      reconnectTask.cancel(false);
      reconnectTask = null;
    }
    releaseOutbound();
    if (channel == null) {
      closeFuture.complete(null);
    } else {
      channel.close().addListener(future -> {
        if (future.isSuccess()) {
          closeFuture.complete(null);
        } else {
          closeFuture.completeExceptionally(future.cause());
        }
      });
    }
  }

  /**
   * Initiates close under the same caller constraints as closeAsync(); does not
   * wait.
   */
  @Override
  public void close() {
    closeAsync();
  }

  private void releaseOutbound() {
    ByteBuf buf;
    while ((buf = outboundBuffer.poll()) != null) {
      buf.release();
    }
  }

  private final class ClientHandler extends ChannelDuplexHandler {
    private boolean errorReported;
    @Override
    public void channelActive(ChannelHandlerContext ctx) {
      if (channelState == ChannelState.Closed) {
        ctx.close();
        return;
      }
      channel = ctx.channel();
      channelState = ChannelState.Establish;
      reconnectBackoffMillis = 0;
      listener.onConnected();
      drainOutbound();
      ctx.fireChannelActive();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
      try {
        if (msg instanceof Decoder.Greeting greeting) {
          listener.onGreeting(greeting);
        } else if (msg instanceof Decoder.Frame frame) {
          listener.onFrame(frame);
        } else {
          throw new IllegalArgumentException("Unexpected inbound message: " + msg.getClass());
        }
      } finally {
        ReferenceCountUtil.release(msg);
      }
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) {
      // Flush can change writability synchronously; defer to avoid recursive
      // draining.
      if (ctx.channel().isWritable()) {
        requestDrain();
      }
      ctx.fireChannelWritabilityChanged();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
      if (ctx.channel() == channel) {
        if (channelState != ChannelState.Closed) {
          channelState = ChannelState.Inactive;
          releaseOutbound();
          scheduleReconnect();
        }
        listener.onDisconnected();
      }
      ctx.fireChannelInactive();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      try {
        if (!errorReported) {
          errorReported = true;
          reportError(cause);
        }
      } finally {
        ctx.close();
      }
    }
  }
}
