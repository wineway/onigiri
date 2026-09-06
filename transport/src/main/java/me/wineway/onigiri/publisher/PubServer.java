package me.wineway.onigiri.publisher;

import java.net.SocketAddress;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import me.wineway.onigiri.protocol.Decoder;
import me.wineway.onigiri.protocol.Encoder;
import me.wineway.onigiri.transport.InboundTimeoutHandler;

/**
 * A sharded ZMTP 3.1 PUB server. Subscriber capacity is configured per worker
 * event loop; supplied event-loop groups remain caller-owned.
 */
public final class PubServer implements AutoCloseable {
  /** Input-batch counts and lane-batch counts have different units; do not add them. */
  public record DropCounts(long noSubscribers, long closedPublisher, long invalidBatches,
      long fullLaneQueues, long stoppedLanes, long failedLaneBatches) {}

  /** Callbacks are serialized on the boss control loop; READY does not acknowledge subscriptions. */
  public interface Listener {
    default void onReady(SocketAddress subscriber) {}
    /**
     * Each valid SUBSCRIBE, including duplicates, after routing is updated. Runs on
     * the control loop with other Listener callbacks. Prefix is an independent copy.
     * The connection can be retained and used from other threads; it may already have
     * disconnected when the callback runs. Overload drops notifications without
     * undoing subscriptions; bounded by subscriptionCallbackCapacity.
     */
    default void onSubscription(Connection connection, byte[] prefix) {}
    default void onDisconnected(SocketAddress subscriber) {}
    default void onError(SocketAddress subscriber, Throwable failure) {}
  }

  private final EventLoop controlLoop;
  private final PubServerConfig config;
  private final Listener listener;
  private final PublishLane[] lanes;
  private final Map<EventLoop, PublishLane> lanesByEventLoop;
  // An accepted channel may finish registration after closeAsync closes the group.
  private final ChannelGroup children = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE, true);
  private final LongAdder droppedBatches = new LongAdder();
  private final LongAdder noSubscriberDrops = new LongAdder();
  private final LongAdder closedDrops = new LongAdder();
  private final LongAdder invalidBatches = new LongAdder();
  private final AtomicInteger pendingSubscriptions = new AtomicInteger();
  private final AtomicInteger activeSubscribers = new AtomicInteger();
  private final AtomicBoolean closing = new AtomicBoolean();
  private final AtomicBoolean binding = new AtomicBoolean();
  private final CompletableFuture<Void> closed = new CompletableFuture<>();
  private final ServerBootstrap bootstrap;
  private volatile Channel serverChannel;
  private ChannelFuture bindAttempt;

  public PubServer(EventLoopGroup group) {
    this(group, group, new PubServerConfig(), new Listener() {});
  }

  public PubServer(EventLoopGroup bossGroup, EventLoopGroup workerGroup,
      PubServerConfig config, Listener listener) {
    this.config = Objects.requireNonNull(config, "config");
    this.listener = Objects.requireNonNull(listener, "listener");
    controlLoop = Objects.requireNonNull(bossGroup, "bossGroup").next();
    Objects.requireNonNull(workerGroup, "workerGroup");
    var laneMap = new IdentityHashMap<EventLoop, PublishLane>();
    for (EventExecutor executor : workerGroup) {
      EventLoop eventLoop = (EventLoop) executor;
      laneMap.put(eventLoop, new PublishLane(eventLoop, config, droppedBatches));
    }
    if (laneMap.isEmpty()) throw new IllegalArgumentException("workerGroup has no event loops");
    lanesByEventLoop = Map.copyOf(laneMap);
    lanes = laneMap.values().toArray(PublishLane[]::new);
    bootstrap = new ServerBootstrap()
        .group(bossGroup, workerGroup)
        .channel(NioServerSocketChannel.class)
        .childOption(ChannelOption.TCP_NODELAY, true)
        .childOption(ChannelOption.SO_KEEPALIVE, true)
        .childHandler(new ChannelInitializer<SocketChannel>() {
          @Override
          protected void initChannel(SocketChannel channel) {
            PublishLane lane = lanesByEventLoop.get(channel.eventLoop());
            if (lane == null) throw new IllegalStateException("Missing publish lane");
            children.add(channel);
            channel.pipeline().addLast("inboundTimeout", new InboundTimeoutHandler());
            channel.pipeline().addLast("decoder", new Decoder());
            channel.pipeline().addLast("publisher", new Session(lane, config, sessionListener));
          }
        });
  }

  private final Session.Listener sessionListener = new Session.Listener() {
    @Override
    public void onSubscription(Session session, Prefix prefix) {
      if (closing.get()) return;
      if (pendingSubscriptions.incrementAndGet() > config.subscriptionCallbackCapacity()) {
        // TODO: add metrics report
        pendingSubscriptions.decrementAndGet();
        return;
      }
      boolean submitted = false;
      try {
        byte[] copy = prefix.copyBytes();
        controlLoop.execute(() -> {
          try {
            if (!closing.get()) listener.onSubscription(session, copy);
          } catch (Throwable failure) {
            System.getLogger(PubServer.class.getName()).log(System.Logger.Level.WARNING,
                "Subscription callback failed", failure);
          } finally {
            pendingSubscriptions.decrementAndGet();
          }
        });
        submitted = true;
      } catch (RejectedExecutionException ignored) {
        // Caller-owned control loop has stopped accepting callbacks.
      } finally {
        if (!submitted) pendingSubscriptions.decrementAndGet();
      }
    }

    @Override
    public void onReady(Session session) {
      activeSubscribers.incrementAndGet();
      SocketAddress address = session.remoteAddress();
      dispatch(() -> listener.onReady(address));
    }

    @Override
    public void onDisconnected(Session session, boolean wasActive) {
      if (wasActive) activeSubscribers.decrementAndGet();
      SocketAddress address = session.remoteAddress();
      dispatch(() -> listener.onDisconnected(address));
    }

    @Override
    public void onError(Session session, Throwable failure) {
      SocketAddress address = session.remoteAddress();
      dispatch(() -> listener.onError(address, failure));
    }
  };

  /** Binds once. The returned future completes when the listen socket is active. */
  public synchronized CompletableFuture<Void> bind(SocketAddress address) {
    Objects.requireNonNull(address, "address");
    if (closing.get()) {
      return CompletableFuture.failedFuture(new IllegalStateException("Publisher is closed"));
    }
    if (!binding.compareAndSet(false, true)) {
      return CompletableFuture.failedFuture(new IllegalStateException("Publisher already bound"));
    }
    CompletableFuture<Void> result = new CompletableFuture<>();
    try {
      bindAttempt = bootstrap.bind(address);
    } catch (RuntimeException failure) {
      result.completeExceptionally(failure);
      return result;
    }
    bindAttempt.addListener(future -> {
      if (future.isSuccess()) {
        serverChannel = bindAttempt.channel();
        if (closing.get()) serverChannel.close();
        result.complete(null);
      } else {
        result.completeExceptionally(future.cause());
      }
    });
    return result;
  }

  public boolean isBound() {
    Channel current = serverChannel;
    return !closing.get() && current != null && current.isActive();
  }

  public SocketAddress localAddress() {
    Channel current = serverChannel;
    return current == null ? null : current.localAddress();
  }

  /** Consumes the complete immutable batch in every outcome. */
  void publishEncoded(EncodedBatch encoded) {
    try {
      if (closing.get()) { closedDrops.increment(); return; }
      if (activeSubscribers.get() == 0) { noSubscriberDrops.increment(); return; }
      for (PublishLane lane : lanes) {
        if (lane.hasActiveSessions()) lane.submit((EncodedBatch) encoded.retain());
      }
    } finally {
      encoded.release();
    }
  }

  /** Transfers both references even on failure. Prefer a reusable writer for repeated sends. */
  public void publishMultipart(ByteBuf topic, ByteBuf payload) {
    boolean topicHandedOff = false;
    boolean payloadHandedOff = false;
    try (MultipartWriter writer = newMultipartWriter()) {
      topicHandedOff = true;
      writer.sendMore(topic);
      payloadHandedOff = true;
      writer.sendLast(payload);
    } finally {
      if (!topicHandedOff) ReferenceCountUtil.release(topic);
      if (!payloadHandedOff) ReferenceCountUtil.release(payload);
    }
  }

  /** Creates a producer-owned writer. That producer must close it before publisher shutdown. */
  public MultipartWriter newMultipartWriter() {
    return new MultipartWriter(this, config, false);
  }

  /** Creates a producer-owned batch writer; close it on its producer thread before shutdown. */
  public PublishBatchWriter newBatchWriter() {
    return new PublishBatchWriter(this, config);
  }

  boolean isClosing() { return closing.get(); }

  /** Publishes one non-empty single-frame message. Consumes its reference in every outcome. */
  public void publish(ByteBuf message) {
    Objects.requireNonNull(message, "message");
    ByteBuf wire = null;
    try {
      int length = message.readableBytes();
      int header = length > 255 ? 9 : 2;
      if (length == 0 || length > config.maxMessageBytes()
          || length > Decoder.DEFAULT_MAX_FRAME_SIZE || (long) length + header > config.maxBatchBytes()) {
        invalidBatches.increment();
        throw new IllegalArgumentException("Message exceeds configured limits or is empty");
      }
      if (closing.get()) { closedDrops.increment(); return; }
      if (activeSubscribers.get() == 0) { noSubscriberDrops.increment(); return; }
      wire = io.netty.buffer.PooledByteBufAllocator.DEFAULT.directBuffer(length + header, length + header);
      Encoder.appendMessage(wire, message, false);
      EncodedBatch encoded = new EncodedBatch(wire, 1,
          new int[] {0, length + header}, new int[] {header}, new int[] {length});
      wire = null;
      publishEncoded(encoded);
    } finally {
      ReferenceCountUtil.release(wire);
      message.release();
    }
  }

  /** Weakly consistent counters; excludes nonmatching and unwritable subscriber deliveries. */
  public DropCounts dropCounts() {
    long full = 0, stopped = 0, failed = 0;
    for (PublishLane lane : lanes) {
      full += lane.fullQueueDrops();
      stopped += lane.stoppedDrops();
      failed += lane.failedBatches();
    }
    return new DropCounts(noSubscriberDrops.sum(), closedDrops.sum(), invalidBatches.sum(),
        full, stopped, failed);
  }

  /** Lane-batches rejected, stopped or failed; this is not a message delivery counter. */
  public long droppedBatches() {
    return droppedBatches.sum();
  }

  /**
   * Stops acceptance and publishing, discards queued lane batches and closes children.
   * First signal producers to stop, let each close its own writers, and wait for them
   * to exit. This method never accesses writer-local buffers or waits for producers;
   * it cannot replace closing a writer. Early publisher close rejects further sends
   * but pending writer storage remains producer-owned until that producer closes it.
   * Completion waits for lane cleanup; it does not acknowledge network delivery or callbacks.
   * Never block on this future from an owned event loop. Supplied groups remain caller-owned.
   */
  public synchronized CompletableFuture<Void> closeAsync() {
    if (closing.compareAndSet(false, true)) {
      CompletableFuture<Void> listenerClosed = closeListener();
      CompletableFuture<Void> childrenClosed = new CompletableFuture<>();
      children.close().addListener(future -> {
        if (future.isSuccess()) childrenClosed.complete(null);
        else childrenClosed.completeExceptionally(future.cause());
      });
      CompletableFuture<?>[] completions = new CompletableFuture<?>[lanes.length + 2];
      completions[0] = listenerClosed;
      completions[1] = childrenClosed;
      for (int i = 0; i < lanes.length; i++) completions[i + 2] = lanes[i].closeAsync();
      CompletableFuture.allOf(completions).whenComplete((ignored, failure) -> {
        if (failure == null) closed.complete(null);
        else closed.completeExceptionally(failure);
      });
    }
    return closed.copy();
  }

  private CompletableFuture<Void> closeListener() {
    if (bindAttempt == null) return CompletableFuture.completedFuture(null);
    CompletableFuture<Void> result = new CompletableFuture<>();
    bindAttempt.addListener(bound -> {
      if (!bound.isSuccess()) {
        result.complete(null);
        return;
      }
      bindAttempt.channel().close().addListener(closed -> {
        if (closed.isSuccess()) result.complete(null);
        else result.completeExceptionally(closed.cause());
      });
    });
    return result;
  }

  private void dispatch(Runnable callback) {
    try {
      controlLoop.execute(() -> {
        try {
          callback.run();
        } catch (Throwable failure) {
          System.getLogger(PubServer.class.getName()).log(System.Logger.Level.WARNING,
              "Publisher callback failed", failure);
        }
      });
    } catch (RejectedExecutionException ignored) {
      // The caller owns the event loop and has stopped callback delivery.
    }
  }

  @Override
  public void close() {
    closeAsync();
  }
}
