package me.wineway.onigiri.session;

import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import io.netty.buffer.ByteBuf;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import me.wineway.onigiri.protocol.Decoder;
import me.wineway.onigiri.transport.ClientChannelConfig;

/**
 * Public SUB client API. It owns subscription intent and one {@link Session} per
 * publisher endpoint. All mutable state is serialized on one event loop. The
 * supplied event loop group remains owned by the caller. Calls that change
 * subscriptions after close begins are ignored.
 */
public final class SubClient implements AutoCloseable {
  public interface Listener {
    /** Connection, protocol, timeout and listener errors; runs on the event loop. */
    default void onError(Throwable failure) {}

    /** Address-aware form used when the client has multiple publishers. */
    default void onError(SocketAddress publisher, Throwable failure) { onError(failure); }

    /** Handshake completed and current subscriptions queued, not acknowledged by PUB. */
    default void onReady() {}

    /** Address-aware form used when the client has multiple publishers. */
    default void onReady(SocketAddress publisher) { onReady(); }

    default void onDisconnected() {}

    /** Address-aware form used when the client has multiple publishers. */
    default void onDisconnected(SocketAddress publisher) { onDisconnected(); }

    /**
     * Called on the event loop for each data frame; must not block. The frame is
     * borrowed until return. Retain/release explicitly for asynchronous use.
     * Multipart boundaries are available through frame.more().
     */
    default void onFrame(Decoder.Frame frame) {}

    /** Address-aware form used when the client has multiple publishers. */
    default void onFrame(SocketAddress publisher, Decoder.Frame frame) { onFrame(frame); }
  }

  /**
   * Opt-in data callback without a Frame or per-message slice allocation. All callbacks
   * remain serialized on the client's EventLoop, including across publishers.
   * The buffer is borrowed until return: do not mutate its indices/content or release it.
   * To keep a message, own data.retainedSlice(index, length) and release that slice later.
   * Data invokes onMessage instead of onFrame. Each multipart part has its own more flag.
   */
  public interface MessageListener extends Listener {
    void onMessage(SocketAddress publisher, ByteBuf data, int index, int length, boolean more);
  }

  /**
   * Complete-message delivery, including single-frame messages. Invoked instead of
   * onFrame/onMessage. View and buffers are borrowed only until the callback returns;
   * retain individual slices explicitly for asynchronous use. Never mutate borrowed data.
   */
  public interface MultipartListener extends Listener {
    void onMultipart(SocketAddress publisher, MultipartView message);
  }

  private final EventLoop eventLoop;
  private final ClientChannelConfig transportConfig;
  private final SubClientConfig config;
  private final Listener listener;
  private final Map<SocketAddress, Session> sessions = new LinkedHashMap<>();
  private final Set<Session> readySessions =
      Collections.newSetFromMap(new IdentityHashMap<>());
  private final Set<ByteBuffer> subscriptions = new LinkedHashSet<>();
  private final AtomicBoolean closing = new AtomicBoolean();
  private final CompletableFuture<Void> closed = new CompletableFuture<>();
  private volatile int readySessionCount;

  public SubClient(EventLoopGroup group) {
    this(group, new ClientChannelConfig(), new Listener() {});
  }

  public SubClient(EventLoopGroup group, ClientChannelConfig config, Listener listener) {
    this(group, config, new SubClientConfig(), listener);
  }

  public SubClient(EventLoopGroup group, ClientChannelConfig transportConfig,
      SubClientConfig config, Listener listener) {
    this.listener = Objects.requireNonNull(listener, "listener");
    this.transportConfig = Objects.requireNonNull(transportConfig, "transportConfig");
    this.config = Objects.requireNonNull(config, "config");
    eventLoop = Objects.requireNonNull(group, "group").next();
    eventLoop.terminationFuture().addListener(ignored -> {
      closing.set(true);
      readySessions.clear();
      readySessionCount = 0;
      sessions.clear();
      subscriptions.clear();
      closed.complete(null);
    });
  }

  private Session newSession(SocketAddress address) {
    return new Session(eventLoop, transportConfig, config, new Session.Listener() {
      @Override
      public void restoreSubscriptions(Session current) {
        for (ByteBuffer prefix : subscriptions) {
          current.sendSubscription(prefix, true);
        }
      }

      @Override
      public void onReady(Session current) {
        if (sessions.get(address) != current) return;
        if (readySessions.add(current)) readySessionCount = readySessions.size();
        SubClient.this.listener.onReady(address);
      }

      @Override
      public void onDisconnected(Session current) {
        markNotReady(current);
        try {
          SubClient.this.listener.onDisconnected(address);
        } catch (RuntimeException failure) {
          notifyError(address, failure);
        }
      }

      @Override
      public void onError(Session current, Throwable failure) {
        markNotReady(current);
        notifyError(address, failure);
      }

      @Override
      public void onFrame(Session current, Decoder.Frame frame) {
        SubClient.this.listener.onFrame(address, frame);
      }

      @Override
      public boolean borrowedMessages() { return listener instanceof MessageListener || listener instanceof MultipartListener; }

      @Override
      public void onMessage(Session current, ByteBuf data, int index, int length, boolean more) {
        ((MessageListener) listener).onMessage(address, data, index, length, more);
      }

      @Override
      public boolean completeMessages() { return listener instanceof MultipartListener; }

      @Override
      public void onMultipart(Session current, MultipartView message) {
        ((MultipartListener) listener).onMultipart(address, message);
      }

      @Override
      public void onClosed(Session current) {
        markNotReady(current);
        sessions.remove(address, current);
      }
    });
  }

  /**
   * Adds a publisher and reports its initial TCP attempt. The session keeps
   * reconnecting; adding the same address twice fails.
   */
  public CompletableFuture<Void> connect(SocketAddress address) {
    Objects.requireNonNull(address, "address");
    if (closing.get()) {
      return CompletableFuture.failedFuture(new IllegalStateException("SubClient is closed"));
    }
    CompletableFuture<Void> result = new CompletableFuture<>();
    try {
      eventLoop.execute(() -> {
        if (closing.get()) {
          result.completeExceptionally(new IllegalStateException("SubClient is closed"));
          return;
        }
        if (sessions.containsKey(address)) {
          result.completeExceptionally(
              new IllegalStateException("Publisher already connected: " + address));
          return;
        }
        Session session = newSession(address);
        sessions.put(address, session);
        session.connect(address).whenComplete((ignored, failure) -> {
          if (failure == null) result.complete(null);
          else result.completeExceptionally(failure);
        });
      });
    } catch (RejectedExecutionException failure) {
      result.completeExceptionally(failure);
    }
    return result;
  }

  /** Removes a publisher and stops reconnecting to it. Missing publishers are ignored. */
  public CompletableFuture<Void> disconnect(SocketAddress address) {
    Objects.requireNonNull(address, "address");
    if (closing.get()) return CompletableFuture.completedFuture(null);
    CompletableFuture<Void> result = new CompletableFuture<>();
    try {
      eventLoop.execute(() -> {
        Session session = sessions.remove(address);
        if (session == null) {
          result.complete(null);
          return;
        }
        markNotReady(session);
        session.closeAsync().whenComplete((ignored, failure) -> {
          if (failure == null) result.complete(null);
          else result.completeExceptionally(failure);
        });
      });
    } catch (RejectedExecutionException failure) {
      if (closing.get()) result.complete(null);
      else result.completeExceptionally(failure);
    }
    return result;
  }

  /** True when at least one publisher has completed its protocol handshake. */
  public boolean isReady() {
    return !closing.get() && readySessionCount > 0;
  }

  public void subscribe(String prefix) {
    if (!closing.get()) {
      subscribe(Objects.requireNonNull(prefix, "prefix").getBytes(StandardCharsets.UTF_8));
    }
  }

  public void unsubscribe(String prefix) {
    if (!closing.get()) {
      unsubscribe(Objects.requireNonNull(prefix, "prefix").getBytes(StandardCharsets.UTF_8));
    }
  }

  /** Copies and queues the prefix; return does not imply execution or remote acknowledgement. */
  public void subscribe(byte[] prefix) {
    change(prefix, true);
  }

  public void unsubscribe(byte[] prefix) {
    change(prefix, false);
  }

  private void change(byte[] prefix, boolean subscribe) {
    if (closing.get()) return;
    ByteBuffer key = ByteBuffer.wrap(Objects.requireNonNull(prefix, "prefix").clone()).asReadOnlyBuffer();
    try {
      eventLoop.execute(() -> {
        if (closing.get()) return;
        boolean changed = subscribe ? subscriptions.add(key) : subscriptions.remove(key);
        if (changed) {
          for (Session session : new ArrayList<>(readySessions)) {
            try {
              session.sendSubscription(key, subscribe);
            } catch (RuntimeException failure) {
              session.fail(failure);
            }
          }
        }
      });
    } catch (RejectedExecutionException failure) {
      if (!closing.get()) throw failure;
    }
  }

  private void markNotReady(Session session) {
    if (readySessions.remove(session)) readySessionCount = readySessions.size();
  }

  private void notifyError(SocketAddress address, Throwable failure) {
    try {
      listener.onError(address, failure);
    } catch (Throwable callbackFailure) {
      System.getLogger(SubClient.class.getName()).log(System.Logger.Level.WARNING,
          "Error callback failed", callbackFailure);
    }
  }

  public CompletableFuture<Void> closeAsync() {
    if (closing.compareAndSet(false, true)) {
      try {
        eventLoop.execute(() -> {
          subscriptions.clear();
          readySessions.clear();
          readySessionCount = 0;
          List<Session> closingSessions = new ArrayList<>(sessions.values());
          sessions.clear();
          CompletableFuture<?>[] futures = closingSessions.stream()
              .map(Session::closeAsync)
              .toArray(CompletableFuture[]::new);
          CompletableFuture.allOf(futures).whenComplete(this::completeClose);
        });
      } catch (RejectedExecutionException ignored) {
        // The event-loop termination callback completes closed and session cleanup.
      }
    }
    return closed.copy();
  }

  private void completeClose(Void ignored, Throwable failure) {
    if (failure == null) {
      closed.complete(null);
    } else {
      closed.completeExceptionally(failure);
    }
  }

  @Override
  public void close() {
    closeAsync();
  }
}
