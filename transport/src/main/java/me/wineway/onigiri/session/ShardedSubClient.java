package me.wineway.onigiri.session;

import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import me.wineway.onigiri.transport.ClientChannelConfig;

/**
 * Multi-publisher SUB client composed of independent, serial SubClient shards.
 * Publisher assignment and subscription broadcasts are ordered on a control loop.
 * Each shard owns its registry/subscription state and restores it on reconnect;
 * no mutable session state or message buffers cross shard loops.
 *
 * <p>Callbacks from different shards may run concurrently. Callbacks from a single
 * publisher remain ordered. The listener must be thread-safe and must not block.
 * MessageListener's borrowed-buffer contract is unchanged. The caller owns the group.
 */
public final class ShardedSubClient implements AutoCloseable {
  private final EventLoop control;
  private final SubClient.Listener listener;
  private final SubClient[] shards;
  private final Map<SocketAddress, SubClient> owners = new HashMap<>();
  private record Disconnecting(SubClient shard) {}
  private final Map<SocketAddress, Disconnecting> disconnecting = new HashMap<>();
  private final AtomicBoolean closing = new AtomicBoolean();
  private final CompletableFuture<Void> closed = new CompletableFuture<>();
  private final CompletableFuture<Void> registryCleared = new CompletableFuture<>();
  private int nextShard;

  public ShardedSubClient(EventLoopGroup group, int shardCount, ClientChannelConfig transport,
      SubClientConfig config, SubClient.Listener listener) {
    Objects.requireNonNull(group, "group");
    Objects.requireNonNull(transport, "transport");
    Objects.requireNonNull(config, "config");
    this.listener = Objects.requireNonNull(listener, "listener");
    var loops = new java.util.ArrayList<EventLoop>();
    for (var executor : group) {
      if (!(executor instanceof EventLoop loop)) throw new IllegalArgumentException("Expected EventLoops");
      loops.add(loop);
    }
    if (shardCount <= 0 || shardCount > loops.size()) {
      throw new IllegalArgumentException("shardCount must be positive and no larger than group size");
    }
    control = loops.get(0);
    shards = new SubClient[shardCount];
    for (int i = 0; i < shardCount; i++) shards[i] = new SubClient(loops.get(i), transport, config, listener);
    control.terminationFuture().addListener(ignored -> {
      owners.clear();
      disconnecting.clear();
      registryCleared.complete(null);
      closeAsync();
    });
  }

  /** One publisher is pinned to a shard until explicitly disconnected. Reports TCP attempt only. */
  public CompletableFuture<Void> connect(SocketAddress address) {
    Objects.requireNonNull(address, "address");
    return ordered(() -> {
      if (owners.containsKey(address)) return CompletableFuture.failedFuture(
          new IllegalStateException("Publisher already connected: " + address));
      Disconnecting pending = disconnecting.get(address);
      // A reconnect queued before disconnect completion stays on the same loop,
      // so old and new callbacks for this address cannot overlap across shards.
      SubClient shard = pending == null ? shards[nextShard] : pending.shard();
      if (pending == null) nextShard = (nextShard + 1) % shards.length;
      owners.put(address, shard);
      return shard.connect(address);
    });
  }

  public CompletableFuture<Void> disconnect(SocketAddress address) {
    Objects.requireNonNull(address, "address");
    if (closing.get()) return CompletableFuture.completedFuture(null);
    return ordered(() -> {
      SubClient shard = owners.remove(address);
      if (shard == null) return CompletableFuture.completedFuture(null);
      Disconnecting pending = new Disconnecting(shard);
      disconnecting.put(address, pending);
      var result = shard.disconnect(address);
      result.whenComplete((ignored, failure) -> {
        try {
          control.execute(() -> {
            if (disconnecting.get(address) == pending) disconnecting.remove(address);
          });
        } catch (RejectedExecutionException ignoredRejection) {
          // Termination owns cleanup after the control loop has stopped.
        }
      });
      return result;
    });
  }

  public void subscribe(String prefix) {
    if (!closing.get()) subscribe(Objects.requireNonNull(prefix).getBytes(StandardCharsets.UTF_8));
  }
  public void unsubscribe(String prefix) {
    if (!closing.get()) unsubscribe(Objects.requireNonNull(prefix).getBytes(StandardCharsets.UTF_8));
  }
  public void subscribe(byte[] prefix) { change(prefix, true); }
  public void unsubscribe(byte[] prefix) { change(prefix, false); }

  private void change(byte[] prefix, boolean subscribe) {
    if (closing.get()) return;
    byte[] copy = Objects.requireNonNull(prefix, "prefix").clone();
    try {
      control.execute(() -> {
        if (closing.get()) return;
        // FIFO broadcasts from this single producer preserve update order on every shard.
        for (SubClient shard : shards) {
          try {
            if (subscribe) shard.subscribe(copy);
            else shard.unsubscribe(copy);
          } catch (RejectedExecutionException failure) {
            // Do not leave a live composite with only some shards updated.
            boolean ownerStopping = control.isShuttingDown();
            closeAsync();
            if (!ownerStopping) {
              try { listener.onError(null, failure); }
              catch (Throwable callbackFailure) {
                System.getLogger(ShardedSubClient.class.getName()).log(
                    System.Logger.Level.WARNING, "Error callback failed", callbackFailure);
              }
            }
            return;
          }
        }
      });
    } catch (RejectedExecutionException failure) {
      if (!closing.get()) throw failure;
    }
  }

  public boolean isReady() {
    if (closing.get()) return false;
    for (SubClient shard : shards) if (shard.isReady()) return true;
    return false;
  }

  private CompletableFuture<Void> ordered(java.util.function.Supplier<CompletableFuture<Void>> operation) {
    if (closing.get()) return CompletableFuture.failedFuture(new IllegalStateException("Client is closed"));
    var result = new CompletableFuture<Void>();
    try {
      control.execute(() -> {
        if (closing.get()) {
          result.completeExceptionally(new IllegalStateException("Client is closed")); return;
        }
        try {
          operation.get().whenComplete((ignored, failure) -> {
            if (failure == null) result.complete(null); else result.completeExceptionally(failure);
          });
        } catch (RuntimeException failure) { result.completeExceptionally(failure); }
      });
    } catch (RejectedExecutionException failure) { result.completeExceptionally(failure); }
    return result;
  }

  /** Waits for all shards and registry cleanup; never block on an owned EventLoop. */
  public CompletableFuture<Void> closeAsync() {
    if (closing.compareAndSet(false, true)) {
      try {
        control.execute(() -> { owners.clear(); disconnecting.clear(); registryCleared.complete(null); });
      } catch (RejectedExecutionException ignored) {
        // The termination hook owns registry cleanup once the loop has stopped.
      }
      CompletableFuture<?>[] futures = new CompletableFuture<?>[shards.length + 1];
      futures[0] = registryCleared;
      for (int i = 0; i < shards.length; i++) futures[i + 1] = shards[i].closeAsync();
      CompletableFuture.allOf(futures).whenComplete((ignored, failure) -> {
        if (failure == null) closed.complete(null); else closed.completeExceptionally(failure);
      });
    }
    return closed.copy();
  }
  @Override public void close() { closeAsync(); }
}
