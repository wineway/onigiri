package me.wineway.onigiri.publisher;

import java.util.Arrays;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

import org.jctools.queues.MpscArrayQueue;

import io.netty.buffer.ByteBuf;
import io.netty.channel.EventLoop;

/** One event-loop-confined publisher shard with a bounded cross-thread batch queue. */
final class PublishLane {
  private final EventLoop eventLoop;
  private final MpscArrayQueue<EncodedBatch> queue;
  private final int drainLimit;
  private final int wordCapacity;
  private final long[] routes;
  private final long[] deliverable;
  private final long[] written;
  private final long[] union;
  private final long[] common;
  private final int[] routeWordCounts;
  private final int[] matchedWords;
  private final int[] counts;
  private final SubscriberSink[] sessions;
  private final SubscriptionTrie subscriptions = new SubscriptionTrie();
  private final AtomicBoolean drainRequested = new AtomicBoolean();
  private final LongAdder droppedBatches;
  private final LongAdder fullQueueDrops = new LongAdder();
  private final LongAdder stoppedDrops = new LongAdder();
  private final LongAdder failedBatches = new LongAdder();
  private final AtomicInteger submitting = new AtomicInteger();
  private final CompletableFuture<Void> closed = new CompletableFuture<>();
  private volatile int activeSessions;
  private int deliverableSessions;
  private int unionWordsToClear;
  private int commonWordsToClear;
  private volatile boolean stopping;
  private volatile boolean terminated;

  PublishLane(EventLoop eventLoop, PubServerConfig config, LongAdder droppedBatches) {
    this.eventLoop = eventLoop;
    queue = new MpscArrayQueue<>(config.laneQueueCapacity());
    drainLimit = config.laneDrainLimit();
    sessions = new SubscriberSink[config.maxSubscribersPerLane()];
    counts = new int[sessions.length];
    wordCapacity = (sessions.length + Long.SIZE - 1) / Long.SIZE;
    routes = new long[Math.multiplyExact(config.maxBatchMessages(), wordCapacity)];
    deliverable = new long[wordCapacity];
    written = new long[wordCapacity];
    union = new long[wordCapacity];
    common = new long[wordCapacity];
    routeWordCounts = new int[config.maxBatchMessages()];
    matchedWords = new int[wordCapacity];
    this.droppedBatches = droppedBatches;
    eventLoop.terminationFuture().addListener(ignored -> {
      terminated = true;
      releaseTerminatedQueue();
    });
  }

  EventLoop eventLoop() {
    return eventLoop;
  }

  boolean hasActiveSessions() {
    return activeSessions > 0;
  }

  /** Takes one batch reference in all outcomes. */
  void submit(EncodedBatch batch) {
    submitting.incrementAndGet();
    try {
      if (stopping || terminated) {
        stoppedDrops.increment();
        drop(batch);
        return;
      }
      if (!queue.offer(batch)) {
        fullQueueDrops.increment();
        drop(batch);
        return;
      }
      requestDrain();
    } finally {
      submitting.decrementAndGet();
      // The termination listener and a producer may race while queue.offer publishes.
      if (terminated) releaseTerminatedQueue();
    }
  }

  CompletableFuture<Void> closeAsync() {
    stopping = true;
    if (terminated) releaseTerminatedQueue();
    else requestDrain();
    return closed.copy();
  }

  long fullQueueDrops() { return fullQueueDrops.sum(); }
  long stoppedDrops() { return stoppedDrops.sum(); }
  long failedBatches() { return failedBatches.sum(); }

  int register(SubscriberSink session) {
    requireEventLoop();
    if (stopping || terminated) return -1;
    for (int i = 0; i < sessions.length; i++) {
      if (sessions[i] == null) {
        sessions[i] = session;
        return i;
      }
    }
    return -1;
  }

  void activate(int slot) {
    requireEventLoop();
    activeSessions++;
    updateWritable(slot, sessions[slot].isWritable());
  }

  void updateWritable(int slot, boolean writable) {
    requireEventLoop();
    int word = slot >>> 6;
    long bit = 1L << (slot & 63);
    boolean wasWritable = (deliverable[word] & bit) != 0;
    if (writable == wasWritable) return;
    if (writable) {
      deliverable[word] |= bit;
      deliverableSessions++;
    } else {
      deliverable[word] &= ~bit;
      deliverableSessions--;
    }
  }

  void updateSubscription(int slot, Prefix prefix, boolean subscribe) {
    requireEventLoop();
    if (subscribe) subscriptions.add(prefix, slot);
    else subscriptions.remove(prefix, slot);
  }

  void unregister(int slot, Iterable<Prefix> prefixes, boolean active) {
    requireEventLoop();
    if (slot < 0 || sessions[slot] == null) return;
    for (Prefix prefix : prefixes) subscriptions.remove(prefix, slot);
    updateWritable(slot, false);
    sessions[slot] = null;
    if (active) activeSessions--;
  }

  private void requestDrain() {
    if (!drainRequested.compareAndSet(false, true)) return;
    try {
      eventLoop.execute(this::drain);
    } catch (RejectedExecutionException ignored) {
      stopping = true;
      drainRequested.set(false);
      if (terminated) releaseTerminatedQueue();
    }
  }

  private void drain() {
    drainRequested.set(false);
    try {
      for (int i = 0; i < drainLimit; i++) {
        EncodedBatch batch = queue.poll();
        if (batch == null) break;
        try {
          if (stopping) {
            stoppedDrops.increment();
            droppedBatches.increment();
          }
          else route(batch);
        } catch (RuntimeException failure) {
          failedBatches.increment();
          droppedBatches.increment();
          if (batch.target() != null) batch.target().fail(failure);
          else reportFailure(failure);
        } finally {
          batch.release();
        }
      }
    } finally {
      if ((stopping && submitting.get() != 0) || !queue.isEmpty()) requestDrain();
      else if (stopping) closed.complete(null);
    }
  }

  private void reportFailure(Throwable failure) {
    for (SubscriberSink session : sessions) {
      if (session != null) session.fail(failure);
    }
  }

  private void route(EncodedBatch batch) {
    SubscriberSink target = batch.target();
    if (target != null) {
      if (target.write(batch.content().retainedDuplicate())) target.flush();
      return;
    }
    int count = batch.messageCount();
    if (deliverableSessions == 0) return;
    Arrays.fill(union, 0, unionWordsToClear, 0);
    Arrays.fill(common, 0, commonWordsToClear, 0);
    int unionWords = 0;
    int commonWords = 0;
    boolean sameRoutes = true;
    for (int i = 0; i < count; i++) {
      int offset = i * wordCapacity;
      Arrays.fill(routes, offset, offset + routeWordCounts[i], 0);
      int matchedWordCount = subscriptions.match(
          batch.content(), batch.bodyOffset(i), batch.topicLength(i),
          routes, offset, matchedWords, 0);
      int routeWords = 0;
      for (int word = 0; word < matchedWordCount; word++) {
        routeWords = Math.max(routeWords, matchedWords[word] + 1);
      }
      routeWordCounts[i] = routeWords;
      unionWords = Math.max(unionWords, routeWords);
      for (int word = 0; word < routeWords; word++) {
        long route = routes[offset + word] & deliverable[word];
        routes[offset + word] = route;
        union[word] |= route;
        if (i == 0) common[word] = route;
      }
      if (i == 0) commonWords = routeWords;
      else if (!sameRoute(routes, offset, routeWords, common, commonWords)) sameRoutes = false;
    }
    unionWordsToClear = unionWords;
    commonWordsToClear = commonWords;
    while (unionWords > 0 && union[unionWords - 1] == 0) unionWords--;
    if (unionWords == 0) return;
    if (sameRoutes) {
      writeShared(batch, common, commonWords);
      return;
    }
    writeFiltered(batch, union, unionWords);
  }

  private void writeShared(EncodedBatch batch, long[] route, int words) {
    Arrays.fill(written, 0, words, 0);
    for (int word = 0; word < words; word++) {
      long bits = route[word];
      while (bits != 0) {
        int slot = (word << 6) + Long.numberOfTrailingZeros(bits);
        bits &= bits - 1;
        SubscriberSink session = sessions[slot];
        if (session != null && session.write(batch.content().retainedDuplicate())) {
          written[word] |= 1L << (slot & 63);
        }
      }
    }
    flush(written, words);
  }

  private void writeFiltered(EncodedBatch batch, long[] matched, int words) {
    for (int word = 0; word < words; word++) {
      long bits = matched[word];
      while (bits != 0) {
        int slot = (word << 6) + Long.numberOfTrailingZeros(bits);
        bits &= bits - 1;
        counts[slot] = 0;
      }
    }
    for (int i = 0; i < batch.messageCount(); i++) {
      int offset = i * wordCapacity;
      for (int word = 0; word < words; word++) {
        long bits = routes[offset + word];
        while (bits != 0) {
          int slot = (word << 6) + Long.numberOfTrailingZeros(bits);
          bits &= bits - 1;
          counts[slot] += batch.wireSize(i);
        }
      }
    }
    Arrays.fill(written, 0, words, 0);
    for (int word = 0; word < words; word++) {
      long bits = matched[word];
      while (bits != 0) {
        int slot = (word << 6) + Long.numberOfTrailingZeros(bits);
        bits &= bits - 1;
        SubscriberSink session = sessions[slot];
        if (session == null) continue;
        int size = counts[slot];
        ByteBuf output = session.allocator().directBuffer(size, size);
        boolean handedOff = false;
        try {
          long bit = 1L << (slot & 63);
          for (int i = 0; i < batch.messageCount(); i++) {
            if ((routes[i * wordCapacity + word] & bit) != 0) {
              output.writeBytes(batch.content(), batch.frameOffset(i), batch.wireSize(i));
            }
          }
          handedOff = true;
          if (session.write(output)) written[word] |= bit;
        } finally {
          if (!handedOff && output.refCnt() > 0) output.release();
        }
      }
    }
    flush(written, words);
  }

  private void flush(long[] mask, int words) {
    for (int word = 0; word < words; word++) {
      long bits = mask[word];
      while (bits != 0) {
        int slot = (word << 6) + Long.numberOfTrailingZeros(bits);
        bits &= bits - 1;
        SubscriberSink session = sessions[slot];
        if (session != null) session.flush();
      }
    }
  }

  private static boolean sameRoute(long[] route, int offset, int routeWords,
      long[] expected, int expectedWords) {
    int words = Math.max(routeWords, expectedWords);
    for (int i = 0; i < words; i++) {
      long actual = i < routeWords ? route[offset + i] : 0;
      long value = i < expectedWords ? expected[i] : 0;
      if (actual != value) return false;
    }
    return true;
  }

  /** Serialized because late producers can observe termination concurrently. */
  private synchronized void releaseTerminatedQueue() {
    EncodedBatch batch;
    while ((batch = queue.poll()) != null) {
      stoppedDrops.increment();
      drop(batch);
    }
    if (submitting.get() == 0) {
      while ((batch = queue.poll()) != null) {
        stoppedDrops.increment();
        drop(batch);
      }
      closed.complete(null);
    }
  }

  private void drop(EncodedBatch batch) {
    droppedBatches.increment();
    batch.release();
  }

  private void requireEventLoop() {
    if (!eventLoop.inEventLoop()) throw new IllegalStateException("Lane operation outside event loop");
  }
}
