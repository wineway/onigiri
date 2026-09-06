package me.wineway.onigiri.publisher;

import java.util.Arrays;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import me.wineway.onigiri.protocol.Decoder;
import me.wineway.onigiri.protocol.Encoder;

/**
 * Reusable multipart builder for one producer. Each send consumes its input reference
 * in every outcome and copies the readable region without changing input indices.
 * sendLast commits the message; sendMore never publishes a partial message.
 * Not thread-safe: construction, sends, flush, abort and close belong to one producer
 * thread. Stop and join producers before closing the publisher; each producer must
 * close its own writer in a finally block. The publisher never releases writer-local
 * buffers. No delivery acknowledgement. Close discards both unfinished messages and
 * unflushed complete messages. No writer-level locks or CAS.
 */
public class MultipartWriter implements AutoCloseable {
  private final PubServer publisher;
  private final PubServerConfig config;
  private final boolean batching;
  private final int[] offsets;
  private final int[] topics;
  private final int[] lengths;
  private ByteBuf wire;
  private int count;
  private int parts;
  private int messageBytes;
  private boolean closed;

  MultipartWriter(PubServer publisher, PubServerConfig config, boolean batching) {
    this.publisher = publisher;
    this.config = config;
    this.batching = batching;
    int capacity = batching ? config.maxBatchMessages() : 1;
    offsets = new int[capacity + 1];
    topics = new int[capacity];
    lengths = new int[capacity];
  }

  public void sendMore(ByteBuf part) { append(part, true, false); }
  public void sendLast(ByteBuf part) { append(part, false, false); }

  protected final void requireMessageBoundary() {
    if (parts != 0) {
      abort();
      throw new IllegalStateException("Complete or abort the multipart message first");
    }
  }

  protected final void appendSingle(ByteBuf message) { append(message, false, true); }

  private void append(ByteBuf part, boolean more, boolean single) {
    try {
      if (closed || publisher.isClosing()) throw new IllegalStateException("Writer is closed");
      if (part == null) throw new NullPointerException("part");
      if (single && parts != 0) throw new IllegalStateException("Complete or abort the multipart message first");
      if (!single && !more && parts == 0) throw new IllegalStateException("sendLast requires sendMore first");
      int length = part.readableBytes();
      if ((single && length == 0) || parts == config.maxPartsPerMessage() || (more && parts + 1 == config.maxPartsPerMessage())
          || length > Decoder.DEFAULT_MAX_FRAME_SIZE
          || (long) messageBytes + length > config.maxMessageBytes()) {
        throw new IllegalArgumentException("Multipart message limit exceeded");
      }
      // A complete single frame needs only its actual size; a staged multipart
      // message reserves its configured maximum so a partial message is never flushed.
      long reservation = single ? (long) length + (length > 255 ? 9 : 2)
          : (long) config.maxMessageBytes() + 9L * config.maxPartsPerMessage();
      if (parts == 0 && count > 0 && wire.writerIndex() + reservation > config.maxBatchBytes()) {
        flushComplete();
      }
      // The pooled unsafe buffer copies ByteBuf memory directly. The adaptive
      // allocator can allocate a temporary NIO duplicate for every part copied.
      if (wire == null) wire = PooledByteBufAllocator.DEFAULT.directBuffer(
          Math.min(65536, config.maxBatchBytes()), config.maxBatchBytes());
      int header = length > 255 ? 9 : 2;
      int start = wire.writerIndex();
      Encoder.appendMessage(wire, part, more);
      if (parts == 0) {
        topics[count] = start + header;
        lengths[count] = length;
      }
      parts++;
      messageBytes += length;
      if (!more) {
        offsets[++count] = wire.writerIndex();
        parts = messageBytes = 0;
        if (!batching || count == config.maxBatchMessages()) flushComplete();
      }
    } catch (RuntimeException | Error failure) {
      abort();
      throw failure;
    } finally {
      ReferenceCountUtil.release(part);
    }
  }

  /** Discards only the unfinished message; completed batch entries remain queued locally. */
  public void abort() {
    if (wire != null) wire.writerIndex(offsets[count]);
    parts = messageBytes = 0;
  }

  /** Submits complete messages. An unfinished message must be completed or aborted first. */
  public void flush() {
    if (closed || publisher.isClosing()) throw new IllegalStateException("Writer is closed");
    if (parts != 0) throw new IllegalStateException("Cannot flush an unfinished multipart message");
    flushComplete();
  }

  private void flushComplete() {
    if (count == 0) return;
    EncodedBatch encoded;
    try {
      encoded = new EncodedBatch(wire, count, Arrays.copyOf(offsets, count + 1),
          Arrays.copyOf(topics, count), Arrays.copyOf(lengths, count));
    } catch (RuntimeException | Error failure) {
      wire.release();
      throw failure;
    } finally {
      wire = null;
      count = 0;
      offsets[0] = 0;
    }
    publisher.publishEncoded(encoded);
  }

  @Override
  public void close() {
    if (closed) return;
    closed = true;
    if (wire != null) { wire.release(); wire = null; }
    parts = count = messageBytes = 0;
  }
}
