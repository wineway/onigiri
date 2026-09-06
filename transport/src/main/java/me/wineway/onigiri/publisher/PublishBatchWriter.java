package me.wineway.onigiri.publisher;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;

/**
 * Reusable batch of variable-size single-frame and multipart messages. Each entry
 * is routed by its first frame and submitted whole. Full batches submit automatically;
 * flush submits a partial batch. Close discards pending entries. One producer per writer.
 * The owning producer must also flush/abort/close this writer; concurrent close is unsupported.
 * All add/send methods consume input references in every outcome.
 */
public final class PublishBatchWriter extends MultipartWriter {
  PublishBatchWriter(PubServer publisher, PubServerConfig config) { super(publisher, config, true); }

  /** Appends one complete non-empty single-frame message. */
  public void add(ByteBuf message) { appendSingle(message); }

  /** Appends a complete two-part message; consumes both references even on failure. */
  public void addMultipart(ByteBuf topic, ByteBuf payload) {
    boolean handedOff = false;
    try {
      // Validate the message boundary before allowing sendMore to extend a pending message.
      requireMessageBoundary();
    } catch (RuntimeException | Error failure) {
      ReferenceCountUtil.release(topic);
      ReferenceCountUtil.release(payload);
      throw failure;
    }
    try {
      sendMore(topic);
      handedOff = true;
      sendLast(payload);
    } finally {
      if (!handedOff) ReferenceCountUtil.release(payload);
    }
  }
}
