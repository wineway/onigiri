package me.wineway.onigiri.session;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.handler.codec.TooLongFrameException;
import java.util.concurrent.TimeUnit;

/** Session-confined bounds/EOF tracking, optionally retaining a complete-message view. */
final class MultipartAssembler {
  private final SubClientConfig config;
  private final MultipartView view;
  private int parts;
  private long bytes;
  private long started;
  private boolean delivering;

  MultipartAssembler(SubClientConfig config, boolean assemble) {
    this.config = config;
    view = assemble ? new MultipartView(config.maxPartsPerMessage()) : null;
  }

  /** Validates before any application callback. Final part remains borrowed. */
  MultipartView accept(ByteBuf buffer, int index, int length, boolean more) {
    if (parts >= config.maxPartsPerMessage() || (more && parts + 1 == config.maxPartsPerMessage())
        || bytes + length > config.maxMessageBytes()) {
      reset();
      throw new TooLongFrameException("Multipart message limit exceeded");
    }
    if (parts == 0 && more && config.multipartTimeoutMillis() > 0) started = System.nanoTime();
    parts++;
    bytes += length;
    if (view != null) view.add(buffer, index, length, more);
    if (more) return null;
    parts = 0;
    bytes = started = 0;
    delivering = view != null;
    return view;
  }

  void delivered() {
    delivering = false;
    reset();
  }

  boolean timedOut(long now) {
    return parts != 0 && config.multipartTimeoutMillis() > 0
        && now - started >= TimeUnit.MILLISECONDS.toNanos(config.multipartTimeoutMillis());
  }

  void endOfInput() {
    boolean truncated = parts != 0;
    reset();
    if (truncated) throw new CorruptedFrameException("Connection ended inside multipart message");
  }

  void reset() {
    // A listener may close its own client. Preserve its borrowed view until return.
    if (delivering) return;
    if (view != null) view.clear();
    parts = 0;
    bytes = started = 0;
  }
}
