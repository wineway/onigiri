package me.wineway.onigiri.publisher;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.DefaultByteBufHolder;

/** Immutable variable-size wire batch shared by publish lanes. */
final class EncodedBatch extends DefaultByteBufHolder {
  private final SubscriberSink target;
  private final int[] offsets;
  private final int[] topicOffsets;
  private final int[] topicLengths;
  private final int messageCount;

  /** Arrays are immutable and owned by this batch; offsets are relative to readerIndex. */
  EncodedBatch(ByteBuf content, int count, int[] offsets, int[] topicOffsets, int[] topicLengths) {
    super(content);
    target = null;
    messageCount = count;
    this.offsets = offsets;
    this.topicOffsets = topicOffsets;
    this.topicLengths = topicLengths;
  }

  /** One complete wire message addressed to an exact session, not a reusable lane slot. */
  EncodedBatch(ByteBuf content, SubscriberSink target) {
    super(content);
    this.target = java.util.Objects.requireNonNull(target, "target");
    messageCount = 1;
    offsets = topicOffsets = topicLengths = null;
  }

  SubscriberSink target() { return target; }

  int wireSize(int index) { return offsets[index + 1] - offsets[index]; }
  int topicLength(int index) { return topicLengths[index]; }
  int messageCount() { return messageCount; }
  int frameOffset(int index) { return content().readerIndex() + offsets[index]; }
  int bodyOffset(int index) { return content().readerIndex() + topicOffsets[index]; }

  @Override
  public EncodedBatch replace(ByteBuf content) {
    return target != null ? new EncodedBatch(content, target)
        : new EncodedBatch(content, messageCount, offsets, topicOffsets, topicLengths);
  }
}
