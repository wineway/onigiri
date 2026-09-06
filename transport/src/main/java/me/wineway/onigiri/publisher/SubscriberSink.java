package me.wineway.onigiri.publisher;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

/** Lane-local batch destination implemented by a subscriber session. */
interface SubscriberSink {
  ByteBufAllocator allocator();
  boolean isWritable();
  /** Takes ownership of wire in every outcome; true requests a later flush. */
  boolean write(ByteBuf wire);
  void flush();
  void fail(Throwable failure);
}
