package me.wineway.onigiri.publisher;

import java.net.SocketAddress;
import io.netty.buffer.ByteBuf;

/**
 * A handle to one accepted connection, never reassigned after disconnect/reconnect.
 * Sends are thread-safe, best effort, and target only this connection without topic
 * routing. Every send consumes its input references, including invalid input, closed
 * connections and full queues. Inputs are copied without changing their indices.
 * No acknowledgement, retry or ordering guarantee relative to concurrent publication.
 */
public interface Connection {
  SocketAddress remoteAddress();

  /** Sends one non-empty single-frame message. */
  void send(ByteBuf message);

  /** Sends a complete two-frame message; empty topic/payload frames are allowed. */
  void send(ByteBuf topic, ByteBuf payload);
}
