package me.wineway.onigiri.protocol;

import java.nio.charset.StandardCharsets;

import io.netty.buffer.ByteBuf;

/** Known wire commands. UNKNOWN preserves extensibility through Frame.command(). */
public enum Command {
  READY, ERROR, SUBSCRIBE, CANCEL, PING, PONG, UNKNOWN;

  private static final Command[] KNOWN = {READY, ERROR, SUBSCRIBE, CANCEL, PING, PONG};
  private final byte[] bytes;

  Command() {
    bytes = name().getBytes(StandardCharsets.US_ASCII);
  }

  int length() {
    return bytes.length;
  }

  void writeName(ByteBuf out) {
    out.writeByte(bytes.length);
    out.writeBytes(bytes);
  }

  static Command find(String name) {
    return switch (name) {
      case "READY" -> READY;
      case "ERROR" -> ERROR;
      case "SUBSCRIBE" -> SUBSCRIBE;
      case "CANCEL" -> CANCEL;
      case "PING" -> PING;
      case "PONG" -> PONG;
      default -> UNKNOWN;
    };
  }

  static Command find(ByteBuf in, int index, int length) {
    for (Command candidate : KNOWN) {
      if (candidate.bytes.length != length) {
        continue;
      }
      int i = 0;
      while (i < length && in.getByte(index + i) == candidate.bytes[i]) {
        i++;
      }
      if (i == length) {
        return candidate;
      }
    }
    return UNKNOWN;
  }
}
