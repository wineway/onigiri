package me.wineway.onigiri.publisher;

import java.util.Arrays;

import io.netty.buffer.ByteBuf;

final class Prefix {
  private final byte[] bytes;
  private final int hash;

  private Prefix(byte[] bytes) {
    this.bytes = bytes;
    hash = Arrays.hashCode(bytes);
  }

  static Prefix copyOf(ByteBuf source) {
    byte[] bytes = new byte[source.readableBytes()];
    source.getBytes(source.readerIndex(), bytes);
    return new Prefix(bytes);
  }

  byte[] copyBytes() { return bytes.clone(); }

  int length() {
    return bytes.length;
  }

  int unsignedByte(int index) {
    return bytes[index] & 0xff;
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof Prefix prefix && Arrays.equals(bytes, prefix.bytes);
  }

  @Override
  public int hashCode() {
    return hash;
  }
}
