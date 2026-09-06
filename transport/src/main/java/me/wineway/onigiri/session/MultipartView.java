package me.wineway.onigiri.session;

import io.netty.buffer.ByteBuf;

/** Reused complete-message view. Valid only inside MultipartListener.onMultipart. */
public final class MultipartView {
  private final ByteBuf[] buffers;
  private final int[] indices;
  private final int[] lengths;
  private int count;
  private int retained;

  MultipartView(int maxParts) {
    buffers = new ByteBuf[maxParts];
    indices = new int[maxParts];
    lengths = new int[maxParts];
  }

  public int partCount() { return count; }
  public ByteBuf buffer(int part) { check(part); return buffers[part]; }
  public int index(int part) { check(part); return indices[part]; }
  public int length(int part) { check(part); return lengths[part]; }

  private void check(int part) {
    if (part < 0 || part >= count) throw new IndexOutOfBoundsException(part);
  }

  void add(ByteBuf buffer, int index, int length, boolean more) {
    if (more) { buffer.retain(); retained++; }
    buffers[count] = buffer;
    indices[count] = index;
    lengths[count++] = length;
  }

  void clear() {
    for (int i = 0; i < count; i++) {
      if (i < retained) buffers[i].release();
      buffers[i] = null;
    }
    count = retained = 0;
  }
}
