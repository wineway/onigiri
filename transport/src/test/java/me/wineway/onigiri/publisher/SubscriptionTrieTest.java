package me.wineway.onigiri.publisher;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

class SubscriptionTrieTest {
  @Test
  void matchesAllBinaryPrefixesWithoutAllocationState() {
    SubscriptionTrie trie = new SubscriptionTrie();
    Prefix all = prefix();
    Prefix a = prefix(0x41);
    Prefix ab = prefix(0x41, 0x42);
    trie.add(all, 0);
    trie.add(a, 1);
    trie.add(ab, 66);
    ByteBuf value = Unpooled.wrappedBuffer(new byte[] {0x41, 0x42, (byte) 0xff});
    long[] matches = new long[2];
    int[] touchedWords = new int[2];
    try {
      trie.match(value, value.readerIndex(), value.readableBytes(), matches, 0, touchedWords, 0);
      assertEquals(3, matches[0]);
      assertEquals(4, matches[1]);

      trie.remove(a, 1);
      matches[0] = matches[1] = 0;
      trie.match(value, value.readerIndex(), value.readableBytes(), matches, 0, touchedWords, 0);
      assertEquals(1, matches[0]);
      assertEquals(4, matches[1]);

      matches[0] = matches[1] = 0;
      trie.match(value, value.readerIndex() + 2, 1, matches, 0, touchedWords, 0);
      assertEquals(1, matches[0]);
      assertEquals(0, matches[1]);
    } finally {
      value.release();
    }
  }

  private static Prefix prefix(int... values) {
    ByteBuf data = Unpooled.buffer(values.length);
    for (int value : values) data.writeByte(value);
    try {
      return Prefix.copyOf(data);
    } finally {
      data.release();
    }
  }
}
