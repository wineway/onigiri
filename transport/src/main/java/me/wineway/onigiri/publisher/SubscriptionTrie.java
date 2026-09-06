package me.wineway.onigiri.publisher;

import java.util.Arrays;

import io.netty.buffer.ByteBuf;

/** Lane-confined binary prefix index. Subscription updates may allocate; matching does not. */
final class SubscriptionTrie {
  private final Node root = new Node();

  void add(Prefix prefix, int subscriberSlot) {
    Node node = root;
    for (int i = 0; i < prefix.length(); i++) node = node.ensure(prefix.unsignedByte(i));
    node.add(subscriberSlot);
  }

  void remove(Prefix prefix, int subscriberSlot) {
    Node node = root;
    Node[] path = new Node[prefix.length() + 1];
    path[0] = root;
    for (int i = 0; i < prefix.length(); i++) {
      node = node.find(prefix.unsignedByte(i));
      if (node == null) return;
      path[i + 1] = node;
    }
    node.remove(subscriberSlot);
    for (int i = prefix.length(); i > 0; i--) {
      if (path[i].children.length != 0 || path[i].subscriberWords.length != 0) break;
      path[i - 1].removeChild(prefix.unsignedByte(i - 1));
    }
  }

  int match(ByteBuf data, int index, int length, long[] result, int resultOffset,
      int[] touchedWords, int touchedOffset) {
    Node node = root;
    int touchedCount = node.addTo(result, resultOffset, touchedWords, touchedOffset, 0);
    for (int i = 0; i < length; i++) {
      node = node.find(data.getUnsignedByte(index + i));
      if (node == null) break;
      touchedCount = node.addTo(
          result, resultOffset, touchedWords, touchedOffset, touchedCount);
    }
    return touchedCount;
  }

  private static final class Node {
    private byte[] labels = new byte[0];
    private Node[] children = new Node[0];
    private int[] subscriberWordIndexes = new int[0];
    private long[] subscriberWords = new long[0];

    void add(int slot) {
      int word = slot >>> 6;
      int index = subscriberWordIndex(word);
      if (index < 0) {
        index = subscriberWords.length;
        subscriberWordIndexes = Arrays.copyOf(subscriberWordIndexes, index + 1);
        subscriberWords = Arrays.copyOf(subscriberWords, index + 1);
        subscriberWordIndexes[index] = word;
      }
      subscriberWords[index] |= 1L << (slot & 63);
    }

    void remove(int slot) {
      int word = slot >>> 6;
      int index = subscriberWordIndex(word);
      if (index < 0) return;
      subscriberWords[index] &= ~(1L << (slot & 63));
      if (subscriberWords[index] == 0) {
        int last = subscriberWords.length - 1;
        subscriberWordIndexes[index] = subscriberWordIndexes[last];
        subscriberWords[index] = subscriberWords[last];
        subscriberWordIndexes = Arrays.copyOf(subscriberWordIndexes, last);
        subscriberWords = Arrays.copyOf(subscriberWords, last);
      }
    }

    int addTo(long[] result, int resultOffset, int[] touchedWords, int touchedOffset,
        int touchedCount) {
      for (int i = 0; i < subscriberWords.length; i++) {
        int word = subscriberWordIndexes[i];
        int resultIndex = resultOffset + word;
        if (result[resultIndex] == 0) touchedWords[touchedOffset + touchedCount++] = word;
        result[resultIndex] |= subscriberWords[i];
      }
      return touchedCount;
    }

    private int subscriberWordIndex(int word) {
      for (int i = 0; i < subscriberWordIndexes.length; i++) {
        if (subscriberWordIndexes[i] == word) return i;
      }
      return -1;
    }

    Node find(int label) {
      for (int i = 0; i < labels.length; i++) {
        if ((labels[i] & 0xff) == label) return children[i];
      }
      return null;
    }

    void removeChild(int label) {
      for (int i = 0; i < labels.length; i++) {
        if ((labels[i] & 0xff) != label) continue;
        int last = labels.length - 1;
        labels[i] = labels[last];
        children[i] = children[last];
        labels = Arrays.copyOf(labels, last);
        children = Arrays.copyOf(children, last);
        return;
      }
    }

    Node ensure(int label) {
      Node existing = find(label);
      if (existing != null) return existing;
      int length = labels.length;
      labels = Arrays.copyOf(labels, length + 1);
      children = Arrays.copyOf(children, length + 1);
      labels[length] = (byte) label;
      return children[length] = new Node();
    }
  }
}
