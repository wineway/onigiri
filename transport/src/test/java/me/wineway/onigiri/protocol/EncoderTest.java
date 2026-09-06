package me.wineway.onigiri.protocol;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;

class EncoderTest {
  private final Encoder encoder = new Encoder(UnpooledByteBufAllocator.DEFAULT);

  @Test
  void encodesEmptyAndMultipartFrames() {
    assertWire("0000", encoder.message(Unpooled.EMPTY_BUFFER, false));
    assertWire("0100", encoder.message(Unpooled.EMPTY_BUFFER, true));
    ByteBuf body = Unpooled.wrappedBuffer(new byte[] {0x41, 0x42, 0x43});
    try {
      assertWire("0103414243", encoder.message(body, true));
      assertWire("0003414243", encoder.message(body, false));
    } finally {
      body.release();
    }
  }

  @Test
  void appendsVariableFramesWithoutChangingInputIndicesAndRollsBackOnFailure() {
    ByteBuf first = Unpooled.buffer().writeByte(0).writeByte(65);
    first.readByte();
    ByteBuf second = Unpooled.buffer(256).writeZero(256);
    ByteBuf wire = Unpooled.directBuffer(268, 268);
    try {
      Encoder.appendMessage(wire, first, false);
      Encoder.appendMessage(wire, second, true);
      assertEquals(268, wire.writerIndex());
      assertEquals(0, wire.getByte(0));
      assertEquals(1, wire.getByte(1));
      assertEquals(65, wire.getByte(2));
      assertEquals(3, wire.getByte(3));
      assertEquals(256, wire.getLong(4));
      assertEquals(1, first.readerIndex());
      assertEquals(0, second.readerIndex());
      assertThrows(IndexOutOfBoundsException.class, () -> Encoder.appendMessage(wire, first, false));
      assertEquals(268, wire.writerIndex());
    } finally { first.release(); second.release(); wire.release(); }
  }

  @ParameterizedTest
  @ValueSource(ints = {255, 256, 65536})
  void encodesDataLengthInNetworkOrder(int length) {
    ByteBuf body = Unpooled.buffer(length).writeZero(length);
    ByteBuf wire = encoder.message(body, true);
    try {
      assertEquals(length > 255 ? 3 : 1, wire.readUnsignedByte());
      if (length > 255) {
        // Check bytes independently of ByteBuf's long decoding.
        for (int shift = 56; shift >= 0; shift -= 8) {
          assertEquals(((long) length >>> shift) & 0xffL, wire.readUnsignedByte());
        }
      } else {
        assertEquals(length, wire.readUnsignedByte());
      }
      assertEquals(length, wire.readableBytes());
      assertEquals(body, wire);
    } finally {
      wire.release();
      body.release();
    }
  }

  @Test
  void encodesBinarySubscriptionsAndCancellation() {
    ByteBuf prefix = Unpooled.wrappedBuffer(new byte[] {0, (byte) 0xff});
    try {
      assertWire("040c0953554253435249424500ff", encoder.subscribe(prefix));
      assertWire("04090643414e43454c00ff", encoder.cancel(prefix));
      assertWire("040a09535542534352494245", encoder.subscribe(Unpooled.EMPTY_BUFFER));
      assertWire("04070643414e43454c", encoder.cancel(Unpooled.EMPTY_BUFFER));
    } finally {
      prefix.release();
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {245, 246})
  void commandLengthIncludesNameBeforeChoosingShortOrLongFrame(int prefixLength) {
    ByteBuf prefix = Unpooled.buffer(prefixLength).writeZero(prefixLength);
    ByteBuf wire = encoder.subscribe(prefix);
    try {
      assertEquals(prefixLength == 245 ? 4 : 6, wire.readUnsignedByte());
      assertEquals(prefixLength + 10,
          prefixLength == 245 ? wire.readUnsignedByte() : wire.readLong());
      assertEquals(9, wire.readUnsignedByte());
      assertEquals("SUBSCRIBE", wire.readCharSequence(9, StandardCharsets.US_ASCII).toString());
      assertEquals(prefix, wire);
    } finally {
      wire.release();
      prefix.release();
    }
  }

  @Test
  void retainsOnlyReadableBytesWithoutCopyingPayload() {
    ByteBuf original = Unpooled.directBuffer(5).writeBytes(new byte[] {9, 8, 7, 6, 5});
    ByteBuf input = original.slice(1, 3).asReadOnly();
    input.readerIndex(1);
    ByteBuf wire = encoder.message(input, false);
    assertEquals(1, input.readerIndex());
    assertEquals(3, input.writerIndex());
    assertEquals(2, original.refCnt());
    assertInstanceOf(io.netty.buffer.CompositeByteBuf.class, wire);
    // White-box ownership check: shared storage proves encoding did not copy.
    // Production callers must not mutate the payload while the output is in use.
    original.setByte(2, 4);
    original.release();
    assertWire("00020406", wire);
    assertEquals(0, original.refCnt());
  }

  @Test
  void releasingOutputLeavesCallersInputReferenceIntact() {
    ByteBuf body = Unpooled.buffer(1).writeByte(42);
    try {
      ByteBuf encoded = encoder.message(body, false);
      encoded.release();
      assertEquals(1, body.refCnt());
      assertEquals(42, body.getByte(0));
    } finally {
      body.release();
    }
  }

  @Test
  void failedPayloadRetainReleasesAllocatedHeader() {
    var allocator = new io.netty.buffer.AbstractByteBufAllocator(false) {
      ByteBuf header;

      @Override
      protected ByteBuf newHeapBuffer(int initialCapacity, int maxCapacity) {
        header = Unpooled.buffer(initialCapacity, maxCapacity);
        return header;
      }

      @Override
      protected ByteBuf newDirectBuffer(int initialCapacity, int maxCapacity) {
        return newHeapBuffer(initialCapacity, maxCapacity);
      }

      @Override
      public boolean isDirectBufferPooled() {
        return false;
      }
    };
    ByteBuf released = Unpooled.buffer(1).writeByte(42);
    released.release();
    assertThrows(io.netty.util.IllegalReferenceCountException.class,
        () -> new Encoder(allocator).message(released, false));
    assertNotNull(allocator.header);
    assertEquals(0, allocator.header.refCnt());
  }

  @Test
  void encodesNullGreeting() {
    assertWire("ff" + "00".repeat(8) + "7f0301" + "4e554c4c"
        + "00".repeat(48), encoder.greeting());
  }

  @Test
  void encodesSubscriberReadyMetadata() {
    assertWire("04190552454144590b536f636b65742d5479706500000003535542",
        encoder.ready(Map.of("Socket-Type", "SUB".getBytes(StandardCharsets.US_ASCII))));
  }

  @Test
  void encodesLongReadyWithBinaryMetadata() {
    ByteBuf wire = encoder.ready(Map.of("X-Data", new byte[256]));
    try {
      assertEquals(6, wire.readUnsignedByte());
      assertEquals(273, wire.readLong());
      wire.skipBytes(6); // command name length and READY
      assertEquals(6, wire.readUnsignedByte());
      assertEquals("X-Data", wire.readCharSequence(6, StandardCharsets.US_ASCII).toString());
      assertEquals(256, wire.readInt());
      assertEquals(256, wire.readableBytes());
    } finally {
      wire.release();
    }
  }

  @Test
  void rejectsInvalidNamesWithoutConsumingInput() {
    ByteBuf body = Unpooled.buffer(1).writeByte(1);
    try {
      for (String name : new String[] {"", "A".repeat(256), "订阅", "A B", "A\0B"}) {
        assertThrows(IllegalArgumentException.class, () -> encoder.command(name, body));
        assertThrows(IllegalArgumentException.class, () -> encoder.ready(Map.of(name, new byte[0])));
      }
      var duplicates = new LinkedHashMap<String, byte[]>();
      duplicates.put("Socket-Type", new byte[0]);
      duplicates.put("socket-type", new byte[0]);
      assertThrows(IllegalArgumentException.class, () -> encoder.ready(duplicates));
      assertEquals(1, body.refCnt());
      assertEquals(0, body.readerIndex());
    } finally {
      body.release();
    }
  }

  private static void assertWire(String expectedHex, ByteBuf actual) {
    try {
      assertEquals(expectedHex, ByteBufUtil.hexDump(actual));
    } finally {
      actual.release();
    }
  }
}
