package me.wineway.onigiri.protocol;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.params.provider.EnumSource;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.handler.codec.TooLongFrameException;

class DecoderTest {
  private final Encoder encoder = new Encoder(UnpooledByteBufAllocator.DEFAULT);

  @ParameterizedTest
  @EnumSource(value = Command.class, names = "UNKNOWN", mode = EnumSource.Mode.EXCLUDE)
  void recognizesKnownCommandsWithoutAllocatingNames(Command command) {
    EmbeddedChannel channel = frames();
    ByteBuf payload = hex("0041ff");
    try {
      channel.writeInbound(encoder.command(command, payload));
      channel.writeInbound(encoder.command(command.name(), payload));
      assertEquals(1, payload.refCnt());
      for (int i = 0; i < 2; i++) {
        Decoder.Frame frame = channel.readInbound();
        try {
          assertSame(command, frame.commandType());
          assertSame(command.name(), frame.command());
          assertEquals(payload, frame.content());
          Decoder.Frame copy = (Decoder.Frame) frame.copy();
          try {
            assertSame(command, copy.commandType());
          } finally {
            copy.release();
          }
        } finally {
          frame.release();
        }
      }
    } finally {
      payload.release();
      channel.finishAndReleaseAll();
    }
  }

  @Test
  void preservesExtensionCommands() {
    EmbeddedChannel channel = frames();
    try {
      channel.writeInbound(encoder.command("CUSTOM", io.netty.buffer.Unpooled.EMPTY_BUFFER));
      Decoder.Frame frame = channel.readInbound();
      try {
        assertSame(Command.UNKNOWN, frame.commandType());
        assertEquals("CUSTOM", frame.command());
      } finally {
        frame.release();
      }
      assertThrows(IllegalArgumentException.class,
          () -> encoder.command(Command.UNKNOWN, io.netty.buffer.Unpooled.EMPTY_BUFFER));
    } finally {
      channel.finishAndReleaseAll();
    }
  }

  @Test
  void decodesGreetingReadyAndSubscriptionOneByteAtATime() {
    EmbeddedChannel channel = new EmbeddedChannel(new Decoder());
    ByteBuf wire = Unpooled.buffer();
    append(wire, encoder.greeting());
    append(wire, encoder.ready(Map.of("Socket-Type", new byte[] {'S', 'U', 'B'})));
    append(wire, encoder.subscribe(Unpooled.EMPTY_BUFFER));
    try {
      while (wire.isReadable()) {
        channel.writeInbound(Unpooled.buffer(1).writeByte(wire.readByte()));
      }
      assertEquals(new Decoder.Greeting(3, 1, "NULL", false), channel.readInbound());
      Decoder.Frame ready = channel.readInbound();
      try {
        assertEquals("READY", ready.command());
        int readerIndex = ready.content().readerIndex();
        assertArrayEquals(new byte[] {'S', 'U', 'B'},
            Decoder.metadata(ready.content()).get("socket-type"));
        assertEquals(readerIndex, ready.content().readerIndex());
      } finally {
        ready.release();
      }
      Decoder.Frame sub = channel.readInbound();
      try {
        assertEquals("SUBSCRIBE", sub.command());
        assertFalse(sub.more());
        assertEquals(0, sub.content().readableBytes());
      } finally {
        sub.release();
      }
      assertNull(channel.readInbound());
    } finally {
      wire.release();
      channel.finishAndReleaseAll();
    }
  }

  @Test
  void decodesCoalescedFramesAndPreservesMultipartFlags() {
    EmbeddedChannel channel = frames();
    // Nonzero reader index, multipart data, cancellation, and an empty data frame.
    ByteBuf wire = hex("ff010241420002434404080643414e43454c410000");
    wire.skipBytes(1);
    try {
      assertTrue(channel.writeInbound(wire));
      assertFrame(channel, null, true, "4142");
      assertFrame(channel, null, false, "4344");
      assertFrame(channel, "CANCEL", false, "41");
      assertFrame(channel, null, false, "");
      assertNull(channel.readInbound());
    } finally {
      channel.finishAndReleaseAll();
    }
    assertEquals(0, wire.refCnt());
  }

  @ParameterizedTest
  @ValueSource(ints = {255, 256, 65536})
  void decodesFragmentedLongAndShortFrames(int size) {
    EmbeddedChannel channel = frames();
    ByteBuf body = Unpooled.buffer(size).writeZero(size);
    body.setByte(size - 1, 0xff);
    ByteBuf wire = encoder.message(body, false);
    try {
      // Split inside the length header and inside the payload.
      assertFalse(channel.writeInbound(wire.readRetainedSlice(1)));
      assertFalse(channel.writeInbound(wire.readRetainedSlice(wire.readableBytes() - 1)));
      assertTrue(channel.writeInbound(wire.readRetainedSlice(1)));
      Decoder.Frame decoded = channel.readInbound();
      try {
        assertEquals(body, decoded.content());
      } finally {
        decoded.release();
      }
    } finally {
      wire.release();
      body.release();
      channel.finishAndReleaseAll();
    }
  }

  @Test
  void retainedOutputSurvivesDecoderClose() {
    EmbeddedChannel channel = frames();
    ByteBuf input = hex("0003414243");
    channel.writeInbound(input);
    Decoder.Frame frame = channel.readInbound();
    channel.finishAndReleaseAll();
    try {
      assertEquals("ABC", frame.content().toString(StandardCharsets.US_ASCII));
      assertTrue(input.refCnt() > 0);
      Decoder.Frame copy = (Decoder.Frame) frame.copy();
      try {
        assertNull(copy.command());
        assertFalse(copy.more());
        assertEquals(frame.content(), copy.content());
      } finally {
        copy.release();
      }
    } finally {
      frame.release();
    }
    assertEquals(0, input.refCnt());
  }

  @ParameterizedTest
  @ValueSource(strings = {"0800", "0500", "0400", "040100", "04020241",
      "040201ff", "028000000000000000"})
  void rejectsMalformedFramesAndReleasesInput(String bytes) {
    EmbeddedChannel channel = frames();
    ByteBuf input = hex(bytes);
    try {
      assertThrows(CorruptedFrameException.class, () -> channel.writeInbound(input));
      assertFalse(channel.isOpen());
      assertNull(channel.readInbound());
    } finally {
      channel.finishAndReleaseAll();
    }
    assertEquals(0, input.refCnt());
  }

  @Test
  void rejectsOversizedLengthWithoutWaitingForBody() {
    EmbeddedChannel channel = new EmbeddedChannel(new Decoder(256, false));
    ByteBuf header = hex("020000000000000101");
    try {
      assertThrows(TooLongFrameException.class, () -> channel.writeInbound(header));
      assertFalse(channel.isOpen());
    } finally {
      channel.finishAndReleaseAll();
    }
    assertEquals(0, header.refCnt());
  }

  @ParameterizedTest
  @ValueSource(strings = {"00", "000341", "02000000"})
  void rejectsTruncatedFramesAtEofAndReleasesCumulation(String bytes) {
    EmbeddedChannel channel = frames();
    ByteBuf partial = hex(bytes);
    assertFalse(channel.writeInbound(partial));
    assertThrows(CorruptedFrameException.class, channel::finish);
    channel.finishAndReleaseAll();
    assertEquals(0, partial.refCnt());
  }

  @Test
  void rejectsTruncatedGreetingAtEof() {
    EmbeddedChannel channel = new EmbeddedChannel(new Decoder());
    ByteBuf partial = hex("ff0000");
    assertFalse(channel.writeInbound(partial));
    assertThrows(CorruptedFrameException.class, channel::finish);
    channel.finishAndReleaseAll();
    assertEquals(0, partial.refCnt());
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 9, 10, 11, 12, 16, 32})
  void rejectsInvalidOrUnsupportedGreeting(int offset) {
    EmbeddedChannel channel = new EmbeddedChannel(new Decoder());
    ByteBuf greeting = encoder.greeting();
    greeting.setByte(offset, greeting.getByte(offset) ^ 1);
    try {
      assertThrows(CorruptedFrameException.class, () -> channel.writeInbound(greeting));
    } finally {
      channel.finishAndReleaseAll();
    }
    assertEquals(0, greeting.refCnt());
  }

  @Test
  void acceptsHigherVersionAndIgnoresSignaturePadding() {
    EmbeddedChannel channel = new EmbeddedChannel(new Decoder());
    ByteBuf greeting = encoder.greeting();
    greeting.setByte(11, 2);
    greeting.setLong(1, 42);
    try {
      channel.writeInbound(greeting);
      assertEquals(new Decoder.Greeting(3, 2, "NULL", false), channel.readInbound());
    } finally {
      channel.finishAndReleaseAll();
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"00", "014100000002ff", "014180000000", "01ff00000000",
      "014100000000016100000000"})
  void rejectsMalformedReadyMetadataWithoutTakingOwnership(String bytes) {
    ByteBuf content = hex(bytes);
    try {
      assertThrows(CorruptedFrameException.class, () -> Decoder.metadata(content));
      assertEquals(0, content.readerIndex());
      assertEquals(1, content.refCnt());
    } finally {
      content.release();
    }
  }

  private static EmbeddedChannel frames() {
    return new EmbeddedChannel(new Decoder(Decoder.DEFAULT_MAX_FRAME_SIZE, false));
  }

  private static ByteBuf hex(String value) {
    return Unpooled.wrappedBuffer(ByteBufUtil.decodeHexDump(value));
  }

  private static void append(ByteBuf target, ByteBuf source) {
    try {
      target.writeBytes(source);
    } finally {
      source.release();
    }
  }

  private static void assertFrame(EmbeddedChannel channel, String command, boolean more, String data) {
    Decoder.Frame frame = channel.readInbound();
    assertNotNull(frame);
    try {
      assertEquals(command, frame.command());
      assertEquals(more, frame.more());
      assertEquals(data, ByteBufUtil.hexDump(frame.content()));
    } finally {
      frame.release();
    }
  }
}
