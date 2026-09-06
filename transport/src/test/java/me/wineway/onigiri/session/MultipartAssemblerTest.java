package me.wineway.onigiri.session;

import static org.junit.jupiter.api.Assertions.*;
import io.netty.buffer.*;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.handler.codec.TooLongFrameException;
import me.wineway.onigiri.protocol.Decoder;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class MultipartAssemblerTest {
  @Test
  void splitReadsRetainUntilCompleteAndExplicitAsyncSliceSurvives() {
    var assembler = new MultipartAssembler(new SubClientConfig(), true);
    ByteBuf topic = Unpooled.buffer().writeByte(65);
    ByteBuf payload = Unpooled.buffer().writeInt(42);
    assertNull(assembler.accept(topic, 0, 1, true));
    topic.release();
    MultipartView view = assembler.accept(payload, 0, 4, false);
    assertEquals(2, view.partCount());
    assertEquals(65, view.buffer(0).getByte(view.index(0)));
    ByteBuf retained = view.buffer(1).retainedSlice(view.index(1), view.length(1));
    assembler.reset(); // close inside callback must not invalidate borrowing
    assertEquals(2, view.partCount());
    assembler.delivered();
    payload.release();
    assertEquals(0, topic.refCnt());
    assertEquals(42, retained.readInt());
    retained.release();
    assertEquals(0, payload.refCnt());
    assertEquals(0, view.partCount());
  }

  @Test
  void truncationAndBoundsReleaseAllReferencesAndResetForReconnect() {
    var assembler = new MultipartAssembler(new SubClientConfig(0, 0, 0, 0, 3, 4, 1), true);
    ByteBuf data = Unpooled.buffer().writeZero(4);
    assembler.accept(data, 0, 1, true);
    assertThrows(CorruptedFrameException.class, assembler::endOfInput);
    assertEquals(1, data.refCnt());
    assembler.accept(data, 0, 1, true);
    assertThrows(TooLongFrameException.class, () -> assembler.accept(data, 0, 4, false));
    assertEquals(1, data.refCnt());
    assembler.accept(data, 0, 0, true);
    assembler.accept(data, 0, 0, true);
    assertThrows(TooLongFrameException.class, () -> assembler.accept(data, 0, 0, true));
    assertEquals(1, data.refCnt());
    assembler.accept(data, 0, 1, true);
    assertTrue(assembler.timedOut(System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(2)));
    assembler.reset();
    MultipartView single = assembler.accept(data, 0, 4, false);
    assertEquals(1, single.partCount());
    assembler.delivered();
    assembler.endOfInput();
    assertEquals(1, data.refCnt());
    data.release();
  }

  @Test
  void decoderFragmentationEmptyAndLongFramesDeliverOnlyCompleteMessages() {
    for (int split : new int[] {1, 2, 3, 9, 257, 1000}) {
      var assembler = new MultipartAssembler(new SubClientConfig(), true);
      int[] delivered = {0};
      var channel = new EmbeddedChannel(new Decoder(4096, false, (data, index, length, more) -> {
        MultipartView view = assembler.accept(data, index, length, more);
        if (view != null) {
          try {
            assertEquals(3, view.partCount());
            assertEquals(255, view.length(0));
            assertEquals(0, view.length(1));
            assertEquals(256, view.length(2));
            delivered[0]++;
          } finally { assembler.delivered(); }
        }
      }));
      ByteBuf wire = Unpooled.buffer();
      for (int i = 0; i < 2; i++) {
        wire.writeByte(1).writeByte(255).writeZero(255);
        wire.writeByte(1).writeByte(0);
        wire.writeByte(2).writeLong(256).writeZero(256);
      }
      try {
        while (wire.isReadable()) channel.writeInbound(wire.readRetainedSlice(Math.min(split, wire.readableBytes())));
        assertEquals(2, delivered[0]);
        assembler.endOfInput();
      } finally { channel.finishAndReleaseAll(); assembler.reset(); wire.release(); }
    }
  }
}
