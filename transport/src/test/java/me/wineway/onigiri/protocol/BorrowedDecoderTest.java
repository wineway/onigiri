package me.wineway.onigiri.protocol;

import static org.junit.jupiter.api.Assertions.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import io.netty.buffer.*;
import io.netty.channel.*;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.ReferenceCountUtil;

class BorrowedDecoderTest {
  @Test
  void commandsAreOrderedWithBorrowedDataAndRetainedViewsSurviveRemoval() {
    var events = new ArrayList<String>();
    var retained = new ArrayList<ByteBuf>();
    var encoder = new Encoder(UnpooledByteBufAllocator.DEFAULT);
    var channel = new EmbeddedChannel(new Decoder(1024, true, (data, index, length, more) -> {
      events.add("data:" + length + ":" + more);
      retained.add(data.retainedSlice(index, length));
    }), new ChannelInboundHandlerAdapter() {
      @Override public void channelRead(ChannelHandlerContext ctx, Object message) {
        events.add(message instanceof Decoder.Greeting ? "greeting" : ((Decoder.Frame) message).command());
        ReferenceCountUtil.release(message);
      }
    });
    ByteBuf wire = Unpooled.buffer();
    try {
      append(wire, encoder.greeting());
      append(wire, encoder.ready(Map.of("Socket-Type", new byte[] {'P','U','B'})));
      wire.writeByte(1).writeByte(0); // Empty first multipart part.
      wire.writeByte(2).writeLong(408).writeZero(408);
      append(wire, encoder.command(Command.PING, Unpooled.EMPTY_BUFFER));
      wire.writeByte(0).writeByte(1).writeByte(255);
      channel.writeInbound(wire);
      assertEquals(List.of("greeting", "READY", "data:0:true", "data:408:false", "PING", "data:1:false"), events);
      channel.finishAndReleaseAll();
      assertEquals(408, retained.get(1).readableBytes());
      assertEquals(255, retained.get(2).getUnsignedByte(0));
    } finally {
      channel.finishAndReleaseAll();
      retained.forEach(ByteBuf::release);
    }
    assertEquals(0, wire.refCnt());
  }

  @Test
  void fragmentedVariableFramesHaveCorrectOffsetsAndReleaseAllInput() {
    var sizes = new ArrayList<Integer>();
    var channel = new EmbeddedChannel(new Decoder(1024, false, (data, index, length, more) -> {
      assertFalse(more); sizes.add(length);
      for (int i = 0; i < length; i++) assertEquals(i & 255, data.getUnsignedByte(index + i));
    }));
    ByteBuf wire = Unpooled.buffer();
    for (int size : new int[] {0, 64, 128, 200, 255, 256, 408}) {
      if (size > 255) wire.writeByte(2).writeLong(size);
      else wire.writeByte(0).writeByte(size);
      for (int i = 0; i < size; i++) wire.writeByte(i);
    }
    try {
      while (wire.isReadable()) channel.writeInbound(wire.readRetainedSlice(Math.min(7, wire.readableBytes())));
      assertEquals(List.of(0, 64, 128, 200, 255, 256, 408), sizes);
    } finally {
      wire.release(); channel.finishAndReleaseAll();
    }
    assertEquals(0, wire.refCnt());
  }

  @Test
  void callbackFailureClosesConnectionAndDoesNotLeakCumulation() {
    var failure = new IllegalStateException("callback failed");
    var caught = new ArrayList<Throwable>();
    var channel = new EmbeddedChannel(new Decoder(1024, false, (data, index, length, more) -> {
      throw failure;
    }), new ChannelInboundHandlerAdapter() {
      @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable error) {
        caught.add(error); ctx.close();
      }
    });
    ByteBuf wire = Unpooled.buffer().writeShort(1).writeByte(1);
    try {
      channel.writeInbound(wire);
      assertFalse(channel.isActive());
      assertEquals(1, caught.size());
      assertSame(failure, caught.get(0).getCause());
    } finally { channel.finishAndReleaseAll(); }
    assertEquals(0, wire.refCnt());
  }

  private static void append(ByteBuf output, ByteBuf input) {
    try { output.writeBytes(input); } finally { input.release(); }
  }
}
