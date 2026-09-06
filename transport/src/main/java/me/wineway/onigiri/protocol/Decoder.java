package me.wineway.onigiri.protocol;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.DefaultByteBufHolder;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.TooLongFrameException;

/**
 * Streaming counterpart to {@link Encoder}, for version 3.1 and the NULL mechanism.
 * Install a new instance per connection. By default a 64-byte greeting is required
 * first; frame-only mode is for pipelines that already handled the greeting.
 *
 * <p>Outputs {@link Greeting} and reference-counted {@link Frame} objects. Downstream
 * owns each frame and must release it (or use a SimpleChannelInboundHandler).
 * Command contents exclude the name and its length byte. Data is emitted one frame
 * at a time; the session must assemble multipart messages before application delivery,
 * validate command semantics, and enforce handshake ordering.
 * Malformed/oversized input closes the connection. A partial frame at EOF is an error.
 */
public final class Decoder extends ByteToMessageDecoder {
  public static final int DEFAULT_MAX_FRAME_SIZE = 16 * 1024 * 1024;

  /**
   * Synchronous data-only callback. Buffer and indices are borrowed until return.
   * Do not modify indices/content or release the buffer. Use retainedSlice(index, length)
   * and release that slice later when asynchronous ownership is needed.
   */
  @FunctionalInterface
  public interface DataConsumer {
    void onData(ByteBuf buffer, int index, int length, boolean more);
  }

  private final DataConsumer dataConsumer;
  private final int maxFrameSize;
  private boolean greetingPending;
  private boolean failed;

  public Decoder() {
    this(DEFAULT_MAX_FRAME_SIZE, true);
  }

  /** maxFrameSize limits the entire frame body, including command names. */
  public Decoder(int maxFrameSize, boolean expectGreeting) {
    this(maxFrameSize, expectGreeting, null);
  }

  /** Optional allocation-free data delivery; greetings and commands still use the pipeline. */
  public Decoder(int maxFrameSize, boolean expectGreeting, DataConsumer dataConsumer) {
    this.dataConsumer = dataConsumer;
    if (maxFrameSize < 0 || maxFrameSize > Integer.MAX_VALUE - 9) {
      throw new IllegalArgumentException("Invalid maximum frame size");
    }
    this.maxFrameSize = maxFrameSize;
    greetingPending = expectGreeting;
  }

  public record Greeting(int major, int minor, String mechanism, boolean asServer) {
  }

  /** A data frame has a null command name. Content is a retained view of input. */
  public static final class Frame extends DefaultByteBufHolder {
    private final String command;
    private final Command commandType;
    private final boolean more;

    private Frame(ByteBuf content, String command, Command commandType, boolean more) {
      super(content);
      this.command = command;
      this.commandType = commandType;
      this.more = more;
    }

    public String command() {
      return command;
    }

    /** Null for data frames; UNKNOWN for an extension command. */
    public Command commandType() {
      return commandType;
    }

    public boolean more() {
      return more;
    }

    @Override
    public Frame replace(ByteBuf content) {
      return new Frame(content, command, commandType, more);
    }
  }

  @Override
  protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
    if (failed) {
      in.skipBytes(in.readableBytes());
      return;
    }
    try {
      if (greetingPending) {
        if (in.readableBytes() < 64) {
          return;
        }
        decodeGreeting(in, out);
        return;
      }
      do {
        int before = in.readerIndex();
        decodeFrame(in, out);
        // Consume adjacent data frames together without the generic per-frame output list.
        // Return at commands so ByteToMessageDecoder delivers them before later data.
        if (dataConsumer == null || !out.isEmpty() || in.readerIndex() == before
            || ctx.isRemoved() || !ctx.channel().isActive()) break;
      } while (in.isReadable());
    } catch (DecoderException failure) {
      failed = true;
      in.skipBytes(in.readableBytes());
      ctx.close();
      throw failure;
    }
  }

  private void decodeGreeting(ByteBuf in, List<Object> out) {
    int start = in.readerIndex();
    if (in.getUnsignedByte(start) != 0xff || in.getUnsignedByte(start + 9) != 0x7f) {
      throw new CorruptedFrameException("Invalid greeting signature");
    }
    int major = in.getUnsignedByte(start + 10);
    int minor = in.getUnsignedByte(start + 11);
    if (major < 3 || (major == 3 && minor < 1)) {
      throw new CorruptedFrameException("Peer version below 3.1 is unsupported");
    }
    String mechanism = in.toString(start + 12, 4, StandardCharsets.US_ASCII);
    if (!mechanism.equals("NULL")) {
      throw new CorruptedFrameException("Only the NULL mechanism is supported");
    }
    for (int i = 16; i < 32; i++) {
      if (in.getByte(start + i) != 0) {
        throw new CorruptedFrameException("Invalid NULL mechanism padding");
      }
    }
    if (in.getByte(start + 32) != 0) {
      throw new CorruptedFrameException("NULL greeting must have as-server zero");
    }
    // Signature padding and reserved filler carry no application data.
    in.skipBytes(64);
    greetingPending = false;
    out.add(new Greeting(major, minor, mechanism, false));
  }

  private void decodeFrame(ByteBuf in, List<Object> out) {
    if (in.readableBytes() < 2) {
      return;
    }
    int start = in.readerIndex();
    int flags = in.getUnsignedByte(start);
    boolean command = (flags & 4) != 0;
    boolean more = (flags & 1) != 0;
    if ((flags & ~7) != 0 || (command && more)) {
      throw new CorruptedFrameException("Invalid frame flags");
    }
    int headerSize = (flags & 2) != 0 ? 9 : 2;
    if (in.readableBytes() < headerSize) {
      return;
    }
    long length = headerSize == 9 ? in.getLong(start + 1) : in.getUnsignedByte(start + 1);
    if (length < 0) {
      throw new CorruptedFrameException("Frame length exceeds signed 64-bit range");
    }
    if (length > maxFrameSize) {
      throw new TooLongFrameException("Frame body exceeds " + maxFrameSize + " bytes");
    }
    if (length > in.readableBytes() - headerSize) {
      return;
    }
    int bodyLength = (int) length;
    int commandHeader = 0;
    String name = null;
    Command commandType = null;
    if (command) {
      if (bodyLength == 0) {
        throw new CorruptedFrameException("Missing command name");
      }
      int nameLength = in.getUnsignedByte(start + headerSize);
      commandHeader = 1 + nameLength;
      if (nameLength == 0 || commandHeader > bodyLength) {
        throw new CorruptedFrameException("Invalid command name length");
      }
      commandType = Command.find(in, start + headerSize + 1, nameLength);
      name = commandType == Command.UNKNOWN
          ? readName(in, start + headerSize + 1, nameLength, false)
          : commandType.name();
    }
    in.skipBytes(headerSize + commandHeader);
    if (!command && dataConsumer != null) {
      int index = in.readerIndex();
      in.skipBytes(bodyLength);
      dataConsumer.onData(in, index, bodyLength, more);
    } else {
      out.add(new Frame(in.readRetainedSlice(bodyLength - commandHeader), name, commandType, more));
    }
  }

  @Override
  protected void decodeLast(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
    decode(ctx, in, out);
    if (!failed && in.isReadable()) {
      failed = true;
      in.skipBytes(in.readableBytes());
      throw new CorruptedFrameException("Truncated greeting or frame at end of stream");
    }
  }

  /**
   * Parses a READY frame's content without changing its indices or reference count.
   * Keys are normalized to lower case; values are independent byte arrays.
   */
  public static Map<String, byte[]> metadata(ByteBuf content) {
    ByteBuf in = content.duplicate();
    var result = new LinkedHashMap<String, byte[]>();
    while (in.isReadable()) {
      int nameLength = in.readUnsignedByte();
      if (nameLength == 0 || in.readableBytes() < nameLength + 4) {
        throw new CorruptedFrameException("Invalid metadata name length");
      }
      String name = readName(in, in.readerIndex(), nameLength, true).toLowerCase(Locale.ROOT);
      in.skipBytes(nameLength);
      long valueLength = in.readUnsignedInt();
      if (valueLength > Integer.MAX_VALUE || valueLength > in.readableBytes()) {
        throw new CorruptedFrameException("Invalid metadata value length");
      }
      if (result.containsKey(name)) {
        throw new CorruptedFrameException("Duplicate metadata name: " + name);
      }
      byte[] value = new byte[(int) valueLength];
      in.readBytes(value);
      result.put(name, value);
    }
    return Collections.unmodifiableMap(result);
  }

  private static String readName(ByteBuf in, int index, int length, boolean metadata) {
    for (int i = 0; i < length; i++) {
      int c = in.getUnsignedByte(index + i);
      boolean letter = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z');
      boolean propertyChar = (c >= '0' && c <= '9') || c == '-' || c == '_'
          || c == '.' || c == '+';
      if (!letter && !(metadata && propertyChar)) {
        throw new CorruptedFrameException("Invalid name character");
      }
    }
    return in.toString(index, length, StandardCharsets.US_ASCII);
  }
}
