package me.wineway.onigiri.protocol;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

/**
 * Encodes version 3.1 wire frames and the NULL mechanism greeting/READY command.
 * This is an encoder only; the caller manages handshake ordering and peer validation.
 *
 * <p>Input indices are unchanged. Data frames retain a view of the readable payload;
 * do not modify that memory until the encoded output is released. The caller may
 * release its own input reference immediately after encoding. Commands copy their
 * data and borrow input only during the call. Every result is owned by the caller, who must
 * release it or transfer it to the transport. Partially encoded results are released
 * on failure. Inputs must not be modified or released concurrently with encoding.
 *
 * @see <a href="https://rfc.zeromq.org/spec/37/">Wire specification</a>
 */
public final class Encoder {
  private static final int MORE = 0x01;
  private static final int LONG = 0x02;
  private static final int COMMAND = 0x04;

  private final ByteBufAllocator allocator;

  public Encoder(ByteBufAllocator allocator) {
    this.allocator = Objects.requireNonNull(allocator, "allocator");
  }

  /** Encodes one data frame. Set more for every frame except the last in a message. */
  public ByteBuf message(ByteBuf body, boolean more) {
    Objects.requireNonNull(body, "body");
    int length = body.readableBytes();
    int headerSize = length > 255 ? 9 : 2;
    Math.addExact(headerSize, length);
    if (length == 0) {
      return frame(more ? MORE : 0, 0, out -> {});
    }
    var out = allocator.compositeBuffer(2);
    boolean success = false;
    try {
      // addComponent takes ownership even when adding a component fails.
      out.addComponent(true, allocate(headerSize,
          header -> writeHeader(header, more ? MORE : 0, length)));
      out.addComponent(true, body.retainedSlice(body.readerIndex(), length));
      success = true;
      return out;
    } finally {
      if (!success) {
        out.release();
      }
    }
  }

  /**
   * Appends a frame to a caller-owned wire buffer, copying the readable body without
   * changing its indices or ownership. Source and destination must be distinct storage.
   * Rolls the destination writer index back on failure; creates no per-frame wrapper.
   */
  public static void appendMessage(ByteBuf out, ByteBuf body, boolean more) {
    Objects.requireNonNull(out, "out");
    Objects.requireNonNull(body, "body");
    int length = body.readableBytes();
    int start = out.writerIndex();
    out.ensureWritable(Math.addExact(length > 255 ? 9 : 2, length));
    try {
      writeHeader(out, more ? MORE : 0, length);
      out.writeBytes(body, body.readerIndex(), length);
    } catch (RuntimeException | Error failure) {
      out.writerIndex(start);
      throw failure;
    }
  }

  /** Encodes a binary subscription prefix; an empty prefix subscribes to all messages. */
  public ByteBuf subscribe(ByteBuf prefix) {
    return command(Command.SUBSCRIBE, prefix);
  }

  /** Encodes cancellation of a binary subscription prefix. */
  public ByteBuf cancel(ByteBuf prefix) {
    return command(Command.CANCEL, prefix);
  }

  /** Encodes a known command using its cached wire name without string conversion. */
  public ByteBuf command(Command command, ByteBuf data) {
    Objects.requireNonNull(command, "command");
    Objects.requireNonNull(data, "data");
    if (command == Command.UNKNOWN) {
      throw new IllegalArgumentException("Use a string name for extension commands");
    }
    int length = data.readableBytes();
    return frame(COMMAND, Math.addExact(1 + command.length(), length), out -> {
      command.writeName(out);
      out.writeBytes(data, data.readerIndex(), length);
    });
  }

  /**
   * Encodes a single command frame. The name must contain 1 to 255 ASCII letters.
   * Data is command-specific and excludes the command name and its length byte.
   */
  public ByteBuf command(String name, ByteBuf data) {
    Command known = Command.find(Objects.requireNonNull(name, "name"));
    if (known != Command.UNKNOWN) {
      return command(known, data);
    }
    byte[] nameBytes = nameBytes(name, false);
    Objects.requireNonNull(data, "data");
    int dataLength = data.readableBytes();
    int bodyLength = Math.addExact(1 + nameBytes.length, dataLength);
    return frame(COMMAND, bodyLength, out -> {
      out.writeByte(nameBytes.length);
      out.writeBytes(nameBytes);
      out.writeBytes(data, data.readerIndex(), dataLength);
    });
  }

  /**
   * Encodes the complete 64-byte version 3.1 NULL greeting, without legacy
   * negotiation. This greeting is not wrapped in a message frame.
   */
  public ByteBuf greeting() {
    return allocate(64, out -> {
      out.writeByte(0xff);
      out.writeZero(8);
      out.writeByte(0x7f);
      out.writeByte(3);
      out.writeByte(1);
      out.writeBytes(new byte[] {'N', 'U', 'L', 'L'});
      out.writeZero(16); // mechanism is padded to 20 bytes
      out.writeByte(0); // NULL has no security server role
      out.writeZero(31);
    });
  }

  /**
   * Encodes NULL handshake metadata, e.g. Socket-Type = ASCII "SUB".
   * Property names are case-insensitive and must be unique. Values are binary.
   * Properties are written in the map's iteration order. This method encodes
   * metadata syntax; the caller supplies the appropriate socket type/properties.
   */
  public ByteBuf ready(Map<String, byte[]> metadata) {
    Objects.requireNonNull(metadata, "metadata");
    var names = new HashSet<String>();
    int bodyLength = 6; // length byte + READY
    for (var entry : metadata.entrySet()) {
      byte[] name = nameBytes(entry.getKey(), true);
      if (!names.add(entry.getKey().toLowerCase(Locale.ROOT))) {
        throw new IllegalArgumentException("Duplicate metadata name: " + entry.getKey());
      }
      byte[] value = Objects.requireNonNull(entry.getValue(), "metadata value");
      bodyLength = Math.addExact(bodyLength, Math.addExact(5 + name.length, value.length));
    }
    return frame(COMMAND, bodyLength, out -> {
      Command.READY.writeName(out);
      for (var entry : metadata.entrySet()) {
        byte[] name = nameBytes(entry.getKey(), true);
        out.writeByte(name.length);
        out.writeBytes(name);
        out.writeInt(entry.getValue().length);
        out.writeBytes(entry.getValue());
      }
    });
  }

  private ByteBuf frame(int flags, int bodyLength, Consumer<ByteBuf> writer) {
    boolean longFrame = bodyLength > 255;
    int capacity = Math.addExact(longFrame ? 9 : 2, bodyLength);
    return allocate(capacity, out -> {
      writeHeader(out, flags, bodyLength);
      writer.accept(out);
    });
  }

  private static void writeHeader(ByteBuf out, int flags, int bodyLength) {
    out.writeByte(flags | (bodyLength > 255 ? LONG : 0));
    if (bodyLength > 255) {
      out.writeLong(bodyLength);
    } else {
      out.writeByte(bodyLength);
    }
  }

  private ByteBuf allocate(int capacity, Consumer<ByteBuf> writer) {
    ByteBuf out = allocator.buffer(capacity, capacity);
    boolean success = false;
    try {
      writer.accept(out);
      success = true;
      return out;
    } finally {
      if (!success) {
        out.release();
      }
    }
  }

  private static byte[] nameBytes(String name, boolean metadata) {
    Objects.requireNonNull(name, "name");
    if (name.isEmpty() || name.length() > 255) {
      throw new IllegalArgumentException("Name must contain 1 to 255 ASCII characters");
    }
    for (int i = 0; i < name.length(); i++) {
      char c = name.charAt(i);
      boolean letter = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z');
      boolean propertyChar = (c >= '0' && c <= '9') || c == '-' || c == '_'
          || c == '.' || c == '+';
      if (!letter && !(metadata && propertyChar)) {
        throw new IllegalArgumentException("Invalid name: " + name);
      }
    }
    return name.getBytes(StandardCharsets.US_ASCII);
  }
}
