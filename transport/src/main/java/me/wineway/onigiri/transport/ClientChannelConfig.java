package me.wineway.onigiri.transport;

/** Immutable client settings. Time values are in milliseconds; batch size counts messages. */
public record ClientChannelConfig(
    long initialReconnectBackoffMillis,
    long maxReconnectBackoffMillis,
    int outboundBufferMaxLength,
    int writeBatchSize,
    int connectTimeoutMillis) {

  public ClientChannelConfig() {
    this(100, 30_000, 1024, 64, 5000);
  }

  public ClientChannelConfig {
    if (initialReconnectBackoffMillis <= 0) {
      throw new IllegalArgumentException("initialReconnectBackoffMillis must be positive");
    }
    if (maxReconnectBackoffMillis < initialReconnectBackoffMillis) {
      throw new IllegalArgumentException("maxReconnectBackoffMillis must be at least the initial backoff");
    }
    if (outboundBufferMaxLength < 1) {
      throw new IllegalArgumentException("inboundBufferMaxLength must be at least 1");
    }
    if (writeBatchSize <= 0) {
      throw new IllegalArgumentException("writeBatchSize must be positive");
    }
    // Zero disables Netty's connection timeout.
    if (connectTimeoutMillis < 0) {
      throw new IllegalArgumentException("connectTimeoutMillis must be non-negative");
    }
  }
}
