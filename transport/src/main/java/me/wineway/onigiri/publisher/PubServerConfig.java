package me.wineway.onigiri.publisher;

/** Publisher control-plane and batching limits. Time values are milliseconds. */
public record PubServerConfig(
    int laneQueueCapacity,
    int laneDrainLimit,
    int maxBatchMessages,
    int maxSubscribersPerLane,
    int handshakeTimeoutMillis,
    int heartbeatIntervalMillis,
    int heartbeatTimeoutMillis,
    int heartbeatTtlMillis,
    int maxSubscriptionsPerSession,
    int maxSubscriptionPrefixBytes,
    int maxPartsPerMessage,
    int maxMessageBytes,
    int maxBatchBytes,
    int subscriptionCallbackCapacity) {

  public PubServerConfig() {
    this(1024, 64, 256, 4096, 5000, 1000, 5000, 5000);
  }

  public PubServerConfig(int laneQueueCapacity, int laneDrainLimit, int maxBatchMessages,
      int maxSubscribersPerLane, int handshakeTimeoutMillis, int heartbeatIntervalMillis,
      int heartbeatTimeoutMillis, int heartbeatTtlMillis) {
    this(laneQueueCapacity, laneDrainLimit, maxBatchMessages, maxSubscribersPerLane,
        handshakeTimeoutMillis, heartbeatIntervalMillis, heartbeatTimeoutMillis,
        heartbeatTtlMillis, Integer.MAX_VALUE, 4096);
  }

  /** Compatibility constructor using the default subscriber capacity. */
  public PubServerConfig(int laneQueueCapacity, int laneDrainLimit, int maxBatchMessages,
      int handshakeTimeoutMillis, int heartbeatIntervalMillis, int heartbeatTimeoutMillis,
      int heartbeatTtlMillis) {
    this(laneQueueCapacity, laneDrainLimit, maxBatchMessages, 4096, handshakeTimeoutMillis,
        heartbeatIntervalMillis, heartbeatTimeoutMillis, heartbeatTtlMillis);
  }

  public PubServerConfig(int laneQueueCapacity, int laneDrainLimit, int maxBatchMessages,
      int maxSubscribersPerLane, int handshakeTimeoutMillis, int heartbeatIntervalMillis,
      int heartbeatTimeoutMillis, int heartbeatTtlMillis, int maxSubscriptionsPerSession,
      int maxSubscriptionPrefixBytes) {
    this(laneQueueCapacity, laneDrainLimit, maxBatchMessages, maxSubscribersPerLane,
        handshakeTimeoutMillis, heartbeatIntervalMillis, heartbeatTimeoutMillis,
        heartbeatTtlMillis, maxSubscriptionsPerSession, maxSubscriptionPrefixBytes,
        64, 16 * 1024 * 1024, 32 * 1024 * 1024);
  }

  public PubServerConfig(int laneQueueCapacity, int laneDrainLimit, int maxBatchMessages,
      int maxSubscribersPerLane, int handshakeTimeoutMillis, int heartbeatIntervalMillis,
      int heartbeatTimeoutMillis, int heartbeatTtlMillis, int maxSubscriptionsPerSession,
      int maxSubscriptionPrefixBytes, int maxPartsPerMessage, int maxMessageBytes, int maxBatchBytes) {
    this(laneQueueCapacity, laneDrainLimit, maxBatchMessages, maxSubscribersPerLane,
        handshakeTimeoutMillis, heartbeatIntervalMillis, heartbeatTimeoutMillis, heartbeatTtlMillis,
        maxSubscriptionsPerSession, maxSubscriptionPrefixBytes, maxPartsPerMessage,
        maxMessageBytes, maxBatchBytes, 65536);
  }

  public PubServerConfig {
    if (subscriptionCallbackCapacity <= 0) {
      throw new IllegalArgumentException("subscriptionCallbackCapacity must be positive");
    }
    if (maxPartsPerMessage < 2 || maxMessageBytes < 0 || maxBatchBytes <= 0
        || (long) maxMessageBytes + 9L * maxPartsPerMessage > maxBatchBytes) {
      throw new IllegalArgumentException("Invalid multipart limits or insufficient batch capacity");
    }
    if (laneQueueCapacity < 2 || Integer.bitCount(laneQueueCapacity) != 1) {
      throw new IllegalArgumentException("laneQueueCapacity must be a power of two");
    }
    if (maxSubscriptionsPerSession <= 0 || maxSubscriptionPrefixBytes < 0) {
      throw new IllegalArgumentException("Invalid subscription limits");
    }
    if (laneDrainLimit <= 0 || maxBatchMessages <= 0 || maxSubscribersPerLane <= 0) {
      throw new IllegalArgumentException("Lane limits must be positive");
    }
    if (handshakeTimeoutMillis < 0 || heartbeatIntervalMillis < 0
        || heartbeatTimeoutMillis < 0 || heartbeatTtlMillis < 0
        || heartbeatTtlMillis > 6_553_599) {
      throw new IllegalArgumentException("Invalid protocol timeout");
    }
    if (heartbeatIntervalMillis == 0 && (heartbeatTimeoutMillis != 0 || heartbeatTtlMillis != 0)) {
      throw new IllegalArgumentException("Heartbeat timeout/advertised TTL require an interval");
    }
  }
}
