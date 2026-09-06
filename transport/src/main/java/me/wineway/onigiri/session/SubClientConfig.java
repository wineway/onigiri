package me.wineway.onigiri.session;

/**
 * Protocol timeouts in milliseconds. Zero disables each local timer.
 * heartbeatTtlMillis is advertised to the peer in PING, rounded down to 100 ms.
 * A received peer TTL is enforced independently of these local settings.
 */
public record SubClientConfig(
    int handshakeTimeoutMillis,
    int heartbeatIntervalMillis,
    int heartbeatTimeoutMillis,
    int heartbeatTtlMillis,
    int maxPartsPerMessage,
    int maxMessageBytes,
    int multipartTimeoutMillis) {

  public SubClientConfig() {
    this(5000, 1000, 5000, 5000);
  }

  public SubClientConfig(int handshakeTimeoutMillis, int heartbeatIntervalMillis,
      int heartbeatTimeoutMillis, int heartbeatTtlMillis) {
    this(handshakeTimeoutMillis, heartbeatIntervalMillis, heartbeatTimeoutMillis,
        heartbeatTtlMillis, 64, 16 * 1024 * 1024, 5000);
  }

  public SubClientConfig {
    if (maxPartsPerMessage < 1 || maxMessageBytes < 0 || multipartTimeoutMillis < 0) {
      throw new IllegalArgumentException("Invalid multipart limits");
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
