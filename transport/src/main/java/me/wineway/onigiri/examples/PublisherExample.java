package me.wineway.onigiri.examples;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import io.netty.buffer.Unpooled;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import me.wineway.onigiri.publisher.PubServer;
import me.wineway.onigiri.publisher.PubServerConfig;

/** Publishes each non-empty stdin line as one UTF-8 message; EOF closes the server. */
public final class PublisherExample {
  private PublisherExample() {}

  public static void main(String[] args) throws Exception {
    int port = args.length == 0 ? 5555 : Integer.parseInt(args[0]);
    var group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    var publisher = new PubServer(group, group, new PubServerConfig(), new PubServer.Listener() {
      @Override
      public void onReady(SocketAddress subscriber) {
        System.out.println("Subscriber handshake completed: " + subscriber);
      }
      @Override
      public void onError(SocketAddress subscriber, Throwable failure) {
        System.err.println("Subscriber " + subscriber + ": " + failure);
      }
    });
    try {
      publisher.bind(new InetSocketAddress("127.0.0.1", port)).get(5, TimeUnit.SECONDS);
      System.out.println("Listening on " + publisher.localAddress() + "; type messages, EOF to stop.");
      var input = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
      String line;
      while ((line = input.readLine()) != null) {
        if (!line.isEmpty()) publisher.publish(Unpooled.wrappedBuffer(line.getBytes(StandardCharsets.UTF_8)));
      }
    } finally {
      try {
        publisher.closeAsync().get(5, TimeUnit.SECONDS);
      } finally {
        group.shutdownGracefully(0, 5, TimeUnit.SECONDS).sync();
      }
    }
  }
}
