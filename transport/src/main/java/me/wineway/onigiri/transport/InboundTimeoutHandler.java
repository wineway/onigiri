package me.wineway.onigiri.transport;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.concurrent.ScheduledFuture;

/** Per-connection byte-level liveness tracking, before protocol decoding. */
public final class InboundTimeoutHandler extends ChannelInboundHandlerAdapter {
  private ChannelHandlerContext context;
  private ScheduledFuture<?> responseTimer;
  private ScheduledFuture<?> idleTimer;
  private long idleNanos;
  private long lastInboundNanos;

  @Override
  public void handlerAdded(ChannelHandlerContext ctx) {
    context = ctx;
    lastInboundNanos = System.nanoTime();
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) {
    if (msg instanceof ByteBuf bytes && bytes.isReadable()) {
      lastInboundNanos = System.nanoTime();
      cancel(responseTimer);
      responseTimer = null;
    }
    ctx.fireChannelRead(msg);
  }

  public void expectInbound(int timeoutMillis) {
    if (timeoutMillis == 0) {
      cancel(responseTimer);
      responseTimer = null;
    } else if (responseTimer == null && context.channel().isActive()) {
      // Repeated probes do not extend an existing deadline.
      responseTimer = context.executor().schedule(() -> fail("Heartbeat"),
          timeoutMillis, TimeUnit.MILLISECONDS);
    }
  }

  public void setIdleTimeout(int timeoutMillis) {
    cancel(idleTimer);
    idleTimer = null;
    idleNanos = TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
    if (idleNanos > 0 && context.channel().isActive()) {
      idleTimer = context.executor().schedule(this::checkIdle, idleNanos, TimeUnit.NANOSECONDS);
    }
  }

  private void checkIdle() {
    long remaining = idleNanos - (System.nanoTime() - lastInboundNanos);
    if (remaining <= 0) {
      fail("Peer PING TTL");
    } else {
      idleTimer = context.executor().schedule(this::checkIdle, remaining, TimeUnit.NANOSECONDS);
    }
  }

  private void fail(String phase) {
    cancelTimers();
    if (context.channel().isActive()) {
      try {
        context.fireExceptionCaught(new TimeoutException(phase + " timed out"));
      } finally {
        context.close();
      }
    }
  }

  private void cancelTimers() {
    cancel(responseTimer);
    cancel(idleTimer);
    responseTimer = idleTimer = null;
    idleNanos = 0;
  }

  private static void cancel(ScheduledFuture<?> timer) {
    if (timer != null) {
      timer.cancel(false);
    }
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) {
    cancelTimers();
    ctx.fireChannelInactive();
  }

  @Override
  public void handlerRemoved(ChannelHandlerContext ctx) {
    cancelTimers();
  }
}
