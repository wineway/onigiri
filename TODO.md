# TODO

## Allow publisher sessions to use different EventLoops

Keep all sessions of one `SubClient` on its single EventLoop for now. Revisit this
only if profiling shows that the EventLoop is a bottleneck.

Moving sessions onto different EventLoops requires an explicit synchronization
design; changing `group.next()` alone is unsafe. Before enabling it:

- Keep the session registry and subscription changes on a control EventLoop.
- Publish subscriptions as immutable, versioned snapshots.
- Let each session reconcile the latest snapshot on its own EventLoop before it
  becomes active after READY.
- Send later subscription changes to each session with their snapshot version and
  ignore stale updates.
- Track each session's applied subscription set to prevent duplicate SUBSCRIBE and
  CANCEL commands around the READY race.
- Decide whether public listener callbacks remain serialized on the control
  EventLoop or may run concurrently on session EventLoops. If frames cross loops,
  retain and release their `ByteBuf` ownership explicitly.
- Keep connect, disconnect, close, ready-count and terminal ERROR removal ordered
  through the control EventLoop.

Required race coverage:

- subscribe/unsubscribe immediately before, during and after READY restoration;
- concurrent subscription changes from multiple caller threads;
- reconnect while subscription updates are pending;
- one session closing while an update is broadcast to other sessions;
- client close racing with session READY and session termination.
