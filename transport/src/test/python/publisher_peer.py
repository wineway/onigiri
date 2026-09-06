"""Real libzmq peer. stdout/stdin are the Java test's publication control channel."""
import signal
import sys
import time
import zmq

# Bound subprocess lifetime even if the control pipe stops responding.
if hasattr(signal, "alarm"):
    signal.alarm(35)

endpoint = sys.argv[1]
context = zmq.Context()
sockets = []


def sub(prefixes):
    sock = context.socket(zmq.SUB)
    sockets.append(sock)
    sock.setsockopt(zmq.LINGER, 0)
    sock.setsockopt(zmq.HEARTBEAT_IVL, 100)
    sock.setsockopt(zmq.HEARTBEAT_TIMEOUT, 2000)
    for prefix in prefixes:
        sock.setsockopt(zmq.SUBSCRIBE, prefix)
    sock.connect(endpoint)
    return sock


def publish(messages):
    assert messages
    print('SEND', ','.join(m.hex() for m in messages), flush=True)
    assert sys.stdin.readline().strip() == 'SENT'


def drain(sock):
    while sock.poll(0):
        sock.recv_multipart()


serial = 0


def sync(sock):
    # Subscription commands and this unique barrier travel on the same ZMTP pipe.
    # Receiving the marker proves earlier updates reached the publisher; READY alone doesn't.
    global serial
    serial += 1
    marker = b'\xfeSYNC' + serial.to_bytes(4, 'big')
    sock.setsockopt(zmq.SUBSCRIBE, marker)
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        publish([marker])
        if sock.poll(50) and sock.recv_multipart() == [marker]:
            sock.setsockopt(zmq.UNSUBSCRIBE, marker)
            drain(sock)
            return
    raise AssertionError('Subscription barrier timed out')


def check(sock, expected):
    for message in expected:
        assert sock.poll(3000), ('Missing message', message[:16])
        actual = sock.recv_multipart()
        assert actual == [message], (actual, message)
    assert not sock.poll(100), 'Unexpected or duplicate message'


try:
    print('VERSION', zmq.__version__, zmq.zmq_version(), flush=True)
    init_first = sub([b'!init'])
    assert init_first.poll(3000) and init_first.recv_multipart() == [b'!init', b'native-init']
    init_second = sub([b'!init'])
    assert init_second.poll(3000) and init_second.recv_multipart() == [b'!init', b'native-init']
    assert not init_first.poll(100), 'Initialization was broadcast to an existing subscriber'
    print('CHECK subscription-callback connection-directed-init', flush=True)
    a = sub([b'\x00\xff', b'\x00\xffA'])
    b = sub([b'B'])
    sync(a)
    sync(b)
    for size in (2, 200, 255, 256, 4096):
        first = (b'\x00\xffA' + b'x' * size)[:size]
        second = b'B' + b'y' * (size - 1)
        ignored = b'C' + b'z' * (size - 1)
        publish([first, second, ignored])
        check(a, [first])
        check(b, [second])
    print('CHECK binary-prefix overlap-dedup multi-subscriber short-long-batch', flush=True)

    def multipart(messages):
        print('MULTI', ';'.join(','.join(p.hex() or '-' for p in m) for m in messages), flush=True)
        assert sys.stdin.readline().strip() == 'SENT'

    for size in (0, 200, 255, 256, 4096):
        first = [b'\x00\xff', b'B' * size]
        second = [b'B', b'', b'\x00\xff' + b'x' * size]
        multipart([first, second, [b'C', b'B']])
        assert a.poll(3000) and a.recv_multipart() == first
        assert b.poll(3000) and b.recv_multipart() == second
        assert not a.poll(50) and not b.poll(50)
    multipart([[b'\x00\xffA' + b'x' * 125], [b'B', b'x' * 256], [b'C', b'B']])
    assert a.poll(3000) and a.recv_multipart() == [b'\x00\xffA' + b'x' * 125]
    assert b.poll(3000) and b.recv_multipart() == [b'B', b'x' * 256]
    assert not a.poll(50) and not b.poll(50)
    print('CHECK variable-size mixed-single-multipart batch filtering', flush=True)
    print('CHECK multipart whole-message-filtering empty-parts long-parts', flush=True)

    a.setsockopt(zmq.SUBSCRIBE, b'D')
    a.setsockopt(zmq.SUBSCRIBE, b'D')
    a.setsockopt(zmq.UNSUBSCRIBE, b'D')
    sync(a)
    publish([b'Duplicate'])
    check(a, [b'Duplicate'])
    a.setsockopt(zmq.UNSUBSCRIBE, b'D')
    a.setsockopt(zmq.UNSUBSCRIBE, b'absent')
    sync(a)
    publish([b'Duplicate'])
    check(a, [])
    print('CHECK duplicate-subscribe balanced-cancel absent-cancel', flush=True)

    a.setsockopt(zmq.UNSUBSCRIBE, b'\x00\xff')
    sync(a)
    publish([b'\x00\xffAx', b'\x00\xffBx'])
    check(a, [b'\x00\xffAx'])
    a.setsockopt(zmq.SUBSCRIBE, b'')
    sync(a)
    publish([b'111', b'222'])
    check(a, [b'111', b'222'])
    a.setsockopt(zmq.UNSUBSCRIBE, b'')
    sync(a)
    publish([b'111'])
    check(a, [])
    print('CHECK overlap-cancel empty-prefix subscribe-unsubscribe', flush=True)

    a.disconnect(endpoint)
    a.connect(endpoint)
    sync(a)
    publish([b'\x00\xffAreconnected'])
    check(a, [b'\x00\xffAreconnected'])
    # Stay idle through multiple heartbeat intervals, then prove the connection is alive.
    assert not a.poll(1200)
    publish([b'\x00\xffAheartbeat'])
    check(a, [b'\x00\xffAheartbeat'])
    print('CHECK reconnect-restoration bidirectional-heartbeat', flush=True)
    monitor = a.get_monitor_socket(events=zmq.EVENT_DISCONNECTED)
    sockets.append(monitor)
    print('CLOSE', flush=True)
    assert sys.stdin.readline().strip() == 'CLOSED'
    assert monitor.poll(5000), 'Native SUB did not observe publisher shutdown'
    from zmq.utils.monitor import recv_monitor_message
    assert recv_monitor_message(monitor)['event'] == zmq.EVENT_DISCONNECTED
    print('CHECK publisher-close-observed-by-native-peer', flush=True)
    print('PASS', flush=True)
finally:
    for sock in sockets:
        sock.close()
    context.term()
