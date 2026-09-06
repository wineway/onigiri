"""Native ZeroMQ PUB controlled by the Java subscriber interop test."""
import signal
import sys
import zmq

# Bound subprocess lifetime even if the control pipe stops responding.
if hasattr(signal, "alarm"):
    signal.alarm(35)

with zmq.Context() as context:
    with context.socket(zmq.PUB) as publisher:
        publisher.setsockopt(zmq.LINGER, 0)
        publisher.setsockopt(zmq.HEARTBEAT_IVL, 100)
        publisher.setsockopt(zmq.HEARTBEAT_TIMEOUT, 2000)
        port = publisher.bind_to_random_port('tcp://127.0.0.1')
        print('PORT', port, zmq.__version__, zmq.zmq_version(), flush=True)
        for line in sys.stdin:
            if line.strip() == 'QUIT':
                break
            operation, payload = line.split()
            if operation == 'MULTI':
                publisher.send_multipart([bytes.fromhex(p) if p != '-' else b'' for p in payload.split(',')])
            else:
                assert operation == 'SEND'
                publisher.send(bytes.fromhex(payload))
            print('SENT', flush=True)
