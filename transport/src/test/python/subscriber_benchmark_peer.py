"""Native libzmq PUB load source; stdin/stdout carry only phase-level control."""
import signal
import struct
import sys
import time
import zmq

if hasattr(signal, 'alarm'):
    signal.alarm(120)

multipart = len(sys.argv) > 1 and sys.argv[1] == 'multipart'
topic = bytes(8)

with zmq.Context() as context:
    with context.socket(zmq.PUB) as publisher:
        publisher.setsockopt(zmq.LINGER, 0)
        publisher.setsockopt(zmq.SNDHWM, 8192)
        port = publisher.bind_to_random_port('tcp://127.0.0.1')
        print('PORT', port, 'pyzmq=' + zmq.__version__, 'libzmq=' + zmq.zmq_version(), flush=True)
        for line in sys.stdin:
            parts = line.split()
            if parts[0] == 'QUIT':
                break
            phase = int(parts[1])
            if parts[0] == 'MARK':
                marker = bytes([127, phase]) + bytes(198)
                if multipart:
                    publisher.send_multipart([topic, marker])
                else:
                    publisher.send(marker)
                print('MARKED', flush=True)
            elif parts[0] == 'RUN':
                payload = bytearray(200)
                payload[0] = phase
                payload[-1] = phase
                offered = 0
                start = time.monotonic_ns()
                deadline = start + int(float(parts[2]) * 1_000_000_000)
                while time.monotonic_ns() < deadline:
                    for _ in range(64):
                        struct.pack_into('>Q', payload, 1, offered)
                        # copy=True makes reuse safe. PUB may silently drop on HWM.
                        if multipart:
                            publisher.send_multipart([topic, payload], copy=True)
                        else:
                            publisher.send(payload, copy=True)
                        offered += 1
                print('DONE', offered, time.monotonic_ns() - start, flush=True)
            else:
                raise AssertionError(parts)
