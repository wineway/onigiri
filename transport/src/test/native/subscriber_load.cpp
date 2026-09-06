#include <zmq.h>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <string>
#include <thread>
#include <stdexcept>

static void check(int rc) { if (rc < 0) throw std::runtime_error(zmq_strerror(zmq_errno())); }
int main() {
  void* context = zmq_ctx_new();
  void* pub = zmq_socket(context, ZMQ_PUB);
  try {
    int hwm = 8192, linger = 0;
    check(zmq_setsockopt(pub, ZMQ_SNDHWM, &hwm, sizeof(hwm)));
    check(zmq_setsockopt(pub, ZMQ_LINGER, &linger, sizeof(linger)));
    check(zmq_bind(pub, "tcp://127.0.0.1:*"));
    char endpoint[256]; size_t length = sizeof(endpoint);
    check(zmq_getsockopt(pub, ZMQ_LAST_ENDPOINT, endpoint, &length));
    int major, minor, patch; zmq_version(&major, &minor, &patch);
    std::cout << endpoint << " libzmq=" << major << '.' << minor << '.' << patch << std::endl;
    std::string command;
    while (std::cin >> command && command != "QUIT") {
      int phase; std::cin >> phase;
      unsigned char body[408] = {};
      if (command == "MARK") {
        body[0] = 127; body[1] = phase;
        check(zmq_send(pub, body, 64, 0));
        std::cout << "MARKED" << std::endl;
      } else if (command == "RUN") {
        int seconds; uint64_t rate; std::string mode; std::cin >> seconds >> mode >> rate;
        const int sizes[] = {64, 128, 200, 408}; // Mean 200 bytes; includes long ZMTP frames.
        uint64_t count = 0, bytes = 0;
        auto start = std::chrono::steady_clock::now();
        auto end = start + std::chrono::seconds(seconds);
        do {
          for (int i = 0; i < 256; ++i) {
            int size = mode == "fixed" ? 200 : sizes[count & 3];
            body[0] = phase;
            for (int b = 0; b < 8; ++b) body[1 + b] = count >> ((7 - b) * 8);
            body[size - 1] = phase;
            check(zmq_send(pub, body, size, 0));
            ++count; bytes += size;
          }
          if (rate != 0) std::this_thread::sleep_until(start + std::chrono::nanoseconds(count * 1000000000ULL / rate));
        } while (std::chrono::steady_clock::now() < end);
        std::cout << "DONE " << count << ' ' << bytes << std::endl;
      } else throw std::runtime_error("Unknown command");
    }
  } catch (const std::exception& error) {
    std::cerr << error.what() << std::endl;
    zmq_close(pub); zmq_ctx_term(context); return 1;
  }
  zmq_close(pub); zmq_ctx_term(context);
}
