// C++ translation of shmipc-rs benches/bench.rs.
//
// Criterion has no offline equivalent available, so this is a standalone
// timing harness running the same parallel ping-pong scenario: shmipc vs
// unix domain socket loopback, across message sizes 64 B .. 4 MiB.

#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "shmipc/shmipc.hpp"

using namespace shmipc;
using namespace std::chrono_literals;

namespace {

SessionManagerConfig benchmark_config() {
  SessionManagerConfig c;
  c.config_mut().queue_cap = 65536;
  c.config_mut().connection_write_timeout = std::chrono::seconds(1);
  c.config_mut().share_memory_buffer_cap = 256 << 20;
  c.config_mut().mem_map_type = MemMapType::kMemFd;
  return c;
}

std::uint32_t write_empty_slice(buffer::BufferSlice& slice,
                                std::uint32_t size) {
  slice.write_index += size;
  if (slice.write_index > slice.cap) {
    const std::uint32_t wrote =
        slice.cap - (static_cast<std::uint32_t>(slice.write_index) - size);
    slice.write_index = slice.cap;
    return wrote;
  }
  return size;
}

void write_empty_buffer(buffer::LinkedBuffer& l, std::uint32_t size) {
  if (size == 0) {
    return;
  }
  std::uint32_t wrote = 0;
  while (true) {
    if (l.slice_list().write() == nullptr) {
      l.alloc(size - wrote);
      l.slice_list_mut().set_write(l.slice_list_mut().front());
    }
    wrote += write_empty_slice(*l.slice_list_mut().write(), size - wrote);
    if (wrote < size) {
      if (l.slice_list().write()->next() == nullptr) {
        l.alloc(size - wrote);
      }
      l.slice_list_mut().set_write(l.slice_list_mut().write()->next());
    } else {
      break;
    }
  }
  l.len_mut() += size;
}

void must_write(Stream& s, std::uint32_t size) {
  write_empty_buffer(s.send_buf(), size);
  while (true) {
    auto r = s.flush(false);
    if (r) {
      return;
    }
    if (r.error() == Err::kQueueFull) {
      std::this_thread::sleep_for(std::chrono::microseconds(1));
      continue;
    }
    std::fprintf(stderr, "must write err: %s\n", r.error().message().c_str());
    std::exit(1);
  }
}

bool must_read(Stream& s, std::uint32_t size) {
  auto r = s.discard(size);
  if (r) {
    return true;
  }
  if (r.error() == Err::kStreamClosed || r.error() == Err::kEndOfStream) {
    return false;
  }
  std::fprintf(stderr, "must read err: %s\n", r.error().message().c_str());
  std::exit(1);
}

std::uint64_t random_u64() {
  std::random_device rd;
  return (static_cast<std::uint64_t>(rd()) << 32) ^ rd();
}

double bench_shmipc(std::uint32_t size, int concurrency, int iters) {
  const std::uint64_t rnd = random_u64();
  const std::string path = "/dev/shm/shmipc" + std::to_string(rnd) + ".sock";
  auto sm_config = benchmark_config();
  if (size >= (4u << 20)) {
    sm_config.config_mut().share_memory_buffer_cap = 1 << 30;
  }
  sm_config.config_mut().buffer_slice_sizes = {
      {size + 256, 70},
      {(16 << 10) + 256, 20},
      {(64 << 10) + 256, 10},
  };
  sm_config.config_mut().share_memory_path_prefix += std::to_string(rnd);
  sm_config.with_session_num(1);

  auto server = Listener::start(
      DefaultUnixListen{}, UnixAddr::from_pathname(path).value(),
      sm_config.config());
  if (!server) {
    std::fprintf(stderr, "listener error: %s\n",
                 server.error().message().c_str());
    std::exit(1);
  }
  std::atomic<bool> stop_server{false};
  std::thread accept_thread([&] {
    while (!stop_server.load()) {
      auto stream = server->accept();
      if (!stream) {
        return;
      }
      std::thread([s = std::move(*stream), size]() mutable {
        while (true) {
          if (!must_read(s, size)) {
            return;
          }
          s.recv_buf().release_previous_read();
          must_write(s, size);
        }
      }).detach();
    }
  });

  auto client = SessionManager<DefaultUnixConnect>::create(
      sm_config, DefaultUnixConnect{}, UnixAddr::from_pathname(path).value());
  if (!client) {
    std::fprintf(stderr, "session manager error: %s\n",
                 client.error().message().c_str());
    std::exit(1);
  }

  std::vector<std::thread> workers;
  const auto start = std::chrono::steady_clock::now();
  for (int i = 0; i < concurrency; ++i) {
    workers.emplace_back([client, size, iters] {
      auto stream = client->get_stream();
      if (!stream) {
        std::fprintf(stderr, "get_stream err: %s\n",
                     stream.error().message().c_str());
        std::exit(1);
      }
      for (int it = 0; it < iters; ++it) {
        must_write(*stream, size);
        must_read(*stream, size);
        stream->release_read_and_reuse();
      }
      (void)stream->close();
    });
  }
  for (auto& w : workers) {
    w.join();
  }
  const double us =
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now() - start)
          .count() /
      1000.0;

  client->close();
  stop_server = true;
  server->close();
  if (accept_thread.joinable()) {
    accept_thread.join();
  }
  return us / static_cast<double>(concurrency * iters);
}

void read_full_or_die(int fd, std::vector<std::uint8_t>& buf) {
  std::size_t got = 0;
  while (got < buf.size()) {
    const ssize_t n = ::read(fd, buf.data() + got, buf.size() - got);
    if (n <= 0) {
      std::exit(0);  // EOF: peer closed, just stop this worker
    }
    got += static_cast<std::size_t>(n);
  }
}

double bench_uds(std::uint32_t size, int concurrency, int iters) {
  const std::uint64_t rnd = random_u64();
  const std::string path = "/dev/shm/uds" + std::to_string(rnd) + ".sock";

  const int listen_fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
  sockaddr_un sa{};
  sa.sun_family = AF_UNIX;
  std::strncpy(sa.sun_path, path.c_str(), sizeof(sa.sun_path) - 1);
  ::unlink(path.c_str());
  if (::bind(listen_fd, reinterpret_cast<sockaddr*>(&sa), sizeof(sa)) != 0 ||
      ::listen(listen_fd, 512) != 0) {
    std::fprintf(stderr, "uds bind failed\n");
    std::exit(1);
  }
  std::thread accept_thread([listen_fd, size] {
    while (true) {
      const int conn = ::accept(listen_fd, nullptr, nullptr);
      if (conn < 0) {
        return;
      }
      std::thread([conn, size] {
        std::vector<std::uint8_t> read_buf(size);
        std::vector<std::uint8_t> write_buf(size, 0);
        while (true) {
          std::size_t got = 0;
          while (got < read_buf.size()) {
            const ssize_t n = ::read(conn, read_buf.data() + got,
                                     read_buf.size() - got);
            if (n <= 0) {
              ::close(conn);
              return;
            }
            got += static_cast<std::size_t>(n);
          }
          std::size_t written = 0;
          while (written < write_buf.size()) {
            const ssize_t n = ::write(conn, write_buf.data() + written,
                                      write_buf.size() - written);
            if (n <= 0) {
              ::close(conn);
              return;
            }
            written += static_cast<std::size_t>(n);
          }
        }
      }).detach();
    }
  });

  std::vector<std::thread> workers;
  const auto start = std::chrono::steady_clock::now();
  for (int i = 0; i < concurrency; ++i) {
    workers.emplace_back([path, size, iters] {
      const int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
      sockaddr_un sa{};
      sa.sun_family = AF_UNIX;
      std::strncpy(sa.sun_path, path.c_str(), sizeof(sa.sun_path) - 1);
      if (::connect(fd, reinterpret_cast<sockaddr*>(&sa), sizeof(sa)) != 0) {
        std::fprintf(stderr, "uds connect failed\n");
        std::exit(1);
      }
      std::vector<std::uint8_t> read_buf(size);
      std::vector<std::uint8_t> write_buf(size, 0);
      for (int it = 0; it < iters; ++it) {
        std::size_t written = 0;
        while (written < write_buf.size()) {
          const ssize_t n = ::write(fd, write_buf.data() + written,
                                    write_buf.size() - written);
          if (n <= 0) {
            std::exit(1);
          }
          written += static_cast<std::size_t>(n);
        }
        read_full_or_die(fd, read_buf);
      }
      ::close(fd);
    });
  }
  for (auto& w : workers) {
    w.join();
  }
  const double us =
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now() - start)
          .count() /
      1000.0;

  ::shutdown(listen_fd, SHUT_RDWR);
  ::close(listen_fd);
  if (accept_thread.joinable()) {
    accept_thread.join();
  }
  return us / static_cast<double>(concurrency * iters);
}

}  // namespace

int main(int argc, char** argv) {
  // Scaled-down knobs compared to the criterion bench (199 tasks x iters)
  // to keep the total runtime reasonable; override via argv.
  int concurrency = 32;
  int base_iters = 2000;
  if (argc > 1) {
    concurrency = std::atoi(argv[1]);
  }
  if (argc > 2) {
    base_iters = std::atoi(argv[2]);
  }

  const std::uint32_t sizes[] = {
      64,   512,   1024,  4096,  16 << 10,  32 << 10,
      64 << 10, 256 << 10, 512 << 10, 1 << 20, 4 << 20,
  };
  std::printf("%-42s %12s %12s %8s\n", "benchmark", "shmipc(us)", "uds(us)",
              "speedup");
  for (const std::uint32_t size : sizes) {
    // Keep runtime per size roughly constant.
    int iters = base_iters;
    while (iters > 1 &&
           static_cast<std::uint64_t>(iters) * size > 256ULL << 20) {
      iters /= 2;
    }
    const double shmipc_us = bench_shmipc(size, concurrency, iters);
    const double uds_us = bench_uds(size, concurrency, iters);
    char name[64];
    std::snprintf(name, sizeof(name),
                  "parallel_ping_pong_%ub", size);
    std::printf("%-42s %12.3f %12.3f %7.2fx\n", name, shmipc_us, uds_us,
                uds_us / shmipc_us);
  }
  return 0;
}
