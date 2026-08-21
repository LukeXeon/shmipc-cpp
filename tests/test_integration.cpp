// C++ translation of shmipc-rs tests/test.rs (end-to-end ping-pong and the
// fallback-before-close scenario over a unix domain socket rendezvous).

#include <gtest/gtest.h>

#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <random>
#include <span>
#include <string>
#include <thread>
#include <vector>

#include "shmipc/buffer/linked.hpp"
#include "shmipc/buffer/slice.hpp"
#include "shmipc/config.hpp"
#include "shmipc/error.hpp"
#include "shmipc/listener.hpp"
#include "shmipc/session_config.hpp"
#include "shmipc/session_manager.hpp"
#include "shmipc/stream.hpp"
#include "shmipc/transport.hpp"

using namespace shmipc;
using namespace std::chrono_literals;

namespace {

std::uint64_t random_u64() {
  std::random_device rd;
  return (static_cast<std::uint64_t>(rd()) << 32) ^ rd();
}

SessionManagerConfig benchmark_config() {
  SessionManagerConfig c;
  c.config_mut().queue_cap = 65536;
  c.config_mut().connection_write_timeout = std::chrono::seconds(1);
  c.config_mut().share_memory_buffer_cap = 256 << 20;
  c.config_mut().mem_map_type = MemMapType::kMemFd;
  return c;
}

// Mirrors write_empty_slice from tests/test.rs.
std::uint32_t write_empty_slice(buffer::BufferSlice& slice, std::uint32_t size) {
  slice.write_index += size;
  if (slice.write_index > slice.cap) {
    const std::uint32_t wrote =
        slice.cap - (static_cast<std::uint32_t>(slice.write_index) - size);
    slice.write_index = slice.cap;
    return wrote;
  }
  return size;
}

// Mirrors write_empty_buffer from tests/test.rs.
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

// Mirrors must_write from tests/test.rs.
void must_write(Stream& s, std::uint32_t size) {
  write_empty_buffer(s.send_buf(), size);
  while (true) {
    auto r = s.flush(false);
    if (!r) {
      if (r.error() == Err::kQueueFull) {
        std::this_thread::sleep_for(std::chrono::microseconds(1));
        continue;
      }
      ADD_FAILURE() << "must write err:" << r.error().message();
      return;
    }
    return;
  }
}

// Mirrors must_read from tests/test.rs.
bool must_read(Stream& s, std::uint32_t size) {
  auto r = s.discard(size);
  if (!r) {
    if (r.error() == Err::kStreamClosed || r.error() == Err::kEndOfStream) {
      return false;
    }
    ADD_FAILURE() << "must read err:" << r.error().message();
    return false;
  }
  return true;
}

}  // namespace

TEST(Integration, PingPongByShmipc) {
  const std::uint64_t rnd = random_u64();
  const std::string path = "/dev/shm/shmipc" + std::to_string(rnd) + ".sock";
  auto sm_config = benchmark_config();
  const std::uint32_t size = 1 << 20;

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
  ASSERT_TRUE(server.has_value());
  const auto start = std::chrono::steady_clock::now();

  std::atomic<bool> server_done{false};
  std::thread server_thread([&] {
    auto stream = server->accept();
    ASSERT_TRUE(stream.has_value());
    Stream s = std::move(*stream);
    ASSERT_TRUE(must_read(s, size));
    s.recv_buf().release_previous_read();
    must_write(s, size);
    server_done = true;
  });

  std::thread client_thread([&] {
    auto client = SessionManager<DefaultUnixConnect>::create(
        sm_config, DefaultUnixConnect{},
        UnixAddr::from_pathname(path).value());
    ASSERT_TRUE(client.has_value());
    auto stream = client->get_stream();
    ASSERT_TRUE(stream.has_value());
    must_write(*stream, size);
    ASSERT_TRUE(must_read(*stream, size));
    stream->release_read_and_reuse();
    ASSERT_TRUE(stream->close().has_value());
    client->close();
  });

  server_thread.join();
  client_thread.join();
  server->close();

  const auto elapsed = std::chrono::steady_clock::now() - start;
  std::printf("elapsed: %lld ms\n",
              static_cast<long long>(
                  std::chrono::duration_cast<std::chrono::milliseconds>(
                      elapsed)
                      .count()));
  EXPECT_TRUE(server_done.load());
}

TEST(Integration, FallbackDataBeforeStreamClose) {
  const std::uint64_t rnd = random_u64();
  const std::string path = "/dev/shm/shmipc" + std::to_string(rnd) + ".sock";
  const std::uint32_t size = 2 << 20;
  auto sm_config = benchmark_config();

  sm_config.config_mut().share_memory_buffer_cap = 1 << 20;
  sm_config.config_mut().buffer_slice_sizes = {{4096, 100}};
  sm_config.config_mut().share_memory_path_prefix += std::to_string(rnd);
  sm_config.with_session_num(1);

  auto server = Listener::start(
      DefaultUnixListen{}, UnixAddr::from_pathname(path).value(),
      sm_config.config());
  ASSERT_TRUE(server.has_value());

  std::atomic<bool> server_ok{false};
  std::thread server_thread([&] {
    auto stream = server->accept();
    ASSERT_TRUE(stream.has_value());
    Stream s = std::move(*stream);
    EXPECT_TRUE(must_read(s, size));
    EXPECT_FALSE(must_read(s, 1));
    server_ok = true;
  });

  std::thread client_thread([&] {
    auto client = SessionManager<DefaultUnixConnect>::create(
        sm_config, DefaultUnixConnect{},
        UnixAddr::from_pathname(path).value());
    ASSERT_TRUE(client.has_value());
    auto stream = client->get_stream();
    ASSERT_TRUE(stream.has_value());
    must_write(*stream, size);
    EXPECT_TRUE(stream->fallback_state());
    ASSERT_TRUE(stream->close().has_value());
    client->close();
  });

  server_thread.join();
  client_thread.join();
  server->close();
  EXPECT_TRUE(server_ok.load());
}
