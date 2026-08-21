// Copyright 2026
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// C++ translation of shmipc-rs `src/session/mod.rs` (Session/Shared and the
// event-handling loops). The tokio tasks become dedicated std::threads held
// by SessionShared; channels/notifications are the thread-mirrored sync
// primitives from shmipc/sync.

#pragma once

#include <unistd.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "shmipc/buffer/manager.hpp"
#include "shmipc/config.hpp"
#include "shmipc/consts.hpp"
#include "shmipc/error.hpp"
#include "shmipc/protocol/header.hpp"
#include "shmipc/queue.hpp"
#include "shmipc/session_config.hpp"
#include "shmipc/stats.hpp"
#include "shmipc/stream.hpp"
#include "shmipc/stream_pool.hpp"
#include "shmipc/sync/channel.hpp"
#include "shmipc/sync/event.hpp"
#include "shmipc/transport.hpp"

namespace shmipc {

// Mirror of SendReady: header bytes (Rust `Option<Header>`; always None in
// the current protocol, so an empty vector represents "no header" — the
// write loop still honors a non-empty header for parity), body, and the
// oneshot completion signal.
struct SendReady {
  std::vector<std::uint8_t> hdr;
  std::vector<std::uint8_t> body;
  std::promise<void> done;
};

struct SessionShared {
  Config config;
  // next_stream_id is the next stream we should send.
  // In client mode it is odd; in server mode even.
  std::atomic<std::uint32_t> next_stream_id{1};
  std::shared_ptr<buffer::BufferManager> buffer_manager;
  QueueManager queue_manager;
  std::uint8_t communication_version = 0;
  std::string name;
  StreamPool pool;
  mutable std::shared_mutex streams_mtx;
  std::unordered_map<std::uint32_t, Stream> streams;
  bool is_client = false;

  std::atomic<std::uint32_t> shutdown{0};
  std::atomic<std::uint32_t> unhealthy{0};

  mutable std::mutex shutdown_err_mtx;
  std::optional<Error> shutdown_err;

  Sender<SendReady> send_tx;
  Receiver<SendReady> send_channel;  // handle used to stop the write loop
  std::optional<Sender<Stream>> accept_tx;
  Receiver<Stream> accept_channel;  // handle used to stop recv_loop
  // Mirrors shutdown_notify (persistent shutdown flag semantics).
  Event shutdown_event;
  Stats stats;
  int conn_fd = -1;

  std::thread read_loop_thread;
  std::thread write_loop_thread;
  // Circuit breaker timer (Rust: tokio::spawn with 30s sleep).
  mutable std::mutex breaker_mtx;
  Event breaker_stop;
  std::thread breaker_thread;

  SessionShared(Config cfg, std::size_t max_stream_num)
      : config(std::move(cfg)), pool(max_stream_num) {}

  // Mirror of `impl Drop for Shared` (abort the backend tasks).
  ~SessionShared();
};

// Session wraps a reliable ordered connection multiplexed into streams.
// Cheap to copy (Rust Clone).
class Session {
 public:
  Session() = default;
  explicit Session(std::shared_ptr<SessionShared> shared)
      : shared_(std::move(shared)) {}

  bool valid() const noexcept { return shared_ != nullptr; }
  const std::shared_ptr<SessionShared>& shared() const noexcept {
    return shared_;
  }
  const std::string& name() const { return shared_->name; }

  // Client constructor (mirrors Session::client). Mutates sm_config exactly
  // like the Rust version (path prefix + queue path).
  template <typename C>
  static Result<Session> client(std::size_t session_id, std::uint64_t epoch_id,
                                std::uint64_t rand_id,
                                SessionManagerConfig& sm_config,
                                const C& connect,
                                const typename C::Address& addr);

  // Server constructor (mirrors Session::server).
  static Result<Session> server(Config config,
                                std::unique_ptr<TransportStream> conn_stream,
                                Sender<Stream> accept_tx);

  // Return whether the session is healthy.
  bool is_healthy() const;
  // Does a safe check to see if we have shutdown.
  bool is_closed() const;

  Result<Stream> get_or_open_stream(std::size_t session_id) const;
  void put_or_close_stream(Stream s) const;
  // Used to create a new stream.
  Result<Stream> open_stream(std::size_t session_id) const;
  // Attempts to send a GoAway before closing the connection.
  void close() const;

  Result<Unit> wait_for_send(std::optional<std::vector<std::uint8_t>> hdr,
                             std::vector<std::uint8_t> body) const;
  Result<Unit> wake_up_peer() const;
  void open_circuit_breaker() const;
  void on_stream_close(std::uint32_t id, std::uint32_t state) const;

  // Forward accepted streams to the listener (blocking; exits when the
  // accept channel is stopped by close()).
  void recv_loop(Receiver<Stream> rx,
                 Sender<IoResult<Stream>> stream_tx) const;

  // Event processing (blocking equivalents of the tokio loops).
  void read_loop(std::unique_ptr<ReadHalf> reader) const;
  void write_loop(std::unique_ptr<WriteHalf> writer,
                  Receiver<SendReady> send_rx) const;

  // Returns (consumed, required_next, error).
  struct EventResult {
    std::size_t consumed = 0;
    std::size_t required = kHeaderSize;
    std::optional<Error> err;
  };
  EventResult handle_events(std::span<const std::uint8_t> buf) const;

 private:
  static Result<Session> create(Config config,
                                std::unique_ptr<TransportStream> conn_stream,
                                std::optional<Sender<Stream>> accept_tx);

  // (consumed, required, stop, error)
  struct HandlerResult {
    std::size_t consumed = 0;
    std::size_t required = kHeaderSize;
    bool stop = false;
    std::optional<Error> err;
  };
  HandlerResult handle_polling(std::span<const std::uint8_t> buf) const;
  HandlerResult handle_fallback_data(
      const protocol::Header& event_header,
      std::span<const std::uint8_t> buf) const;
  HandlerResult handle_stream_close(std::span<const std::uint8_t> buf) const;
  std::optional<Stream> get_stream(std::uint32_t id, std::uint32_t state) const;
  Result<Unit> handle_stream_message(const Stream& stream,
                                     BufferSliceWrapper wrapper,
                                     std::uint32_t state) const;
  // Used to handle an error that is causing the session to terminate.
  void exit_err(Error err) const;

  std::shared_ptr<SessionShared> shared_;
};

// ---------------------------------------------------------------------------
// Template implementations
// ---------------------------------------------------------------------------

template <typename C>
Result<Session> Session::client(std::size_t session_id, std::uint64_t epoch_id,
                                std::uint64_t rand_id,
                                SessionManagerConfig& sm_config,
                                const C& connect,
                                const typename C::Address& addr) {
  auto conn = connect.connect(addr);
  if (!conn) {
    return std::unexpected(conn.error());
  }

  Config& cfg = sm_config.config_mut();
  cfg.share_memory_path_prefix += "_" + std::to_string(::getpid());
  if (cfg.mem_map_type == MemMapType::kDevShmFile &&
      cfg.share_memory_path_prefix.size() + kEpochInfoMaxLen +
              kQueueInfoMaxLen >
          kFileNameMaxLen) {
    return std::unexpected(Err::kFileNameTooLong);
  }
  if (epoch_id > 0) {
    cfg.share_memory_path_prefix +=
        "_epoch_" + std::to_string(epoch_id) + "_" + std::to_string(rand_id);
  }
  if (!cfg.share_memory_path_prefix.empty()) {
    cfg.queue_path = cfg.share_memory_path_prefix + "_queue_" +
                     std::to_string(session_id);
  }

  return Session::create(cfg, std::make_unique<typename C::Stream>(
                                  std::move(*conn)),
                         std::nullopt);
}

}  // namespace shmipc
