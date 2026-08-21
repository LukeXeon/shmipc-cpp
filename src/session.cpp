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

#include "shmipc/session.hpp"

#include <sys/socket.h>

#include <cassert>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <utility>
#include <vector>

#include "shmipc/consts.hpp"
#include "shmipc/log.hpp"
#include "shmipc/protocol/event.hpp"
#include "shmipc/protocol/protocol.hpp"
#include "shmipc/util/buf_reader.hpp"

namespace shmipc {

namespace {

std::size_t buf_reader_capacity() {
  static const std::size_t cap = [] {
    const char* v = std::getenv("BUF_READER_CAPACITY");
    if (v != nullptr) {
      char* end = nullptr;
      const long parsed = std::strtol(v, &end, 10);
      if (end != v && parsed > 0) {
        return static_cast<std::size_t>(parsed);
      }
    }
    return static_cast<std::size_t>(32 * 1024 * 1024);
  }();
  return cap;
}

void ignore_sigpipe_once() {
  static std::once_flag flag;
  std::call_once(flag, [] { ::signal(SIGPIPE, SIG_IGN); });
}

std::uint32_t load_be_u32(const std::uint8_t* p) {
  return (static_cast<std::uint32_t>(p[0]) << 24) |
         (static_cast<std::uint32_t>(p[1]) << 16) |
         (static_cast<std::uint32_t>(p[2]) << 8) |
         static_cast<std::uint32_t>(p[3]);
}

}  // namespace

// ---------------------------------------------------------------------------
// SessionShared
// ---------------------------------------------------------------------------

namespace {

// Join `t` unless it is the calling thread (joining oneself throws
// std::errc::resource_deadlock_would_occur). The destructor can legitimately
// run on a backend loop thread: each loop closure holds the last strong
// reference to the SessionShared, so when it returns and drops that
// reference, ~SessionShared executes on the loop thread itself. In that case
// the thread is already winding down, so detaching it is correct.
void join_or_detach_self(std::thread& t) {
  if (!t.joinable()) {
    return;
  }
  if (t.get_id() == std::this_thread::get_id()) {
    t.detach();
  } else {
    t.join();
  }
}

}  // namespace

SessionShared::~SessionShared() {
  // Mirror of `impl Drop for Shared` (which aborts the tokio tasks): stop
  // all backend loops and join them.
  breaker_stop.set();
  {
    const std::lock_guard<std::mutex> lock(breaker_mtx);
    join_or_detach_self(breaker_thread);
  }
  if (conn_fd >= 0) {
    ::shutdown(conn_fd, SHUT_RDWR);
  }
  send_channel.stop();
  accept_channel.stop();
  join_or_detach_self(read_loop_thread);
  join_or_detach_self(write_loop_thread);
}

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

Result<Session> Session::server(Config config,
                                std::unique_ptr<TransportStream> conn_stream,
                                Sender<Stream> accept_tx) {
  return Session::create(std::move(config), std::move(conn_stream),
                         std::move(accept_tx));
}

Result<Session> Session::create(Config config,
                                std::unique_ptr<TransportStream> conn_stream,
                                std::optional<Sender<Stream>> accept_tx) {
  ignore_sigpipe_once();

  if (auto v = config.verify(); !v) {
    return std::unexpected(Error::other("verify config failed: " +
                                        v.error().message()));
  }

  const int conn_fd = conn_stream->as_raw_fd();
  auto [read_half, write_half] = conn_stream->into_split();
  if (read_half == nullptr || write_half == nullptr) {
    return std::unexpected(Error::from_errno(errno));
  }
  const bool is_client = !accept_tx.has_value();

  // on server mode the backend task will use accept_tx to transfer streams
  const std::uint32_t next_stream_id = is_client ? 1 : 2;

  std::shared_ptr<buffer::BufferManager> bm;
  QueueManager qm;
  std::uint8_t communication_version = 0;
  if (is_client) {
    auto managers = protocol::init_manager(config);
    if (!managers) {
      return std::unexpected(Error::other(
          "create share memory buffer manager failed, error=" +
          managers.error().message()));
    }
    bm = std::move(managers->first);
    qm = std::move(managers->second);
    auto version = protocol::init_client_protocol(
        bm->path(), bm->memfd(), qm.path(), qm.memfd(), conn_fd,
        config.mem_map_type, config.initialize_timeout);
    if (!version) {
      return std::unexpected(version.error());
    }
    communication_version = *version;
  } else {
    auto init = protocol::init_server_protocol(conn_fd,
                                               config.initialize_timeout);
    if (!init) {
      return std::unexpected(init.error());
    }
    bm = std::move(std::get<0>(*init));
    qm = std::move(std::get<1>(*init));
    communication_version = std::get<2>(*init);
  }

  auto [send_tx, send_rx] = make_channel<SendReady>(4096);

  auto shared = std::make_shared<SessionShared>(config, config.max_stream_num);
  shared->next_stream_id.store(next_stream_id, std::memory_order_seq_cst);
  shared->buffer_manager = std::move(bm);
  shared->name = qm.path();
  shared->queue_manager = std::move(qm);
  shared->communication_version = communication_version;
  shared->is_client = is_client;
  shared->send_tx = std::move(send_tx);
  shared->send_channel = send_rx;
  shared->accept_tx = std::move(accept_tx);
  shared->conn_fd = conn_fd;

  Session session(shared);

  // uds read loop
  shared->read_loop_thread = std::thread(
      [session, rh = std::move(read_half)]() mutable {
        session.read_loop(std::move(rh));
      });
  // uds write loop
  shared->write_loop_thread = std::thread(
      [session, wh = std::move(write_half), rx = std::move(send_rx)]() mutable {
        session.write_loop(std::move(wh), std::move(rx));
      });

  return session;
}

// ---------------------------------------------------------------------------
// Session state
// ---------------------------------------------------------------------------

bool Session::is_healthy() const {
  return shared_->unhealthy.load(std::memory_order_seq_cst) == 0;
}

bool Session::is_closed() const {
  return shared_->shutdown.load(std::memory_order_seq_cst) == 1;
}

Result<Stream> Session::get_or_open_stream(std::size_t session_id) const {
  if (!is_healthy()) {
    return std::unexpected(Err::kSessionUnhealthy);
  }

  while (auto stream = shared_->pool.pop()) {
    // ensure we return an open stream
    if (stream->is_open()) {
      return *stream;
    }
  }

  return open_stream(session_id);
}

void Session::put_or_close_stream(Stream s) const {
  // if the stream is in fallback state, we will not reuse it
  if (s.fallback_state()) {
    if (auto r = s.close(); !r) {
      SHMIPC_ERROR(shared_->name + " close stream error: " +
                   r.error().message());
    }
    return;
  }
  if (auto r = s.reset()) {
    s.release_read_and_reuse();
    if (auto pr = shared_->pool.push(std::move(s)); !pr) {
      SHMIPC_ERROR(std::string("put stream to pool error: ") +
                   pr.error().message());
    }
  } else {
    SHMIPC_ERROR(shared_->name + " put_or_close_stream error: " +
                 r.error().message());
    if (auto cr = s.close(); !cr) {
      SHMIPC_ERROR(shared_->name + " close stream error: " +
                   cr.error().message());
    }
  }
}

Result<Stream> Session::open_stream(std::size_t session_id) const {
  if (is_closed()) {
    std::optional<Error> err;
    {
      const std::lock_guard<std::mutex> lock(shared_->shutdown_err_mtx);
      err = std::move(shared_->shutdown_err);
      shared_->shutdown_err.reset();
    }
    if (err.has_value()) {
      return std::unexpected(*err);
    }
    return std::unexpected(Err::kSessionShutdown);
  }
  if (shared_->unhealthy.load(std::memory_order_seq_cst) == 1) {
    return std::unexpected(Err::kSessionUnhealthy);
  }

  // get an id, and check for stream exhaustion
  const std::uint32_t id =
      shared_->next_stream_id.fetch_add(1, std::memory_order_seq_cst) + 1;

  {
    const std::shared_lock<std::shared_mutex> lock(shared_->streams_mtx);
    if (shared_->streams.count(id) != 0) {
      return std::unexpected(Err::kStreamsExhausted);
    }
  }

  Stream stream(id, session_id, shared_);
  {
    const std::unique_lock<std::shared_mutex> lock(shared_->streams_mtx);
    shared_->streams.emplace(id, stream);
  }

  char msg[96];
  std::snprintf(msg, sizeof(msg), "%s open stream %u",
                shared_->name.c_str(), id);
  SHMIPC_TRACE(msg);

  return stream;
}

void Session::close() const {
  std::uint32_t expected = 0;
  if (!shared_->shutdown.compare_exchange_strong(expected, 1,
                                                 std::memory_order_seq_cst,
                                                 std::memory_order_seq_cst)) {
    return;
  }
  {
    char msg[160];
    std::snprintf(msg, sizeof(msg), "close session %s hadShutDown:%u",
                  shared_->name.c_str(),
                  shared_->shutdown.load(std::memory_order_seq_cst));
    SHMIPC_INFO(msg);
  }
  shared_->pool.close();

  shared_->shutdown_event.set();

  // close all streams
  std::vector<Stream> streams;
  {
    const std::unique_lock<std::shared_mutex> lock(shared_->streams_mtx);
    streams.reserve(shared_->streams.size());
    for (auto& [id, stream] : shared_->streams) {
      streams.push_back(stream);
    }
    shared_->streams.clear();
  }
  for (auto& stream : streams) {
    (void)stream.close();
  }

  buffer::add_global_buffer_manager_ref_count(
      shared_->buffer_manager->path(), -1);
  shared_->queue_manager.unmap();

  // Stop the backend loops (equivalent of the tokio tasks observing
  // shutdown_notify and returning).
  shared_->send_channel.stop();
  shared_->accept_channel.stop();
  if (shared_->conn_fd >= 0) {
    ::shutdown(shared_->conn_fd, SHUT_RDWR);
  }
}

// ---------------------------------------------------------------------------
// Sending
// ---------------------------------------------------------------------------

Result<Unit> Session::wait_for_send(
    std::optional<std::vector<std::uint8_t>> hdr,
    std::vector<std::uint8_t> body) const {
  std::promise<void> promise;
  auto future = promise.get_future();
  SendReady ready{hdr.has_value() ? std::move(*hdr) : std::vector<std::uint8_t>{},
                  std::move(body), std::move(promise)};
  if (!shared_->send_tx.send_timeout(std::move(ready),
                                     shared_->config.connection_write_timeout)) {
    SHMIPC_DEBUG("write timeout, send channel is full");
    return std::unexpected(Err::kConnectionWriteTimeout);
  }

  future.wait();
  try {
    future.get();
  } catch (const std::future_error&) {
    return std::unexpected(
        Error::other("wait for send failed, error=channel closed"));
  }
  return Unit{};
}

Result<Unit> Session::wake_up_peer() const {
  if (!shared_->queue_manager.send_queue.mark_working()) {
    return Unit{};
  }
  shared_->stats.send_polling_event_count.fetch_add(
      1, std::memory_order_seq_cst);
  SendReady ready{{},
                  protocol::polling_event_with_version()
                      [shared_->communication_version],
                  std::promise<void>()};
  (void)shared_->send_tx.send(std::move(ready));
  return Unit{};
}

void Session::open_circuit_breaker() const {
  static const bool debug_mode =
      std::getenv("SHMIPC_DEBUG_MODE") != nullptr;
  if (debug_mode) {
    return;
  }

  std::uint32_t expected = 0;
  if (!shared_->unhealthy.compare_exchange_strong(
          expected, 1, std::memory_order_seq_cst, std::memory_order_seq_cst)) {
    return;
  }
  SHMIPC_INFO("session " + shared_->name +
              " circuit breaker open, set unhealthy status");

  std::weak_ptr<SessionShared> weak = shared_;
  const std::lock_guard<std::mutex> lock(shared_->breaker_mtx);
  if (shared_->breaker_thread.joinable()) {
    shared_->breaker_thread.join();
  }
  shared_->breaker_thread = std::thread([weak] {
    auto shared = weak.lock();
    if (shared == nullptr) {
      return;
    }
    // Interruptible 30s wait; exits early when the session is torn down.
    if (shared->breaker_stop.wait_for(std::chrono::seconds(30))) {
      return;
    }
    shared->unhealthy.store(0, std::memory_order_seq_cst);
    SHMIPC_INFO("session " + shared->name +
                " circuit breaker closed, remove unhealthy status");
  });
}

void Session::on_stream_close(std::uint32_t id, std::uint32_t state) const {
  char msg[64];
  std::snprintf(msg, sizeof(msg), "stream:%u close state:%u", id, state);
  SHMIPC_TRACE(msg);
  const std::unique_lock<std::shared_mutex> lock(shared_->streams_mtx);
  shared_->streams.erase(id);
}

// ---------------------------------------------------------------------------
// Backend loops
// ---------------------------------------------------------------------------

void Session::recv_loop(Receiver<Stream> rx,
                        Sender<IoResult<Stream>> stream_tx) const {
  shared_->accept_channel = rx;
  while (true) {
    auto stream_opt = rx.recv();
    if (!stream_opt.has_value()) {
      SHMIPC_INFO("session shutdown");
      return;
    }
    if (!stream_tx.send(IoResult<Stream>(std::move(*stream_opt)))) {
      return;
    }
  }
}

void Session::read_loop(std::unique_ptr<ReadHalf> reader) const {
  BufReader buf_reader(reader->fd(), buf_reader_capacity());
  std::size_t len = kHeaderSize;
  while (true) {
    auto buf = buf_reader.fill_buf_at_least(len);
    if (shared_->shutdown.load(std::memory_order_seq_cst) == 1) {
      return;
    }
    if (!buf.has_value()) {
      exit_err(buf.error());
      return;
    }
    const EventResult result = handle_events(*buf);
    buf_reader.consume(result.consumed);
    len = result.required;
    if (result.err.has_value() && !is_closed()) {
      exit_err(*result.err);
      return;
    }
  }
}

void Session::write_loop(std::unique_ptr<WriteHalf> writer,
                         Receiver<SendReady> send_rx) const {
  while (true) {
    auto ready_opt = send_rx.recv();
    if (!ready_opt.has_value()) {
      return;
    }
    SendReady ready = std::move(*ready_opt);
    // send a header if present
    if (!ready.hdr.empty()) {
      if (auto r = writer->write_all(ready.hdr); !r) {
        // Dropping `ready` without setting the promise signals the error to
        // the waiter (Rust: drop(ready.tx)).
        exit_err(r.error());
        return;
      }
    }
    // send data from the body if given
    if (!ready.body.empty()) {
      if (auto r = writer->write_all(ready.body); !r) {
        exit_err(r.error());
        return;
      }
    }
    // no error, successful send
    ready.done.set_value();
  }
}

void Session::exit_err(Error err) const {
  SHMIPC_WARN(shared_->name + " exit with error: " + err.message());
  shared_->stats.event_conn_error_count.fetch_add(1, std::memory_order_seq_cst);
  {
    const std::lock_guard<std::mutex> lock(shared_->shutdown_err_mtx);
    shared_->shutdown_err = err;
  }
  close();
}

// ---------------------------------------------------------------------------
// Event handling
// ---------------------------------------------------------------------------

Session::EventResult Session::handle_events(
    std::span<const std::uint8_t> buf) const {
  std::size_t consumed = 0;
  while (buf.size() - consumed >= kHeaderSize) {
    const protocol::Header event_header(
        const_cast<std::uint8_t*>(buf.data() + consumed));
    if (auto valid = protocol::check_event_valid(event_header); !valid) {
      return {consumed + kHeaderSize, kHeaderSize, valid.error()};
    }
    HandlerResult hr;
    switch (event_header.msg_type()) {
      case protocol::EventType::kPolling:
        hr = handle_polling(buf.subspan(consumed + kHeaderSize));
        break;
      case protocol::EventType::kStreamClose:
        hr = handle_stream_close(buf.subspan(consumed + kHeaderSize));
        break;
      case protocol::EventType::kFallbackData:
        hr = handle_fallback_data(event_header,
                                  buf.subspan(consumed + kHeaderSize));
        break;
      default:
        return {consumed + kHeaderSize, kHeaderSize,
                Error(Err::kInvalidMsgType)};
    }
    consumed += hr.consumed;
    if (hr.err.has_value()) {
      return {consumed, kHeaderSize, hr.err};
    }
    if (hr.stop) {
      return {consumed, hr.required, std::nullopt};
    }
  }
  return {consumed, kHeaderSize, std::nullopt};
}

Session::HandlerResult Session::handle_polling(
    std::span<const std::uint8_t> /*buf*/) const {
  shared_->stats.recv_polling_event_count.fetch_add(
      1, std::memory_order_seq_cst);
  std::optional<Error> ret_err;
  while (true) {
    while (auto ele = shared_->queue_manager.recv_queue.pop()) {
      const std::uint32_t state = ele->status & 0xff;
      if (auto stream = get_stream(ele->seq_id, state)) {
        if (auto r = handle_stream_message(
                *stream, BufferSliceWrapper{std::nullopt, ele->offset_in_shm_buf},
                state);
            !r) {
          ret_err = r.error();
        }
      } else if (state == kStreamOpened) {
        auto slice = shared_->buffer_manager->read_buffer_slice(
            ele->offset_in_shm_buf);
        if (!slice) {
          return {kHeaderSize, kHeaderSize, false, slice.error()};
        }
        shared_->buffer_manager->recycle_buffers(std::move(*slice));
      } else {
        continue;
      }
    }

    std::this_thread::yield();
    if (shared_->queue_manager.recv_queue.mark_not_working()) {
      break;
    }
  }
  return {kHeaderSize, kHeaderSize, false, ret_err};
}

Session::HandlerResult Session::handle_fallback_data(
    const protocol::Header& event_header,
    std::span<const std::uint8_t> buf) const {
  const std::size_t event_len = event_header.length();
  const std::size_t payload_len = event_len - kHeaderSize;
  constexpr std::size_t kFallbackDataHeader = 8;
  if (buf.size() < payload_len) {
    return {0, event_len, true, std::nullopt};
  }
  assert(payload_len >= kFallbackDataHeader);
  std::vector<std::uint8_t> data(buf.begin() + kFallbackDataHeader,
                                 buf.begin() + payload_len);
  // fallback data layout: eventHeader | seqID | status | payload
  const std::uint32_t seq_id = load_be_u32(buf.data());
  // the first byte of status is the stream state; the rest is undefined
  const std::uint32_t status = load_be_u32(buf.data() + 4) & 0xff;
  {
    char msg[256];
    std::snprintf(msg, sizeof(msg),
                  "session %s receive fallback data, length:%zu seqID:%u "
                  "status:%u",
                  shared_->name.c_str(),
                  event_len - kHeaderSize - kFallbackDataHeader, seq_id,
                  status);
    SHMIPC_WARN(msg);
  }
  Session(shared_).open_circuit_breaker();
  buffer::BufferSlice fallback_slice =
      buffer::BufferSlice::from_heap(std::move(data));
  fallback_slice.write_index = fallback_slice.cap;
  shared_->stats.fallback_read_count.fetch_add(1, std::memory_order_seq_cst);
  if (auto stream = get_stream(seq_id, status)) {
    auto r = handle_stream_message(
        *stream, BufferSliceWrapper{std::move(fallback_slice), 0}, status);
    return {event_len, kHeaderSize, false,
            r.has_value() ? std::nullopt : std::optional<Error>(r.error())};
  }
  return {event_len, kHeaderSize, false, std::nullopt};
}

Session::HandlerResult Session::handle_stream_close(
    std::span<const std::uint8_t> buf) const {
  constexpr std::size_t kIdLen = 4;
  if (buf.size() < kIdLen) {
    return {0, kHeaderSize + kIdLen, true, std::nullopt};
  }
  const std::uint32_t id = load_be_u32(buf.data());
  {
    char msg[64];
    std::snprintf(msg, sizeof(msg), "receive peer stream[%u] goaway.", id);
    SHMIPC_DEBUG(msg);
  }

  Stream stream;
  {
    const std::unique_lock<std::shared_mutex> lock(shared_->streams_mtx);
    auto it = shared_->streams.find(id);
    if (it != shared_->streams.end()) {
      stream = std::move(it->second);
      shared_->streams.erase(it);
    }
  }
  if (stream.valid()) {
    stream.half_close();
  } else {
    char msg[64];
    std::snprintf(msg, sizeof(msg), "missing stream: %u", id);
    SHMIPC_WARN(msg);
  }
  return {kHeaderSize + kIdLen, kHeaderSize, false, std::nullopt};
}

std::optional<Stream> Session::get_stream(std::uint32_t id,
                                          std::uint32_t state) const {
  {
    const std::shared_lock<std::shared_mutex> lock(shared_->streams_mtx);
    auto it = shared_->streams.find(id);
    if (it != shared_->streams.end()) {
      return it->second;
    }
  }
  if (!shared_->is_client && state == kStreamOpened) {
    Stream stream(id, 0, shared_);
    {
      const std::unique_lock<std::shared_mutex> lock(shared_->streams_mtx);
      shared_->streams.emplace(id, stream);
    }

    if (shared_->accept_tx.has_value()) {
      // Blocks until accepted by recv_loop, or the channel is stopped.
      (void)shared_->accept_tx->send(stream);
    }
    return stream;
  }
  return std::nullopt;
}

Result<Unit> Session::handle_stream_message(const Stream& stream,
                                            BufferSliceWrapper wrapper,
                                            std::uint32_t state) const {
  if (state == kStreamClosed) {
    stream.half_close();
    return Unit{};
  }

  return stream.fill_data_to_read_buffer(std::move(wrapper));
}

}  // namespace shmipc
