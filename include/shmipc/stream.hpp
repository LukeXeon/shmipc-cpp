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
// C++ translation of shmipc-rs `src/stream.rs`.
//
// Concurrency model: the tokio futures become blocking methods; the session's
// receive path only appends to `pending_data` and notifies, while the user
// thread owns `recv_buf`/`send_buf` accesses — the same discipline the Rust
// UnsafeCell fields rely on.

#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <vector>

#include "shmipc/buffer/linked.hpp"
#include "shmipc/error.hpp"

namespace shmipc {

struct SessionShared;  // defined in session.hpp

inline constexpr std::uint32_t kStreamOpened = 0;
inline constexpr std::uint32_t kStreamClosed = 1;
inline constexpr std::uint32_t kStreamHalfClosed = 2;

// Mirror of BufferSliceWrapper: either a fallback (UDS) slice or the offset
// of a shared-memory buffer chain.
struct BufferSliceWrapper {
  std::optional<buffer::BufferSlice> fallback_slice;
  std::uint32_t offset = 0;
};

struct StreamInner {
  buffer::LinkedBuffer recv_buf;
  buffer::LinkedBuffer send_buf;
  std::mutex pending_mtx;
  std::vector<BufferSliceWrapper> pending_data;
  std::atomic<std::uint32_t> state{kStreamOpened};
  // Combined recv/close notification (Rust: recv_notify + close_notify).
  std::mutex notify_mtx;
  std::condition_variable notify_cv;
  // if in_fallback_state is set to true, sending should use uds
  std::atomic<bool> in_fallback_state{false};

  explicit StreamInner(std::shared_ptr<buffer::BufferManager> bm)
      : recv_buf(bm), send_buf(std::move(bm)) {}
};

// Stream represents a logical stream within a session. Cheap to copy like
// the Rust Clone impl (all state is behind shared pointers).
class Stream {
 public:
  Stream() = default;
  Stream(std::uint32_t id, std::size_t session_id,
         std::shared_ptr<SessionShared> shared);

  std::uint32_t stream_id() const noexcept { return id_; }
  std::size_t session_id() const noexcept { return session_id_; }
  bool valid() const noexcept { return inner_ != nullptr; }
  const std::shared_ptr<SessionShared>& session_shared() const noexcept {
    return shared_;
  }

  buffer::LinkedBuffer& recv_buf() const { return inner_->recv_buf; }
  buffer::LinkedBuffer& send_buf() const { return inner_->send_buf; }

  // Blocking IO (mirrors the async fns). -------------------------------

  // Read a shm buffer; the length depends on how much the peer writes at
  // once. After using the buffer you MUST call release_read_and_reuse().
  Result<buffer::Buf> read();

  // Read until at least `size` bytes are available (blocks forever if the
  // data never arrives, like the Rust version).
  Result<buffer::Buf> read_bytes(std::size_t size);
  Result<buffer::Buf> peek(std::size_t size);
  Result<std::size_t> discard(std::size_t size);

  Result<std::span<std::uint8_t>> reserve(std::size_t size);
  Result<std::size_t> write_bytes(std::span<const std::uint8_t> data);
  Result<Unit> flush(bool end_stream);

  Result<Unit> close();
  // Clean the stream's all status for reusing.
  Result<Unit> reset() const;
  // Release the data previously read and reuse the last share memory slice
  // for the next write.
  void release_read_and_reuse() const;

  // Return the stream to its session's pool (or close it).
  void reuse() const;

  // Session receive-path API. --------------------------------------------

  Result<Unit> fill_data_to_read_buffer(BufferSliceWrapper wrapper) const;
  bool is_open() const;
  void safe_close_notify() const;
  void half_close() const;
  bool fallback_state() const;

  friend bool operator==(const Stream& lhs, const Stream& rhs) noexcept {
    return lhs.inner_ == rhs.inner_;
  }

 private:
  Result<Unit> read_more(std::size_t min_size, buffer::LinkedBuffer& buf) const;
  void move_pending_data(buffer::LinkedBuffer& buf) const;
  Result<Unit> write_fallback(std::uint32_t stream_status, Error err,
                              buffer::LinkedBuffer& send_buf) const;
  void clean() const;
  void clean_pending_data() const;

  std::shared_ptr<StreamInner> inner_;
  std::uint32_t id_ = 0;
  std::shared_ptr<SessionShared> shared_;
  std::size_t session_id_ = 0;
};

}  // namespace shmipc
