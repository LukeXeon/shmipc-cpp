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

#include "shmipc/stream.hpp"

#include <chrono>
#include <cstdio>
#include <utility>

#include "shmipc/consts.hpp"
#include "shmipc/log.hpp"
#include "shmipc/protocol/event.hpp"
#include "shmipc/protocol/header.hpp"
#include "shmipc/session.hpp"

namespace shmipc {

using buffer::LinkedBuffer;

Stream::Stream(std::uint32_t id, std::size_t session_id,
               std::shared_ptr<SessionShared> shared)
    : id_(id), shared_(std::move(shared)), session_id_(session_id) {
  inner_ = std::make_shared<StreamInner>(shared_->buffer_manager);
}

// ---------------------------------------------------------------------------
// Read path
// ---------------------------------------------------------------------------

Result<Unit> Stream::read_more(std::size_t min_size, LinkedBuffer& buf) const {
  move_pending_data(buf);
  const std::size_t recv_len = buf.len();
  if (recv_len >= min_size) {
    return Unit{};
  }

  if (recv_len == 0 && inner_->state.load(std::memory_order_seq_cst) !=
                           kStreamOpened) {
    return std::unexpected(Err::kEndOfStream);
  }

  while (true) {
    {
      std::unique_lock<std::mutex> lock(inner_->notify_mtx);
      inner_->notify_cv.wait(lock, [this] {
        {
          const std::lock_guard<std::mutex> pending_lock(inner_->pending_mtx);
          if (!inner_->pending_data.empty()) {
            return true;
          }
        }
        return inner_->state.load(std::memory_order_seq_cst) != kStreamOpened;
      });
    }
    move_pending_data(buf);
    if (buf.len() >= min_size) {
      return Unit{};
    }

    const std::uint32_t state = inner_->state.load(std::memory_order_seq_cst);
    if (state != kStreamOpened) {
      if (state == kStreamHalfClosed) {
        return std::unexpected(Err::kEndOfStream);
      }
      return std::unexpected(Err::kStreamClosed);
    }
  }
}

void Stream::move_pending_data(LinkedBuffer& buf) const {
  std::vector<BufferSliceWrapper> pending_data;
  {
    const std::lock_guard<std::mutex> lock(inner_->pending_mtx);
    pending_data = std::move(inner_->pending_data);
    inner_->pending_data.clear();
  }
  if (pending_data.empty()) {
    return;
  }
  const std::size_t pre_len = buf.len();
  for (auto& data : pending_data) {
    if (data.fallback_slice.has_value()) {
      buf.append_buffer_slice(std::move(*data.fallback_slice));
      inner_->in_fallback_state.store(true, std::memory_order_seq_cst);
      continue;
    }
    std::uint32_t offset = data.offset;
    while (true) {
      auto slice = shared_->buffer_manager->read_buffer_slice(offset);
      if (!slice) {
        SHMIPC_ERROR("read_buffer_slice error " + slice.error().message());
        break;
      }
      const bool has_next = slice->buffer_header.has_value() &&
                            slice->buffer_header->has_next();
      if (slice->size() == 0) {
        const std::uint32_t next_offset =
            slice->buffer_header->next_buffer_offset();
        shared_->buffer_manager->recycle_buffer(std::move(*slice));
        if (has_next) {
          offset = next_offset;
          continue;
        }
        if (auto* back = buf.slice_list().back();
            back != nullptr && back->buffer_header.has_value()) {
          back->buffer_header->clear_flag();
          back->buffer_header->set_in_used();
        }
        break;
      }
      if (!has_next) {
        buf.append_buffer_slice(std::move(*slice));
        break;
      }
      offset = slice->buffer_header->next_buffer_offset();
      buf.append_buffer_slice(std::move(*slice));
    }
  }
  shared_->stats.in_flow_bytes.fetch_add(
      static_cast<std::uint64_t>(buf.len() - pre_len),
      std::memory_order_seq_cst);
}

Result<buffer::Buf> Stream::read() {
  LinkedBuffer& buf = recv_buf();
  if (buf.is_empty()) {
    char msg[64];
    std::snprintf(msg, sizeof(msg), "read_bytes seqID:%u", id_);
    SHMIPC_DEBUG(msg);
    if (auto r = read_more(1, buf); !r) {
      return std::unexpected(r.error());
    }
  }
  return buf.read_bytes(buf.len());
}

Result<buffer::Buf> Stream::read_bytes(std::size_t size) {
  LinkedBuffer& buf = recv_buf();
  if (buf.len() < size) {
    char msg[96];
    std::snprintf(msg, sizeof(msg), "read_bytes seqID:%u len:%zu size:%zu",
                  id_, buf.len(), size);
    SHMIPC_DEBUG(msg);
    if (auto r = read_more(size, buf); !r) {
      return std::unexpected(r.error());
    }
  }
  return buf.read_bytes(size);
}

Result<buffer::Buf> Stream::peek(std::size_t size) {
  LinkedBuffer& buf = recv_buf();
  if (buf.len() < size) {
    if (auto r = read_more(size, buf); !r) {
      return std::unexpected(r.error());
    }
  }
  return buf.peek(size);
}

Result<std::size_t> Stream::discard(std::size_t size) {
  LinkedBuffer& buf = recv_buf();
  if (buf.len() < size) {
    if (auto r = read_more(size, buf); !r) {
      return std::unexpected(r.error());
    }
  }
  return buf.discard(size);
}

// ---------------------------------------------------------------------------
// Write path
// ---------------------------------------------------------------------------

Result<std::span<std::uint8_t>> Stream::reserve(std::size_t size) {
  return send_buf().reserve(size);
}

Result<std::size_t> Stream::write_bytes(std::span<const std::uint8_t> data) {
  return send_buf().write_bytes(data);
}

Result<Unit> Stream::write_fallback(std::uint32_t stream_status, Error err,
                                    LinkedBuffer& send_buf) const {
  const std::size_t buf_len = send_buf.len();
  if (buf_len > static_cast<std::size_t>(UINT32_MAX) - 16) {
    send_buf.recycle();
    return std::unexpected(Err::kNoMoreBuffer);
  }
  {
    char msg[320];
    std::snprintf(
        msg, sizeof(msg),
        "session %s stream fallback seqID:%u len:%zu reason:%s, "
        "send_buf.is_from_share_memory: %s",
        shared_->name.c_str(), id_, buf_len, err.message().c_str(),
        send_buf.is_from_share_memory() ? "true" : "false");
    SHMIPC_WARN(msg);
  }
  std::uint8_t event_buf[16] = {0};
  protocol::FallbackDataEvent event(event_buf);
  event.encode(static_cast<std::uint32_t>(buf_len) + 16,
               shared_->communication_version, id_, stream_status);
  std::vector<std::uint8_t> data;
  data.reserve(send_buf.len() + 16);
  const auto event_slice = event.as_slice();
  data.insert(data.end(), event_slice.begin(), event_slice.end());
  for (const auto* s = send_buf.slice_list().front(); s != nullptr;
       s = s->next()) {
    // Mirrors the Rust span exactly: starts at s.data with length
    // write_index - read_index.
    data.insert(data.end(), s->data,
                s->data + (s->write_index - s->read_index));
    if (send_buf.slice_list().write() == s) {
      break;
    }
  }
  send_buf.recycle();
  Session(shared_).open_circuit_breaker();
  shared_->stats.fallback_write_count.fetch_add(1, std::memory_order_seq_cst);
  return Session(shared_).wait_for_send(std::nullopt, std::move(data));
}

Result<Unit> Stream::flush(bool end_stream) {
  LinkedBuffer& send_buf = this->send_buf();
  if (send_buf.is_empty()) {
    return Unit{};
  }
  shared_->stats.out_flow_bytes.fetch_add(
      static_cast<std::uint64_t>(send_buf.len()), std::memory_order_seq_cst);
  const std::uint32_t state = inner_->state.load(std::memory_order_seq_cst);
  if (state != kStreamOpened) {
    send_buf.recycle();
    return std::unexpected(Err::kStreamClosed);
  }
  send_buf.done(end_stream);
  // Once we send data using uds, for this stream we will always use uds
  // later to avoid unordering.
  if (!send_buf.is_from_share_memory()) {
    inner_->in_fallback_state.store(true, std::memory_order_seq_cst);
  }
  if (inner_->in_fallback_state.load(std::memory_order_seq_cst)) {
    auto ret = write_fallback(state, Error(Err::kNoMoreBuffer), send_buf);
    send_buf.clean();
    return ret;
  }

  auto put = shared_->queue_manager.send_queue.put(
      QueueElement{id_, send_buf.root_buf_offset(), state});
  if (put) {
    auto ret = Session(shared_).wake_up_peer();
    send_buf.clean();
    return ret;
  }
  if (!(put.error() == Err::kQueueFull)) {
    send_buf.recycle();
    return std::unexpected(put.error());
  }
  shared_->stats.queue_full_error_count.fetch_add(1, std::memory_order_seq_cst);
  for (int retry = 0; retry < 10; ++retry) {
    {
      std::unique_lock<std::mutex> lock(inner_->notify_mtx);
      const bool closed = inner_->notify_cv.wait_for(
          lock, std::chrono::milliseconds(10), [this] {
            return inner_->state.load(std::memory_order_seq_cst) !=
                   kStreamOpened;
          });
      if (closed) {
        send_buf.recycle();
        return std::unexpected(Err::kStreamClosed);
      }
    }

    put = shared_->queue_manager.send_queue.put(
        QueueElement{id_, send_buf.root_buf_offset(), state});
    if (put) {
      auto ret = Session(shared_).wake_up_peer();
      send_buf.clean();
      return ret;
    }
    if (put.error() == Err::kQueueFull) {
      continue;
    }
    send_buf.recycle();
    return std::unexpected(put.error());
  }
  // Upstream fix (approved for this port): the Rust version returns Ok(())
  // here after 10 queue-full retries, silently dropping the buffer without
  // recycling it (stream.rs:585). The C++ port recycles the buffer and
  // reports QueueFull so the caller can retry.
  send_buf.recycle();
  return std::unexpected(Err::kQueueFull);
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

void Stream::clean() const {
  Session(shared_).on_stream_close(
      id_, inner_->state.load(std::memory_order_seq_cst));
  clean_pending_data();
  inner_->recv_buf.recycle();
  inner_->send_buf.recycle();
}

void Stream::clean_pending_data() const {
  std::vector<BufferSliceWrapper> pending_data;
  {
    const std::lock_guard<std::mutex> lock(inner_->pending_mtx);
    pending_data = std::move(inner_->pending_data);
    inner_->pending_data.clear();
  }
  for (auto& data : pending_data) {
    if (data.fallback_slice.has_value()) {
      if (!data.fallback_slice->is_from_shm) {
        // Heap storage is freed by RAII (Rust reconstructs the Vec here).
      } else {
        char msg[96];
        std::snprintf(msg, sizeof(msg),
                      "fallback slice is from shm, offset:%u",
                      data.fallback_slice->offset_in_shm);
        SHMIPC_WARN(msg);
      }
      continue;
    }
    auto slice = shared_->buffer_manager->read_buffer_slice(data.offset);
    if (!slice) {
      SHMIPC_ERROR("read_buffer_slice error " + slice.error().message());
      break;
    }
    shared_->buffer_manager->recycle_buffers(std::move(*slice));
  }
}

Result<Unit> Stream::reset() const {
  if (inner_->state.load(std::memory_order_seq_cst) != kStreamOpened) {
    return std::unexpected(Err::kStreamClosed);
  }
  // return error if there is any unread data
  const std::size_t unread_size = inner_->recv_buf.len();
  if (unread_size > 0) {
    return std::unexpected(Error::with_size(Err::kStreamHasUnreadData,
                                            unread_size));
  }

  std::size_t pending_data_len = 0;
  {
    const std::lock_guard<std::mutex> lock(inner_->pending_mtx);
    pending_data_len = inner_->pending_data.size();
  }
  if (pending_data_len > 0) {
    return std::unexpected(Error::with_size(Err::kStreamHasPendingData,
                                            pending_data_len));
  }

  inner_->in_fallback_state.store(false, std::memory_order_seq_cst);
  return Unit{};
}

void Stream::release_read_and_reuse() const {
  LinkedBuffer& recv_buf = inner_->recv_buf;
  LinkedBuffer& send_buf = inner_->send_buf;
  recv_buf.release_previous_read_and_reserve();
  if (recv_buf.is_empty() && recv_buf.slice_list().size() == 1) {
    std::swap(recv_buf, send_buf);
  }
}

Result<Unit> Stream::fill_data_to_read_buffer(BufferSliceWrapper wrapper) const {
  {
    const std::lock_guard<std::mutex> lock(inner_->pending_mtx);
    inner_->pending_data.push_back(std::move(wrapper));
  }
  // The stream may have been closed, e.g. by the user due to a timeout.
  if (inner_->state.load(std::memory_order_seq_cst) == kStreamClosed) {
    clean_pending_data();
    inner_->recv_buf.recycle();
    return Unit{};
  }
  // Unblock any readers.
  safe_close_notify();
  return Unit{};
}

bool Stream::is_open() const {
  return inner_->state.load(std::memory_order_seq_cst) == kStreamOpened;
}

void Stream::safe_close_notify() const {
  const std::lock_guard<std::mutex> lock(inner_->notify_mtx);
  inner_->notify_cv.notify_all();
}

void Stream::half_close() const {
  std::uint32_t expected = kStreamOpened;
  if (inner_->state.compare_exchange_strong(expected, kStreamHalfClosed,
                                            std::memory_order_seq_cst,
                                            std::memory_order_seq_cst)) {
    safe_close_notify();
  }
}

bool Stream::fallback_state() const {
  return inner_->in_fallback_state.load(std::memory_order_seq_cst);
}

void Stream::reuse() const {
  Session(shared_).put_or_close_stream(*this);
}

Result<Unit> Stream::close() {
  const std::uint32_t old_state =
      inner_->state.exchange(kStreamClosed, std::memory_order_release);
  if (old_state == kStreamClosed) {
    return Unit{};
  }
  clean();
  if (old_state != kStreamOpened) {
    return Unit{};
  }
  safe_close_notify();

  if (shared_->shutdown.load(std::memory_order_seq_cst) == 1) {
    return Unit{};
  }
  if (!inner_->in_fallback_state.load(std::memory_order_seq_cst)) {
    if (shared_->queue_manager.send_queue
            .put(QueueElement{id_, 0, kStreamClosed})
            .has_value()) {
      return Session(shared_).wake_up_peer();
    }
    shared_->stats.queue_full_error_count.fetch_add(
        1, std::memory_order_seq_cst);
  }

  // notify close
  std::vector<std::uint8_t> event(12, 0);
  {
    protocol::Header h(event.data());
    h.encode(12, shared_->communication_version,
             protocol::EventType::kStreamClose);
    event[8] = static_cast<std::uint8_t>(id_ >> 24);
    event[9] = static_cast<std::uint8_t>(id_ >> 16);
    event[10] = static_cast<std::uint8_t>(id_ >> 8);
    event[11] = static_cast<std::uint8_t>(id_);
  }
  return Session(shared_).wait_for_send(std::nullopt, std::move(event));
}

}  // namespace shmipc
