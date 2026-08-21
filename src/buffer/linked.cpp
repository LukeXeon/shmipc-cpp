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

#include "shmipc/buffer/linked.hpp"

#include <utility>
#include <vector>

#include "shmipc/consts.hpp"

namespace shmipc::buffer {

void LinkedBuffer::alloc(std::uint32_t size) {
  std::int64_t remain = static_cast<std::int64_t>(size);
  if (auto buf = buffer_manager_->alloc_shm_buffer(size)) {
    slice_list_.push_back(std::move(*buf));
    return;
  }
  const std::int64_t alloc_size =
      buffer_manager_->alloc_shm_buffers(slice_list_, size);
  remain -= alloc_size;
  // Fallback: allocate a memory buffer (not shm).
  if (remain > 0) {
    if (remain < kDefaultSingleBufferSize) {
      remain = kDefaultSingleBufferSize;
    }
    slice_list_.push_back(
        BufferSlice::from_heap(std::vector<std::uint8_t>(remain, 0)));
    is_from_shm_ = false;
  }
}

void LinkedBuffer::done(bool /*end_stream*/) {
  if (!is_from_shm_) {
    return;
  }
  for (auto* s = slice_list_.front(); s != nullptr; s = s->next()) {
    s->update();
    if (slice_list_.write() == s) {
      break;
    }
  }
  // Recycle unused slices.
  if (slice_list_.write() != nullptr && slice_list_.write()->next() != nullptr) {
    std::optional<BufferSlice> slice = slice_list_.split_from_write();
    while (slice.has_value()) {
      std::unique_ptr<BufferSlice> next = slice->take_next();
      buffer_manager_->recycle_buffer(std::move(*slice));
      slice.reset();
      if (next != nullptr) {
        slice = std::move(*next);
      }
    }
  }
}

void LinkedBuffer::append_buffer_slice(BufferSlice slice) {
  if (!slice.is_from_shm) {
    is_from_shm_ = false;
  }
  len_ += slice.size();
  slice_list_.push_back(std::move(slice));
  slice_list_.set_write(slice_list_.back());
}

void LinkedBuffer::release_previous_read_and_reserve() {
  clean_pinned_list();
  // Try to reserve space in long-stream mode for improving performance:
  // reuse the read buffer as the next write buffer, avoiding share memory
  // allocation and recycling.
  if (len_ == 0 && slice_list_.size() == 1) {
    if (slice_list_.front()->is_from_shm) {
      slice_list_.front()->reset();
    } else {
      // Heap storage is freed by RAII inside the popped slice.
      (void)slice_list_.pop_front();
    }
    // Defensive (upstream leaves the cursor dangling when the list became
    // empty): an empty list has no write cursor.
    if (slice_list_.size() == 0) {
      slice_list_.set_write(nullptr);
    }
  }
}

void LinkedBuffer::recycle() {
  const std::lock_guard<std::mutex> guard(*recycle_mtx_);
  while (auto slice = slice_list_.pop_front()) {
    if (slice->is_from_shm) {
      buffer_manager_->recycle_buffer(std::move(*slice));
    }
    // Non-shm: heap storage freed by RAII.
  }
  slice_list_.set_write(nullptr);
  is_from_shm_ = true;
  end_stream_ = false;
  current_pinned_ = false;
  len_ = 0;
}

void LinkedBuffer::clean() {
  while (auto slice = slice_list_.pop_front()) {
    // Shm slices are intentionally NOT recycled: their ownership was handed
    // to the peer via the IO queue (mirrors Rust clean()). Heap storage of
    // non-shm slices is freed by RAII.
  }
  slice_list_.set_write(nullptr);
  is_from_shm_ = true;
  end_stream_ = false;
  current_pinned_ = false;
  len_ = 0;
}

std::uint32_t LinkedBuffer::root_buf_offset() const {
  if (slice_list_.front() != nullptr) {
    return slice_list_.front()->offset_in_shm;
  }
  return 0;
}

std::size_t LinkedBuffer::cap() const {
  std::size_t sum = 0;
  for (const auto* e = slice_list_.front(); e != nullptr; e = e->next()) {
    sum += e->capacity();
  }
  return sum;
}

void LinkedBuffer::read_next_slice() {
  auto slice = slice_list_.pop_front();
  if (slice.has_value() && slice->is_from_shm) {
    if (current_pinned_) {
      pinned_list_.push_back(std::move(*slice));
    } else {
      buffer_manager_->recycle_buffer(std::move(*slice));
    }
  }
  current_pinned_ = false;
}

void LinkedBuffer::clean_pinned_list() {
  if (pinned_list_.size() == 0) {
    return;
  }
  current_pinned_ = false;
  while (pinned_list_.size() > 0) {
    if (auto slice = pinned_list_.pop_front()) {
      if (slice->is_from_shm) {
        buffer_manager_->recycle_buffer(std::move(*slice));
      }
      // Non-shm: heap storage freed by RAII.
    }
  }
}

// ---------------------------------------------------------------------------
// BufferReader
// ---------------------------------------------------------------------------

Result<Buf> LinkedBuffer::read_bytes(std::size_t size) {
  if (size == 0) {
    return Buf::exm(Bytes{});
  }
  if (len_ < size) {
    return std::unexpected(Err::kNotEnoughData);
  }

  if (slice_list_.front() != nullptr && slice_list_.front()->size() == 0) {
    read_next_slice();
  }

  if (auto* front = slice_list_.front();
      front != nullptr && front->size() >= size) {
    current_pinned_ = true;
    len_ -= size;
    return Buf::shm(slice_list_.front()->read(size));
  }

  // slow path
  len_ -= size;
  std::vector<std::uint8_t> result;
  result.reserve(size);
  while (size > 0) {
    auto* slice = slice_list_.front();
    if (slice == nullptr) {
      break;  // defensive; Rust relies on len_ >= size guaranteeing data
    }
    const auto read_data = slice->read(size);
    result.insert(result.end(), read_data.begin(), read_data.end());
    const std::size_t read_size = read_data.size();
    if (read_size != size) {
      read_next_slice();
    }
    size -= read_size;
  }
  return Buf::exm(Bytes::from_vec(std::move(result)));
}

Result<Buf> LinkedBuffer::peek(std::size_t size) {
  if (size == 0) {
    return Buf::exm(Bytes{});
  }
  if (len_ < size) {
    return std::unexpected(Err::kNotEnoughData);
  }

  if (auto* front = slice_list_.front(); front != nullptr) {
    const auto read_bytes = front->peek(size);
    if (read_bytes.size() == size) {
      current_pinned_ = true;
      return Buf::shm(read_bytes);
    }
  }

  // slow path
  std::vector<std::uint8_t> result;
  result.reserve(size);
  for (auto* e = slice_list_.front(); size > 0 && e != nullptr; e = e->next()) {
    const auto read_bytes = e->peek(size);
    result.insert(result.end(), read_bytes.begin(), read_bytes.end());
    size -= read_bytes.size();
  }
  return Buf::exm(Bytes::from_vec(std::move(result)));
}

Result<std::size_t> LinkedBuffer::discard(std::size_t size) {
  if (len_ < size) {
    return std::unexpected(Err::kNotEnoughData);
  }
  std::size_t n = 0;
  while (true) {
    auto* slice = slice_list_.front();
    if (slice == nullptr) {
      break;  // defensive
    }
    const std::size_t skip = slice->skip(size);
    n += skip;
    size -= skip;
    if (size == 0) {
      break;
    }
    read_next_slice();
  }
  len_ -= n;
  return n;
}

void LinkedBuffer::release_previous_read() {
  clean_pinned_list();

  if (slice_list_.size() == 0) {
    return;
  }

  if (slice_list_.front()->size() == 0 &&
      slice_list_.front() == slice_list_.write()) {
    buffer_manager_->recycle_buffer(std::move(*slice_list_.pop_front()));
    slice_list_.set_write(nullptr);
  }
}

// ---------------------------------------------------------------------------
// BufferWriter
// ---------------------------------------------------------------------------

Result<std::span<std::uint8_t>> LinkedBuffer::reserve(std::size_t size) {
  // 1. use the current slice
  if (slice_list_.write() == nullptr) {
    alloc(static_cast<std::uint32_t>(size));
    slice_list_.set_write(slice_list_.front());
  }
  auto* write_slice = slice_list_.write();
  if (auto ret = write_slice->reserve(size)) {
    len_ += size;
    return ret;
  }

  // 2. use the next slice
  if (auto* e = write_slice->next(); e != nullptr) {
    if (auto ret = e->reserve(size)) {
      slice_list_.set_write(e);
      len_ += size;
      return ret;
    }
  }

  // 3. allocate a new slice
  if (auto buf = buffer_manager_->alloc_shm_buffer(
          static_cast<std::uint32_t>(size))) {
    slice_list_.push_back(std::move(*buf));
  } else {
    // fallback
    std::size_t alloc_size = size;
    if (alloc_size < static_cast<std::size_t>(kDefaultSingleBufferSize)) {
      alloc_size = static_cast<std::size_t>(kDefaultSingleBufferSize);
    }
    slice_list_.push_back(
        BufferSlice::from_heap(std::vector<std::uint8_t>(alloc_size, 0)));
    is_from_shm_ = false;
  }
  slice_list_.set_write(slice_list_.back());
  len_ += size;
  return slice_list_.write()->reserve(size);
}

Result<std::size_t> LinkedBuffer::write_bytes(
    std::span<const std::uint8_t> bytes) {
  if (bytes.empty()) {
    return std::size_t{0};
  }
  if (slice_list_.write() == nullptr) {
    alloc(static_cast<std::uint32_t>(bytes.size()));
    slice_list_.set_write(slice_list_.front());
  }
  std::size_t n = 0;
  while (true) {
    n += slice_list_.write()->append(bytes.subspan(n));
    if (n < bytes.size()) {
      // The write slice must be used up here.
      if (slice_list_.write()->next() == nullptr) {
        // No allocated BufferSlice is left.
        alloc(static_cast<std::uint32_t>(bytes.size() - n));
      }
      slice_list_.set_write(slice_list_.write()->next());
    } else {
      break;
    }
  }
  len_ += n;
  return n;
}

}  // namespace shmipc::buffer
