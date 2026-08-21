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
// C++ translation of shmipc-rs `src/buffer/linked.rs` plus the
// BufferReader/BufferWriter traits from `src/buffer/mod.rs`.

#pragma once

#include <cstddef>
#include <memory>
#include <mutex>
#include <span>

#include "shmipc/buffer/buf.hpp"
#include "shmipc/buffer/manager.hpp"
#include "shmipc/buffer/slice.hpp"
#include "shmipc/error.hpp"

namespace shmipc::buffer {

// Mirror of Rust trait `BufferReader`.
//
// Read `size` bytes from share memory; if `release_previous_read()` is
// called, the results of previous `read_bytes()` become invalid.
class BufferReader {
 public:
  virtual ~BufferReader() = default;

  virtual Result<Buf> read_bytes(std::size_t size) = 0;

  // Peek `size` bytes from share memory. Unlike `read_bytes()`, `peek()`
  // does not decrease the unread size. Results of a previous `peek()` stay
  // valid until `release_previous_read()` is called.
  virtual Result<Buf> peek(std::size_t size) = 0;

  // Drop data of the given size.
  virtual Result<std::size_t> discard(std::size_t size) = 0;

  // Call when it is safe to drop all previous results of `read_bytes()` and
  // `peek()`, otherwise shm memory will leak.
  virtual void release_previous_read() = 0;
};

// Mirror of Rust trait `BufferWriter`.
class BufferWriter {
 public:
  virtual ~BufferWriter() = default;

  // Reserve `size` bytes of share memory space for zero-copy writes.
  virtual Result<std::span<std::uint8_t>> reserve(std::size_t size) = 0;

  // Copy data to share memory; returns the copied size on success.
  virtual Result<std::size_t> write_bytes(
      std::span<const std::uint8_t> bytes) = 0;
};

class LinkedBuffer : public BufferReader, public BufferWriter {
 public:
  explicit LinkedBuffer(std::shared_ptr<BufferManager> buffer_manager)
      : buffer_manager_(std::move(buffer_manager)) {}

  LinkedBuffer(const LinkedBuffer&) = delete;
  LinkedBuffer& operator=(const LinkedBuffer&) = delete;
  LinkedBuffer(LinkedBuffer&&) noexcept = default;
  LinkedBuffer& operator=(LinkedBuffer&&) noexcept = default;

  void alloc(std::uint32_t size);
  void done(bool end_stream);
  void append_buffer_slice(BufferSlice slice);
  void release_previous_read_and_reserve();
  void recycle();
  void clean();

  std::uint32_t root_buf_offset() const;
  std::size_t cap() const;
  std::size_t len() const noexcept { return len_; }
  std::size_t& len_mut() noexcept { return len_; }
  bool is_empty() const noexcept { return len_ == 0; }
  bool is_from_share_memory() const noexcept { return is_from_shm_; }

  const SliceList& slice_list() const noexcept { return slice_list_; }
  SliceList& slice_list_mut() noexcept { return slice_list_; }

  // BufferReader
  Result<Buf> read_bytes(std::size_t size) override;
  Result<Buf> peek(std::size_t size) override;
  Result<std::size_t> discard(std::size_t size) override;
  void release_previous_read() override;

  // BufferWriter
  // 1. if the current slice can contain the size, reserve and return it.
  // 2. if the next slice can contain the size, reserve and return it.
  // 3. allocate a new slice which can contain the size.
  Result<std::span<std::uint8_t>> reserve(std::size_t size) override;
  Result<std::size_t> write_bytes(std::span<const std::uint8_t> bytes) override;

  // Test accessor (Rust tests read pinned_list.size()).
  std::size_t pinned_list_size() const noexcept { return pinned_list_.size(); }

 private:
  void read_next_slice();
  void clean_pinned_list();

  SliceList slice_list_;
  // LinkedBuffer's recycle() holds this lock; in most scenarios there is no
  // competition, but stream.close() may race with the session's receive
  // path (see the Rust comment). Heap-held so LinkedBuffer stays movable.
  std::unique_ptr<std::mutex> recycle_mtx_ = std::make_unique<std::mutex>();
  std::shared_ptr<BufferManager> buffer_manager_;
  // Already-read slices dropped by read_bytes() are saved here instead of
  // recycled instantly; they are recycled on release_previous_read().
  SliceList pinned_list_;
  bool current_pinned_ = false;
  bool end_stream_ = false;
  bool is_from_shm_ = true;
  std::size_t len_ = 0;
};

}  // namespace shmipc::buffer
