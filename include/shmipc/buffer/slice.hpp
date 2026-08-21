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
// C++ translation of shmipc-rs `src/buffer/slice.rs`.

#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <vector>

#include "shmipc/buffer/layout.hpp"
#include "shmipc/error.hpp"

namespace shmipc::buffer {

// BufferHeader is the header of a buffer slice; a non-owning view into the
// shared memory region.
// Layout: cap 4 byte | size 4 byte | start 4 byte | next 4 byte | flag 2 byte
//         | unused 2 byte
class BufferHeader {
 public:
  BufferHeader() = default;
  explicit BufferHeader(std::uint8_t* p) noexcept : p_(p) {}

  std::uint8_t* ptr() const noexcept { return p_; }
  explicit operator bool() const noexcept { return p_ != nullptr; }

  std::uint32_t next_buffer_offset() const noexcept;
  bool has_next() const noexcept;
  void clear_flag() const noexcept;
  void set_in_used() const noexcept;
  bool is_in_used() const noexcept;
  void link_next(std::uint32_t next) const noexcept;
  std::uint32_t cap() const noexcept;
  std::uint32_t size() const noexcept;
  void set_size(std::uint32_t size) const noexcept;
  std::uint32_t start() const noexcept;
  void set_start(std::uint32_t start) const noexcept;

  friend bool operator==(const BufferHeader& lhs,
                         const BufferHeader& rhs) noexcept {
    return lhs.p_ == rhs.p_;
  }

 private:
  std::uint8_t* p_ = nullptr;
};

// BufferSlice describes one slice of the shared-memory buffer pool, or a
// fallback heap slice (is_from_shm == false). Move-only: the slice owns its
// successor chain (`next_slice`) exactly like the Rust version owns `Box`ed
// nodes, and heap-backed slices own their storage (Rust reconstructs the
// `Vec` manually on every recycle path; RAII does it here).
class BufferSlice {
 public:
  BufferSlice() = default;

  // Mirrors `BufferSlice::new(header, data, offset_in_shm, is_from_shm)`.
  BufferSlice(std::optional<BufferHeader> header, std::span<std::uint8_t> data,
              std::uint32_t offset_in_shm, bool is_from_shm);

  // Fallback slice owning a heap allocation (Rust: vec![] + mem::forget).
  static BufferSlice from_heap(std::vector<std::uint8_t> storage);

  BufferSlice(const BufferSlice&) = delete;
  BufferSlice& operator=(const BufferSlice&) = delete;
  BufferSlice(BufferSlice&&) noexcept = default;
  BufferSlice& operator=(BufferSlice&&) noexcept = default;
  ~BufferSlice() = default;

  // Mirrors `update()`: flush size/start (and the link to the next slice)
  // into the shared-memory header.
  void update() const;

  // Mirrors `reset()`.
  void reset();

  std::size_t size() const noexcept { return write_index - read_index; }
  std::size_t remain() const noexcept {
    return static_cast<std::size_t>(cap) - write_index;
  }
  std::size_t capacity() const noexcept { return cap; }

  Result<std::span<std::uint8_t>> reserve(std::size_t size);
  std::size_t append(std::span<const std::uint8_t> data);
  std::span<const std::uint8_t> read(std::size_t size);
  std::span<const std::uint8_t> peek(std::size_t size) const;
  std::size_t skip(std::size_t size);

  BufferSlice* next() const noexcept { return next_slice_.get(); }

  // Chain ownership (Rust: `next_slice: Option<NonNull<BufferSlice>>` with
  // Box ownership spread across call sites; here it is centralized).
  void set_next(std::unique_ptr<BufferSlice> next) { next_slice_ = std::move(next); }
  std::unique_ptr<BufferSlice> take_next() { return std::move(next_slice_); }

  // Field-by-field comparison mirroring Rust's derived PartialEq (used by
  // the SliceList tests); heap storage is excluded (it does not exist in
  // Rust).
  friend bool operator==(const BufferSlice& lhs, const BufferSlice& rhs) noexcept;

  // Public fields, mirroring the Rust struct's pub fields.
  std::optional<BufferHeader> buffer_header;
  std::uint8_t* data = nullptr;
  std::uint32_t cap = 0;
  // use for prepend
  std::uint32_t start = 0;
  std::uint32_t offset_in_shm = 0;
  std::size_t read_index = 0;
  std::size_t write_index = 0;
  bool is_from_shm = false;

 private:
  std::unique_ptr<BufferSlice> next_slice_;
  // Owning storage for fallback (non-shm) slices; null for shm slices and
  // for slices borrowing memory owned elsewhere.
  std::shared_ptr<std::vector<std::uint8_t>> heap_storage_;
};

// Intrusive singly-linked list of heap-owned BufferSlice nodes, mirroring
// `SliceList`. `write_slice` is a non-owning cursor like in Rust.
class SliceList {
 public:
  SliceList() = default;

  SliceList(const SliceList&) = delete;
  SliceList& operator=(const SliceList&) = delete;
  SliceList(SliceList&&) noexcept = default;
  SliceList& operator=(SliceList&&) noexcept = default;

  BufferSlice* front() const noexcept { return front_; }
  BufferSlice* back() const noexcept { return back_; }
  BufferSlice* write() const noexcept { return write_; }
  void set_write(BufferSlice* s) noexcept { write_ = s; }
  std::size_t size() const noexcept { return len_; }

  void push_back(BufferSlice s);
  std::optional<BufferSlice> pop_front();
  // Detach everything after the write cursor; returns the detached chain's
  // head (owning the rest through its next pointers), or nullopt when the
  // write cursor is the back. Mirrors `split_from_write()`.
  std::optional<BufferSlice> split_from_write();

 private:
  std::unique_ptr<BufferSlice> front_owner_;  // owns the first node
  BufferSlice* front_ = nullptr;
  BufferSlice* write_ = nullptr;
  BufferSlice* back_ = nullptr;
  std::size_t len_ = 0;
};

}  // namespace shmipc::buffer
