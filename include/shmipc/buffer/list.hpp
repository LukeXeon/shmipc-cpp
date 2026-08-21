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
// C++ translation of shmipc-rs `src/buffer/list.rs`.
//
// BufferList's layout in share memory:
//   size 4 byte | cap 4 byte | head 4 byte | tail 4 byte | cap_per_buffer
//   4 byte | counter 4 byte | reserved 8 byte | buffer_region n byte
//
// Thread safe, lock free. Supports push & pop concurrently even across
// different processes. The atomics live directly in the mapped shared memory
// (same technique as the Rust version; std::atomic<uint32_t> is verified to
// be 4 bytes and always lock-free).
//
// Upstream fix (approved for this port): the Rust `BufferList::mapping`
// reads `counter` from header offset +24 while `BufferList::create` writes
// it at +20 (list.rs:109 vs list.rs:191). Both use +20 here.

#pragma once

#include <atomic>
#include <cstdint>

#include "shmipc/buffer/slice.hpp"
#include "shmipc/error.hpp"

namespace shmipc::buffer {

// size 4 | cap 4 | head 4 | tail 4 | capPerBuffer 4 | counter 4 | reserved 8
inline constexpr std::uint32_t kBufferListHeaderSize = 36;
inline constexpr std::uint32_t kCounterListOffset = 20;

class BufferList {
 public:
  BufferList() = default;

  // Create a new BufferList in share memory, used for the client side.
  static Result<BufferList> create(std::uint32_t buffer_num,
                                   std::uint32_t cap_per_buffer,
                                   std::uint8_t* mem, std::size_t mem_len,
                                   std::uint32_t offset_in_mem);

  // Mapping a BufferList from share memory, used for the server side.
  static Result<BufferList> mapping(const std::uint8_t* mem,
                                    std::size_t mem_len,
                                    std::uint32_t offset_in_shm);

  // Push a buffer to the list (consumes the slice, like the Rust version).
  void push(BufferSlice buffer) const;

  // Pop a buffer from the list.
  // When data races occur, it retries 200 times at most; if it still fails
  // to pop, returns NoMoreBuffer.
  Result<BufferSlice> pop() const;

  // When the size is 1, pop is not allowed, solving a problem about
  // concurrent operating.
  std::int64_t remain() const {
    return static_cast<std::int64_t>(
               size_->load(std::memory_order_seq_cst)) -
           1;
  }

  // Accessors used by tests and BufferManager (Rust: pub(crate) fields).
  std::int32_t size_load() const { return size_->load(std::memory_order_seq_cst); }
  std::uint32_t cap_load() const { return cap_->load(std::memory_order_seq_cst); }
  std::uint32_t head_load() const { return head_->load(std::memory_order_seq_cst); }
  std::uint32_t tail_load() const { return tail_->load(std::memory_order_seq_cst); }
  std::int32_t counter_load() const {
    return counter_->load(std::memory_order_seq_cst);
  }
  std::uint32_t cap_per_buffer() const { return *cap_per_buffer_; }
  std::uint32_t offset_in_shm() const { return offset_in_shm_; }

 private:
  // the number of free buffers in list
  std::atomic<std::int32_t>* size_ = nullptr;
  // the capacity of list
  std::atomic<std::uint32_t>* cap_ = nullptr;
  // points to the first free buffer (offset in buffer_region)
  std::atomic<std::uint32_t>* head_ = nullptr;
  // points to the last free buffer (offset in buffer_region)
  std::atomic<std::uint32_t>* tail_ = nullptr;
  // the capacity of each buffer
  std::uint32_t* cap_per_buffer_ = nullptr;
  std::atomic<std::int32_t>* counter_ = nullptr;
  // underlying memory
  std::uint8_t* buffer_region_ = nullptr;
  std::uint32_t buffer_region_len_ = 0;
  std::uint32_t buffer_region_offset_in_shm_ = 0;
  // the buffer_list's location offset in share memory
  std::uint32_t offset_in_shm_ = 0;
};

inline std::uint32_t count_buffer_list_mem_size(std::uint32_t buffer_num,
                                                std::uint32_t cap_per_buffer) {
  return kBufferListHeaderSize +
         buffer_num * (cap_per_buffer + kBufferHeaderSize);
}

}  // namespace shmipc::buffer
