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

#include "shmipc/buffer/list.hpp"

#include <cstdio>
#include <cstring>
#include <limits>

#include "shmipc/log.hpp"

namespace shmipc::buffer {

namespace {

template <typename Atomic>
Atomic* atomic_at(std::uint8_t* base, std::size_t offset) {
  return reinterpret_cast<Atomic*>(base + offset);
}

void store_u32(std::uint8_t* p, std::uint32_t v) {
  std::memcpy(p, &v, sizeof(v));
}

}  // namespace

static_assert(sizeof(std::atomic<std::int32_t>) == 4);
static_assert(sizeof(std::atomic<std::uint32_t>) == 4);
static_assert(std::atomic<std::int32_t>::is_always_lock_free);
static_assert(std::atomic<std::uint32_t>::is_always_lock_free);

Result<BufferList> BufferList::create(std::uint32_t buffer_num,
                                      std::uint32_t cap_per_buffer,
                                      std::uint8_t* mem, std::size_t mem_len,
                                      std::uint32_t offset_in_mem) {
  if (buffer_num == 0 || cap_per_buffer == 0) {
    char buf[128];
    std::snprintf(buf, sizeof(buf),
                  "buffer_num:%u and cap_per_buffer:%u could not be 0",
                  buffer_num, cap_per_buffer);
    return std::unexpected(Error::other(buf));
  }
  // 64-bit arithmetic: the Rust debug build panics on u32 overflow here; the
  // C++ port detects the overflow and returns an error instead.
  const std::uint64_t at_least_size_64 =
      static_cast<std::uint64_t>(kBufferListHeaderSize) +
      static_cast<std::uint64_t>(buffer_num) *
          (static_cast<std::uint64_t>(cap_per_buffer) + kBufferHeaderSize);
  if (at_least_size_64 > std::numeric_limits<std::uint32_t>::max()) {
    return std::unexpected(Error::other(
        "buffer list size overflow: buffer_num/cap_per_buffer too large"));
  }
  const std::uint32_t at_least_size =
      static_cast<std::uint32_t>(at_least_size_64);
  const auto mem_len32 = static_cast<std::uint32_t>(mem_len);
  if (mem_len < static_cast<std::size_t>(offset_in_mem) + at_least_size ||
      offset_in_mem > mem_len32 || at_least_size > mem_len32) {
    char buf[256];
    std::snprintf(buf, sizeof(buf),
                  "mem's size is at least:%u but:%zu offset_in_mem:%u "
                  "at_least_size:%u",
                  offset_in_mem + at_least_size, mem_len, offset_in_mem,
                  at_least_size);
    return std::unexpected(Error::other(buf));
  }

  const std::uint32_t buffer_region_start = offset_in_mem + kBufferListHeaderSize;
  const std::uint32_t buffer_region_end = offset_in_mem + at_least_size;
  if (buffer_region_end <= buffer_region_start) {
    char buf[160];
    std::snprintf(buf, sizeof(buf),
                  "buffer_region_start:%u buffer_region_end:%u slice bounds "
                  "out of range",
                  buffer_region_start, buffer_region_end);
    return std::unexpected(Error::other(buf));
  }

  BufferList b;
  const std::size_t offset = offset_in_mem;
  b.size_ = atomic_at<std::atomic<std::int32_t>>(mem, offset);
  b.cap_ = atomic_at<std::atomic<std::uint32_t>>(mem, offset + 4);
  b.head_ = atomic_at<std::atomic<std::uint32_t>>(mem, offset + 8);
  b.tail_ = atomic_at<std::atomic<std::uint32_t>>(mem, offset + 12);
  b.cap_per_buffer_ = reinterpret_cast<std::uint32_t*>(mem + offset + 16);
  b.counter_ = atomic_at<std::atomic<std::int32_t>>(
      mem, offset + kCounterListOffset);
  b.buffer_region_ = mem + offset + kBufferListHeaderSize;
  b.buffer_region_len_ = at_least_size - kBufferListHeaderSize;
  b.buffer_region_offset_in_shm_ = offset_in_mem + kBufferListHeaderSize;
  b.offset_in_shm_ = offset_in_mem;

  b.size_->store(static_cast<std::int32_t>(buffer_num), std::memory_order_seq_cst);
  b.cap_->store(buffer_num, std::memory_order_seq_cst);
  b.head_->store(0, std::memory_order_seq_cst);
  b.tail_->store((buffer_num - 1) * (cap_per_buffer + kBufferHeaderSize),
                 std::memory_order_seq_cst);
  *b.cap_per_buffer_ = cap_per_buffer;
  b.counter_->store(0, std::memory_order_seq_cst);

  {
    char buf[256];
    std::snprintf(buf, sizeof(buf),
                  "create buffer list: buffer_num:%u cap_per_buffer:%u "
                  "offset_in_mem:%u need_size:%u buffer_region_len:%u",
                  buffer_num, cap_per_buffer, offset_in_mem, at_least_size,
                  at_least_size - kBufferListHeaderSize);
    SHMIPC_INFO(buf);
  }

  std::uint32_t current = 0;
  std::uint32_t next = 0;
  for (std::uint32_t i = 0; i < buffer_num; ++i) {
    next = current + cap_per_buffer + kBufferHeaderSize;
    store_u32(b.buffer_region_ + current, cap_per_buffer);
    store_u32(b.buffer_region_ + current + kBufferSizeOffset, 0);
    store_u32(b.buffer_region_ + current + kBufferDataStartOffset, 0);
    if (i < (buffer_num - 1)) {
      store_u32(b.buffer_region_ + current + kNextBufferOffset, next);
      store_u32(b.buffer_region_ + current + kBufferFlagOffset, 0);
      b.buffer_region_[current + kBufferFlagOffset] |= kHasNextBufferFlag;
    }
    current = next;
  }
  // Clear the flag word of the tail buffer.
  std::uint8_t* tail =
      b.buffer_region_ + b.tail_->load(std::memory_order_seq_cst);
  store_u32(tail + kBufferFlagOffset, 0);

  return b;
}

Result<BufferList> BufferList::mapping(const std::uint8_t* mem,
                                       std::size_t mem_len,
                                       std::uint32_t offset_in_shm) {
  if (mem_len < static_cast<std::size_t>(offset_in_shm) + kBufferListHeaderSize) {
    char buf[160];
    std::snprintf(buf, sizeof(buf),
                  "mapping buffer list failed, mem's size is at least %u",
                  offset_in_shm + kBufferListHeaderSize);
    return std::unexpected(Error::other(buf));
  }

  auto* base = const_cast<std::uint8_t*>(mem);
  const std::size_t offset = offset_in_shm;

  BufferList b;
  b.size_ = atomic_at<std::atomic<std::int32_t>>(base, offset);
  b.cap_ = atomic_at<std::atomic<std::uint32_t>>(base, offset + 4);
  b.head_ = atomic_at<std::atomic<std::uint32_t>>(base, offset + 8);
  b.tail_ = atomic_at<std::atomic<std::uint32_t>>(base, offset + 12);
  b.cap_per_buffer_ = reinterpret_cast<std::uint32_t*>(base + offset + 16);
  // Upstream fix: the Rust version reads the counter from offset +24 here
  // while `create` writes it at +20; both sides use +20 in this port.
  b.counter_ = atomic_at<std::atomic<std::int32_t>>(
      base, offset + kCounterListOffset);

  // 64-bit arithmetic guards against u32 overflow on garbage shared memory
  // (the Rust u32 arithmetic wraps/panics here instead).
  const std::uint64_t need_size_64 =
      static_cast<std::uint64_t>(kBufferListHeaderSize) +
      static_cast<std::uint64_t>(b.cap_->load(std::memory_order_seq_cst)) *
          (static_cast<std::uint64_t>(*b.cap_per_buffer_) + kBufferHeaderSize);
  if (need_size_64 > std::numeric_limits<std::uint32_t>::max()) {
    return std::unexpected(Error::other(
        "mapping buffer list failed: corrupted header (size overflow)"));
  }
  const std::uint32_t need_size = static_cast<std::uint32_t>(need_size_64);
  if (offset_in_shm + need_size > mem_len ||
      offset_in_shm + need_size < offset_in_shm + kBufferListHeaderSize) {
    char buf[320];
    std::snprintf(
        buf, sizeof(buf),
        "mapping buffer list failed, size:%d cap:%u head:%u tail:%u "
        "cap_per_buffer:%u err: mem's size is at least %u but:%zu",
        b.size_->load(std::memory_order_seq_cst),
        b.cap_->load(std::memory_order_seq_cst),
        b.head_->load(std::memory_order_seq_cst),
        b.tail_->load(std::memory_order_seq_cst), *b.cap_per_buffer_, need_size,
        mem_len);
    return std::unexpected(Error::other(buf));
  }

  b.buffer_region_ = base + offset + kBufferListHeaderSize;
  b.buffer_region_len_ = need_size - kBufferListHeaderSize;
  b.buffer_region_offset_in_shm_ = offset_in_shm + kBufferListHeaderSize;
  b.offset_in_shm_ = offset_in_shm;
  return b;
}

void BufferList::push(BufferSlice buffer) const {
  buffer.reset();
  while (true) {
    std::uint32_t old_tail = tail_->load(std::memory_order_seq_cst);
    const std::uint32_t new_tail =
        buffer.offset_in_shm - buffer_region_offset_in_shm_;
    if (tail_->compare_exchange_strong(old_tail, new_tail,
                                       std::memory_order_seq_cst,
                                       std::memory_order_seq_cst)) {
      BufferHeader(buffer_region_ + old_tail).link_next(new_tail);
      size_->fetch_add(1, std::memory_order_seq_cst);
      counter_->fetch_sub(1, std::memory_order_seq_cst);
      return;
    }
  }
}

Result<BufferSlice> BufferList::pop() const {
  std::uint32_t old_head = head_->load(std::memory_order_seq_cst);
  const std::int32_t remain = size_->fetch_sub(1, std::memory_order_seq_cst);

  if (remain <= 1 ||
      old_head + kBufferHeaderSize + *cap_per_buffer_ > buffer_region_len_) {
    size_->fetch_add(1, std::memory_order_seq_cst);
    return std::unexpected(Err::kNoMoreBuffer);
  }

  // when data races occurred, max retry 200 times.
  for (int retry = 0; retry < 200; ++retry) {
    const BufferHeader bh(buffer_region_ + old_head);
    if (bh.has_next()) {
      if (head_->compare_exchange_strong(old_head, bh.next_buffer_offset(),
                                         std::memory_order_seq_cst,
                                         std::memory_order_seq_cst)) {
        const BufferHeader h(buffer_region_ + old_head);
        h.clear_flag();
        h.set_in_used();
        counter_->fetch_add(1, std::memory_order_seq_cst);
        return BufferSlice(
            h,
            std::span<std::uint8_t>(buffer_region_ + old_head + kBufferHeaderSize,
                                    *cap_per_buffer_),
            old_head + buffer_region_offset_in_shm_, true);
      }
    } else {
      // don't alloc the last slice
      if (size_->load(std::memory_order_seq_cst) <= 1) {
        size_->fetch_add(1, std::memory_order_seq_cst);
        return std::unexpected(Err::kNoMoreBuffer);
      }
    }
    old_head = head_->load(std::memory_order_seq_cst);
  }
  size_->fetch_add(1, std::memory_order_seq_cst);
  return std::unexpected(Err::kNoMoreBuffer);
}

}  // namespace shmipc::buffer
