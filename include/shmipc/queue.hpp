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
// C++ translation of shmipc-rs `src/queue.rs`.
//
// Queue is a single-producer (put is mutex-protected) / single-consumer
// (pop is lock-free) ring mapped into shared memory. The binary layout must
// stay identical to the Rust implementation (x86_64 variant):
//
//   offset 0:  cap      u32
//   offset 4:  head     i64 (unaligned)
//   offset 12: tail     i64 (unaligned)
//   offset 20: working_flag u32 (atomic)
//   offset 24: elements [cap] x { seq_id u32, offset_in_shm_buf u32,
//                                 status u32 }  (native endian)

#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>

#include "shmipc/config.hpp"
#include "shmipc/error.hpp"

namespace shmipc {

inline constexpr std::size_t kQueueHeaderLength = 24;

struct QueueElement {
  std::uint32_t seq_id = 0;
  std::uint32_t offset_in_shm_buf = 0;
  std::uint32_t status = 0;
};

class Queue {
 public:
  Queue() = default;

  // Create a fresh queue header in `data` with the given capacity.
  static Queue create_from_bytes(std::uint8_t* data, std::uint32_t cap);

  // Map an existing queue header (created by the peer process).
  static Queue mapping_from_bytes(std::uint8_t* data);

  Queue(const Queue&) = delete;
  Queue& operator=(const Queue&) = delete;
  Queue(Queue&&) noexcept = default;
  Queue& operator=(Queue&&) noexcept = default;

  Result<Unit> put(const QueueElement& element) const;
  Result<QueueElement> pop() const;

  bool is_full() const { return size() == cap_; }
  bool is_empty() const { return size() == 0; }
  std::int64_t size() const {
    return load_tail_unaligned() - load_head_unaligned();
  }

  bool consumer_is_working() const;
  // Returns true if the working flag transition happened.
  bool mark_working() const;
  // Returns true when the queue is drained and the flag stays cleared.
  bool mark_not_working() const;

 private:
  std::int64_t load_head_unaligned() const;
  std::int64_t load_tail_unaligned() const;
  void store_head_unaligned(std::int64_t v) const;
  void store_tail_unaligned(std::int64_t v) const;

  std::int64_t* head_ = nullptr;  // unaligned, in shared memory
  std::int64_t* tail_ = nullptr;  // unaligned, in shared memory
  std::atomic<std::uint32_t>* working_flag_ = nullptr;
  const std::uint8_t* queue_bytes_on_memory_ = nullptr;
  std::int64_t cap_ = 0;
  std::size_t len_ = 0;
  // Heap-held so Queue stays movable (Rust Mutex<()> is movable).
  mutable std::unique_ptr<std::mutex> lock_ = std::make_unique<std::mutex>();
};

// RAII mmap wrapper used by QueueManager (and BufferManager).
class MemoryMap {
 public:
  MemoryMap() = default;
  MemoryMap(void* ptr, std::size_t len) : ptr_(ptr), len_(len) {}
  ~MemoryMap() { unmap(); }

  MemoryMap(const MemoryMap&) = delete;
  MemoryMap& operator=(const MemoryMap&) = delete;
  MemoryMap(MemoryMap&& other) noexcept
      : ptr_(other.ptr_), len_(other.len_) {
    other.ptr_ = nullptr;
    other.len_ = 0;
  }
  MemoryMap& operator=(MemoryMap&& other) noexcept {
    if (this != &other) {
      unmap();
      ptr_ = other.ptr_;
      len_ = other.len_;
      other.ptr_ = nullptr;
      other.len_ = 0;
    }
    return *this;
  }

  void unmap();

  std::uint8_t* data() const noexcept { return static_cast<std::uint8_t*>(ptr_); }
  std::size_t size() const noexcept { return len_; }

 private:
  void* ptr_ = nullptr;
  std::size_t len_ = 0;
};

class QueueManager {
 public:
  QueueManager() = default;
  QueueManager(QueueManager&&) noexcept = default;
  QueueManager& operator=(QueueManager&&) noexcept = default;
  ~QueueManager();

  static Result<QueueManager> create_with_memfd(const std::string& queue_path,
                                                std::uint32_t queue_cap);
  static Result<QueueManager> create_with_file(const std::string& shm_path,
                                               std::uint32_t queue_cap);
  static Result<QueueManager> mapping_with_memfd(const std::string& queue_path,
                                                 int memfd);
  static Result<QueueManager> mapping_with_file(const std::string& shm_path);

  // Remove the backing file (kDevShmFile) or close the memfd (kMemFd).
  // Mirrors Rust `QueueManager::unmap()` (the mapping itself is released by
  // the destructor, like MmapMut drop).
  void unmap();

  const std::string& path() const noexcept { return path_; }
  int memfd() const noexcept { return memfd_; }
  MemMapType map_type() const noexcept { return map_type_; }

  Queue send_queue;
  Queue recv_queue;

 private:
  std::string path_;
  int memfd_ = 0;
  MemoryMap mem_;
  MemMapType map_type_ = MemMapType::kMemFd;
};

std::size_t count_queue_mem_size(std::uint32_t queue_cap);

}  // namespace shmipc
