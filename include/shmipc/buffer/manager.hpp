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
// C++ translation of shmipc-rs `src/buffer/manager.rs`.
//
// BufferManager's layout in share memory:
//   list_size 2 byte | reserved 2 byte | used_length 4 byte | buffer_list n
//
// Like the Rust version, managers are cached process-globally by path
// (BUFFER_MANAGERS) and reference counted; `add_global_buffer_manager_
// ref_count(path, -1)` unmaps once the count reaches zero.

#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <vector>

#include "shmipc/config.hpp"
#include "shmipc/buffer/list.hpp"
#include "shmipc/buffer/slice.hpp"
#include "shmipc/queue.hpp"

namespace shmipc::buffer {

class BufferManager {
 public:
  ~BufferManager() = default;
  // std::atomic is not movable, so the move operations are hand-rolled.
  BufferManager(BufferManager&& other) noexcept
      : lists_(std::move(other.lists_)),
        mem_(std::move(other.mem_)),
        min_slice_size_(other.min_slice_size_),
        max_slice_size_(other.max_slice_size_),
        mem_map_type_(other.mem_map_type_),
        path_(std::move(other.path_)),
        memfd_(other.memfd_) {
    ref_count_.store(other.ref_count_.load(std::memory_order_seq_cst),
                     std::memory_order_seq_cst);
  }
  BufferManager& operator=(BufferManager&&) noexcept = delete;

  static Result<std::shared_ptr<BufferManager>> get_with_memfd(
      const std::string& buffer_path_name, int memfd, std::uint32_t capacity,
      bool create, std::vector<SizePercentPair>& pairs);

  static Result<std::shared_ptr<BufferManager>> get_with_file(
      const std::string& shm_path, std::uint32_t capacity, bool create,
      std::vector<SizePercentPair>& pairs);

  // Create a buffer manager in share memory, used for the client side.
  static Result<BufferManager> create(
      std::span<const SizePercentPair> list_size_percent, std::string path,
      MemoryMap mem, std::uint32_t offset);

  // Mapping a buffer manager in share memory, used for the server side.
  static Result<BufferManager> mapping(std::string path, MemoryMap mem,
                                       std::uint32_t buffer_region_start_offset);

  std::uint32_t remain_size() const;

  // Alloc a single buffer slice, whose performance is better than
  // alloc_shm_buffers.
  Result<BufferSlice> alloc_shm_buffer(std::uint32_t size) const;

  // Returns the total allocated capacity (may be less than `size`).
  std::int64_t alloc_shm_buffers(SliceList& slices, std::uint32_t size) const;

  void recycle_buffer(BufferSlice slice) const;
  void recycle_buffers(BufferSlice slice) const;

  std::int64_t slice_size() const;

  Result<BufferSlice> read_buffer_slice(std::uint32_t offset) const;

  bool check_buffer_returned() const;

  // Spin up to 5s waiting for all buffers to be returned, then release the
  // mapping and remove the file / close the memfd.
  // (Rust: async fn; this port is synchronous, mirroring the 50x100ms spin.)
  void unmap();

  const std::string& path() const noexcept { return path_; }
  int memfd() const noexcept { return memfd_; }
  MemMapType map_type() const noexcept { return mem_map_type_; }
  std::int32_t ref_count() const noexcept {
    return ref_count_.load(std::memory_order_seq_cst);
  }
  void add_ref(std::int32_t delta) noexcept {
    ref_count_.fetch_add(delta, std::memory_order_seq_cst);
  }
  // Returns the previous ref count (Rust fetch_add semantics).
  std::int32_t ref_count_fetch_add(std::int32_t delta) noexcept {
    return ref_count_.fetch_add(delta, std::memory_order_seq_cst);
  }
  const MemoryMap& mem() const noexcept { return mem_; }
  const std::vector<BufferList>& lists() const noexcept { return lists_; }

 private:
  BufferManager() = default;

  // Ascending ordered by BufferList cap_per_buffer
  std::vector<BufferList> lists_;
  MemoryMap mem_;
  std::uint32_t min_slice_size_ = 0;
  std::uint32_t max_slice_size_ = 0;
  std::atomic<std::int32_t> ref_count_{1};
  MemMapType mem_map_type_ = MemMapType::kMemFd;
  std::string path_;
  int memfd_ = 0;
};

// Mirror of `add_global_buffer_manager_ref_count` (sync in this port).
void add_global_buffer_manager_ref_count(const std::string& path,
                                         std::int32_t c);

// Test helper: process-wide anonymous mapping (Rust: MmapOptions::map_anon).
Result<MemoryMap> map_anonymous(std::size_t len);

}  // namespace shmipc::buffer
