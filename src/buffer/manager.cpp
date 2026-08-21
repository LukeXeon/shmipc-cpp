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

#ifndef _GNU_SOURCE
#define _GNU_SOURCE  // memfd_create
#endif

#include "shmipc/buffer/manager.hpp"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <limits>
#include <mutex>
#include <thread>
#include <unordered_map>

#include "shmipc/consts.hpp"
#include "shmipc/log.hpp"
#include "shmipc/util/dev_shm.hpp"

namespace shmipc::buffer {

namespace {

std::uint16_t load_u16(const std::uint8_t* p) {
  std::uint16_t v;
  std::memcpy(&v, p, sizeof(v));
  return v;
}

std::uint32_t load_u32(const std::uint8_t* p) {
  std::uint32_t v;
  std::memcpy(&v, p, sizeof(v));
  return v;
}

void store_u16(std::uint8_t* p, std::uint16_t v) { std::memcpy(p, &v, 2); }
void store_u32(std::uint8_t* p, std::uint32_t v) { std::memcpy(p, &v, 4); }

// Global registry, mirror of Rust BUFFER_MANAGERS.
std::mutex& registry_mutex() {
  static std::mutex m;
  return m;
}

std::unordered_map<std::string, std::shared_ptr<BufferManager>>& registry() {
  static std::unordered_map<std::string, std::shared_ptr<BufferManager>> r;
  return r;
}

Result<MemoryMap> mmap_fd_len(int fd, std::size_t len) {
  void* ptr = ::mmap(nullptr, len, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  if (ptr == MAP_FAILED) {
    return std::unexpected(Error::from_errno(errno));
  }
  return MemoryMap{ptr, len};
}

}  // namespace

Result<MemoryMap> map_anonymous(std::size_t len) {
  void* ptr =
      ::mmap(nullptr, len, PROT_READ | PROT_WRITE,
             MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (ptr == MAP_FAILED) {
    return std::unexpected(Error::from_errno(errno));
  }
  return MemoryMap{ptr, len};
}

// ---------------------------------------------------------------------------
// BufferManager factory functions
// ---------------------------------------------------------------------------

Result<std::shared_ptr<BufferManager>> BufferManager::get_with_memfd(
    const std::string& buffer_path_name, int memfd, std::uint32_t capacity,
    bool create, std::vector<SizePercentPair>& pairs) {
  {
    const std::lock_guard<std::mutex> guard(registry_mutex());
    auto it = registry().find(buffer_path_name);
    if (it != registry().end()) {
      it->second->add_ref(1);
      if (memfd > 0 && !create) {
        // FIXME: whether need to close memfd? (kept from upstream)
        ::close(memfd);
      }
      return it->second;
    }
  }

  if (create) {
    const std::string name = "shmipc" + buffer_path_name;
    const int fd = ::memfd_create(name.c_str(), 0);
    if (fd < 0) {
      return std::unexpected(Error::other(
          "BufferManager get_with_memfd memfd_create failed: " +
          std::string(std::strerror(errno))));
    }
    if (::ftruncate(fd, static_cast<off_t>(capacity)) != 0) {
      const int err = errno;
      ::close(fd);
      return std::unexpected(Error::other(
          "BufferManager get_with_memfd truncate share memory failed: " +
          std::string(std::strerror(err))));
    }
    memfd = fd;
  } else {
    struct stat st {};
    if (::fstat(memfd, &st) != 0) {
      return std::unexpected(Error::other(
          "BufferManager get_with_memfd mapping failed: " +
          std::string(std::strerror(errno))));
    }
    if (st.st_size < 0 ||
        st.st_size > static_cast<off_t>(std::numeric_limits<std::uint32_t>::max())) {
      char buf[128];
      std::snprintf(buf, sizeof(buf),
                    "BufferManager get_with_memfd invalid share memory size: "
                    "%lld",
                    static_cast<long long>(st.st_size));
      return std::unexpected(Error::other(buf));
    }
    capacity = static_cast<std::uint32_t>(st.st_size);
  }

  auto mem = mmap_fd_len(memfd, capacity);
  if (!mem) {
    if (!create) {
      // The fd belongs to the peer in mapping mode; don't close it here,
      // mirroring the Rust version which borrows the fd for mapping.
    } else {
      ::close(memfd);
    }
    return std::unexpected(Error::other(
        "BufferManager get_with_memfd mmap failed: " + mem.error().message()));
  }

  Result<BufferManager> bm_res =
      create
          ? ([&] {
              std::sort(pairs.begin(), pairs.end(),
                        [](const auto& a, const auto& b) {
                          return a.size < b.size;
                        });
              return BufferManager::create(pairs, buffer_path_name,
                                           std::move(*mem), 0);
            }())
          : BufferManager::mapping(buffer_path_name, std::move(*mem), 0);
  if (!bm_res) {
    return std::unexpected(bm_res.error());
  }

  std::shared_ptr<BufferManager> bm =
      std::shared_ptr<BufferManager>(new BufferManager(std::move(*bm_res)));
  bm->memfd_ = memfd;
  bm->mem_map_type_ = MemMapType::kMemFd;

  const std::lock_guard<std::mutex> guard(registry_mutex());
  registry().emplace(buffer_path_name, bm);
  return bm;
}

Result<std::shared_ptr<BufferManager>> BufferManager::get_with_file(
    const std::string& shm_path, std::uint32_t capacity, bool create,
    std::vector<SizePercentPair>& pairs) {
  {
    const std::lock_guard<std::mutex> guard(registry_mutex());
    auto it = registry().find(shm_path);
    if (it != registry().end()) {
      it->second->add_ref(1);
      return it->second;
    }
  }

  // ignore mkdir error
  std::error_code ec;
  const std::filesystem::path path(shm_path);
  std::filesystem::create_directories(
      path.has_parent_path() ? path.parent_path() : std::filesystem::path("/"),
      ec);
  std::filesystem::permissions(shm_path, std::filesystem::perms::owner_all |
                                             std::filesystem::perms::group_all |
                                             std::filesystem::perms::others_all,
                               ec);

  int fd = -1;
  if (create) {
    if (!can_create_on_dev_shm(capacity, shm_path)) {
      return std::unexpected(Error::other(
          "get_global_buffer_manager can not create on dev shm"));
    }
    fd = ::open(shm_path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0777);
    if (fd < 0) {
      return std::unexpected(Error::from_errno(errno));
    }
    if (::fchmod(fd, 0777) != 0 ||
        ::ftruncate(fd, static_cast<off_t>(capacity)) != 0) {
      const int err = errno;
      ::close(fd);
      return std::unexpected(Error::from_errno(err));
    }
  } else {
    // File flag doesn't include O_CREAT, because in this case the share
    // memory should have been created by the peer.
    fd = ::open(shm_path.c_str(), O_RDWR);
    if (fd < 0) {
      return std::unexpected(Error::from_errno(errno));
    }
    struct stat st {};
    if (::fstat(fd, &st) != 0) {
      const int err = errno;
      ::close(fd);
      return std::unexpected(Error::other(
          "get_global_buffer_manager mapping failed, " +
          std::string(std::strerror(err))));
    }
    if (st.st_size < 0 || static_cast<std::uint64_t>(st.st_size) >
                              std::numeric_limits<std::uint32_t>::max()) {
      char buf[128];
      std::snprintf(buf, sizeof(buf),
                    "get_global_buffer_manager invalid share memory size: %lld",
                    static_cast<long long>(st.st_size));
      ::close(fd);
      return std::unexpected(Error::other(buf));
    }
    capacity = static_cast<std::uint32_t>(st.st_size);
  }

  auto mem = mmap_fd_len(fd, capacity);
  ::close(fd);  // the mapping keeps the file alive
  if (!mem) {
    return std::unexpected(mem.error());
  }

  Result<BufferManager> bm_res =
      create
          ? ([&] {
              std::sort(pairs.begin(), pairs.end(),
                        [](const auto& a, const auto& b) {
                          return a.size < b.size;
                        });
              return BufferManager::create(pairs, shm_path, std::move(*mem), 0);
            }())
          : BufferManager::mapping(shm_path, std::move(*mem), 0);
  if (!bm_res) {
    return std::unexpected(bm_res.error());
  }

  std::shared_ptr<BufferManager> bm =
      std::shared_ptr<BufferManager>(new BufferManager(std::move(*bm_res)));
  bm->mem_map_type_ = MemMapType::kDevShmFile;

  const std::lock_guard<std::mutex> guard(registry_mutex());
  registry().emplace(shm_path, bm);
  return bm;
}

Result<BufferManager> BufferManager::create(
    std::span<const SizePercentPair> list_size_percent, std::string path,
    MemoryMap mem, std::uint32_t offset) {
  if (mem.size() <= static_cast<std::size_t>(offset)) {
    char buf[128];
    std::snprintf(buf, sizeof(buf), "mem's size is at least: %u, but: %zu",
                  offset + 1, mem.size());
    return std::unexpected(Error::other(buf));
  }

  // number of list 2 byte | 2 byte reserve | used_length 4 byte
  const std::uint64_t buffer_region_cap =
      static_cast<std::uint64_t>(mem.size()) - offset -
      static_cast<std::uint64_t>(kBufferListHeaderSize) *
          list_size_percent.size() -
      kBufferManagerHeaderSize;
  {
    char buf[256];
    std::snprintf(buf, sizeof(buf),
                  "create buffer manager path:%s mem_size:%zu "
                  "buffer_region_cap:%llu offset:%u",
                  path.c_str(), mem.size(),
                  static_cast<unsigned long long>(buffer_region_cap), offset);
    SHMIPC_INFO(buf);
  }
  store_u16(mem.data() + offset,
            static_cast<std::uint16_t>(list_size_percent.size()));

  std::uint32_t had_used_offset = kBufferManagerHeaderSize + offset;
  std::vector<BufferList> free_buffer_lists;
  free_buffer_lists.reserve(list_size_percent.size());
  std::uint32_t sum_percent = 0;
  for (const auto& pair : list_size_percent) {
    sum_percent += pair.percent;
    if (sum_percent > 100) {
      return std::unexpected(Error::other(
          "the sum of all size_percent_pairs's percent must be equals 100"));
    }
    const std::uint32_t buffer_num = static_cast<std::uint32_t>(
        buffer_region_cap * pair.percent / 100 /
        (static_cast<std::uint64_t>(pair.size) + kBufferHeaderSize));
    const std::uint32_t need_size =
        count_buffer_list_mem_size(buffer_num, pair.size);
    auto free_list = BufferList::create(buffer_num, pair.size, mem.data(),
                                        mem.size(), had_used_offset);
    if (!free_list) {
      return std::unexpected(free_list.error());
    }
    free_buffer_lists.push_back(std::move(*free_list));
    had_used_offset += need_size;
  }
  store_u32(mem.data() + offset + kBmCapOffset,
            had_used_offset - kBufferManagerHeaderSize);

  BufferManager bm;
  bm.min_slice_size_ = list_size_percent.front().size;
  bm.max_slice_size_ = list_size_percent.back().size;
  bm.lists_ = std::move(free_buffer_lists);
  bm.mem_ = std::move(mem);
  bm.path_ = std::move(path);
  bm.ref_count_.store(1, std::memory_order_seq_cst);
  return bm;
}

Result<BufferManager> BufferManager::mapping(
    std::string path, MemoryMap mem, std::uint32_t buffer_region_start_offset) {
  if (mem.size() <= static_cast<std::size_t>(buffer_region_start_offset) +
                        kBmCapOffset ||
      mem.size() <= static_cast<std::size_t>(buffer_region_start_offset)) {
    char buf[192];
    std::snprintf(buf, sizeof(buf),
                  "mem's size is at least:%u but:%zu "
                  "buffer_region_start_offset:%u",
                  buffer_region_start_offset + kBmCapOffset + 1, mem.size(),
                  buffer_region_start_offset);
    return std::unexpected(Error::other(buf));
  }
  const std::uint16_t list_num =
      load_u16(mem.data() + buffer_region_start_offset);
  const std::uint32_t length =
      load_u32(mem.data() + buffer_region_start_offset + kBmCapOffset);
  if (mem.size() < kBufferManagerHeaderSize + length || list_num == 0) {
    char buf[192];
    std::snprintf(buf, sizeof(buf),
                  "could not mapping buffer manager ,list_num:%u len(mem) at "
                  "least:%u but:%zu",
                  list_num, kBufferManagerHeaderSize + length, mem.size());
    return std::unexpected(Error::other(buf));
  }

  std::uint32_t had_used_offset = kBufferManagerHeaderSize;
  {
    char buf[128];
    std::snprintf(buf, sizeof(buf), "mapping buffer manager, list_num:%u length:%u",
                  list_num, length);
    SHMIPC_INFO(buf);
  }

  std::vector<BufferList> free_lists;
  free_lists.reserve(list_num);
  for (std::uint16_t i = 0; i < list_num; ++i) {
    auto l = BufferList::mapping(mem.data(), mem.size(),
                                 buffer_region_start_offset + had_used_offset);
    if (!l) {
      return std::unexpected(l.error());
    }
    {
      char buf[256];
      std::snprintf(buf, sizeof(buf),
                    "mapping buffer list offset:%u size:%d head:%u tail:%u "
                    "capPerBuffer:%u",
                    buffer_region_start_offset + had_used_offset,
                    l->size_load(), l->head_load(), l->tail_load(),
                    l->cap_per_buffer());
      SHMIPC_INFO(buf);
    }
    const std::uint32_t size =
        count_buffer_list_mem_size(l->cap_load(), l->cap_per_buffer());
    had_used_offset += size;
    free_lists.push_back(std::move(*l));
  }

  BufferManager bm;
  bm.path_ = std::move(path);
  bm.mem_ = std::move(mem);
  bm.min_slice_size_ = free_lists.front().cap_per_buffer();
  bm.max_slice_size_ = free_lists.back().cap_per_buffer();
  bm.lists_ = std::move(free_lists);
  bm.ref_count_.store(1, std::memory_order_seq_cst);
  return bm;
}

// ---------------------------------------------------------------------------
// Global ref-count management
// ---------------------------------------------------------------------------

void add_global_buffer_manager_ref_count(const std::string& path,
                                         std::int32_t c) {
  std::shared_ptr<BufferManager> bm;
  {
    const std::lock_guard<std::mutex> guard(registry_mutex());
    auto it = registry().find(path);
    if (it != registry().end()) {
      bm = std::move(it->second);
      registry().erase(it);
    }
  }
  if (bm != nullptr) {
    const std::int32_t old = bm->ref_count_fetch_add(c);
    if (old + c <= 0) {
      SHMIPC_INFO("clean buffer manager:" + path);
      bm->unmap();
      return;
    }
  }
  if (bm != nullptr) {
    const std::lock_guard<std::mutex> guard(registry_mutex());
    registry().emplace(path, std::move(bm));
  }
}

// ---------------------------------------------------------------------------
// BufferManager methods
// ---------------------------------------------------------------------------

std::uint32_t BufferManager::remain_size() const {
  std::uint32_t result = 0;
  for (const auto& list : lists_) {
    const std::int64_t remain = static_cast<std::int64_t>(list.size_load()) *
                                static_cast<std::int64_t>(list.cap_per_buffer());
    if (remain > 0) {
      result += static_cast<std::uint32_t>(remain);
    }
  }
  return result;
}

Result<BufferSlice> BufferManager::alloc_shm_buffer(std::uint32_t size) const {
  if (size <= max_slice_size_) {
    for (const auto& list : lists_) {
      if (size <= list.cap_per_buffer()) {
        return list.pop();
      }
    }
  }
  return std::unexpected(Err::kNoMoreBuffer);
}

std::int64_t BufferManager::alloc_shm_buffers(SliceList& slices,
                                              std::uint32_t size) const {
  std::int64_t remain = static_cast<std::int64_t>(size);
  std::int64_t alloc_size = 0;
  for (std::int64_t i = static_cast<std::int64_t>(lists_.size()) - 1;
       i >= 0 && remain > 0; --i) {
    while (remain > 0) {
      auto buf = lists_[static_cast<std::size_t>(i)].pop();
      if (!buf) {
        break;
      }
      alloc_size += static_cast<std::int64_t>(buf->cap);
      remain -= static_cast<std::int64_t>(buf->cap);
      slices.push_back(std::move(*buf));
    }
  }
  return alloc_size;
}

void BufferManager::recycle_buffer(BufferSlice slice) const {
  if (slice.is_from_shm) {
    if (!slice.buffer_header.has_value()) {
      char buf[256];
      std::snprintf(buf, sizeof(buf),
                    "skip recycling shm buffer without header: path=%s "
                    "offset=%u cap=%u size=%zu",
                    path_.c_str(), slice.offset_in_shm, slice.cap,
                    slice.size());
      SHMIPC_WARN(buf);
      return;
    }
    if (!slice.buffer_header->is_in_used()) {
      char buf[256];
      std::snprintf(buf, sizeof(buf),
                    "skip recycling shm buffer that is not in use: path=%s "
                    "offset=%u cap=%u size=%zu",
                    path_.c_str(), slice.offset_in_shm, slice.cap,
                    slice.size());
      SHMIPC_WARN(buf);
      return;
    }
    for (const auto& list : lists_) {
      if (slice.cap == list.cap_per_buffer()) {
        list.push(std::move(slice));
        break;
      }
    }
  }
  // Non-shm slices: the heap storage is released by RAII when `slice`
  // goes out of scope (Rust reconstructs the Vec manually here).
}

void BufferManager::recycle_buffers(BufferSlice slice) const {
  if (!slice.is_from_shm) {
    return;  // RAII releases the heap storage.
  }
  while (true) {
    // The header is always present on shm slices (unwrap in Rust).
    if (!slice.buffer_header->has_next()) {
      recycle_buffer(std::move(slice));
      return;
    }
    const std::uint32_t next_slice_offset =
        slice.buffer_header->next_buffer_offset();
    recycle_buffer(std::move(slice));
    auto next = read_buffer_slice(next_slice_offset);
    if (!next) {
      SHMIPC_ERROR(
          "BufferManager recycle_buffers read_buffer_slice failed, err=" +
          next.error().message());
      return;
    }
    slice = std::move(*next);
  }
}

std::int64_t BufferManager::slice_size() const {
  std::int64_t size = 0;
  for (const auto& list : lists_) {
    size += static_cast<std::int64_t>(list.size_load());
  }
  return size;
}

Result<BufferSlice> BufferManager::read_buffer_slice(std::uint32_t offset) const {
  if (static_cast<std::size_t>(offset) + kBufferHeaderSize >= mem_.size()) {
    char buf[160];
    std::snprintf(buf, sizeof(buf),
                  "broken share memory. readBufferSlice unexpect offset:%u "
                  "buffers cap:%zu",
                  offset, mem_.size());
    return std::unexpected(Error::other(buf));
  }
  const std::uint32_t buf_cap = load_u32(mem_.data() + offset + kBufferCapOffset);
  const std::uint32_t buf_end_offset = offset + kBufferHeaderSize + buf_cap;
  if (buf_end_offset > mem_.size()) {
    char buf[224];
    std::snprintf(buf, sizeof(buf),
                  "broken share memory. readBufferSlice unexpect "
                  "buffer_end_offset:%u buffer_start_offset:%u buffers cap:%zu",
                  buf_end_offset, offset, mem_.size());
    return std::unexpected(Error::other(buf));
  }
  return BufferSlice(
      BufferHeader(mem_.data() + offset),
      std::span<std::uint8_t>(mem_.data() + offset + kBufferHeaderSize,
                              buf_end_offset - (offset + kBufferHeaderSize)),
      offset, true);
}

bool BufferManager::check_buffer_returned() const {
  for (const auto& list : lists_) {
    if (list.size_load() != static_cast<std::int32_t>(list.cap_load())) {
      return false;
    }
    if (list.counter_load() != 0) {
      return false;
    }
  }
  return true;
}

void BufferManager::unmap() {
  // Spin 5s to check if all buffers are returned; if timeout, force unmap.
  for (int i = 0; i < 50; ++i) {
    if (check_buffer_returned()) {
      SHMIPC_INFO("all buffer returned before unmap");
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  mem_.unmap();
  if (mem_map_type_ == MemMapType::kDevShmFile) {
    if (::unlink(path_.c_str()) != 0) {
      char buf[512];
      std::snprintf(buf, sizeof(buf),
                    "bufferManager remove file:%s failed, error=%s",
                    path_.c_str(), std::strerror(errno));
      SHMIPC_WARN(buf);
    } else {
      SHMIPC_INFO("bufferManager remove file:" + path_);
    }
  } else {
    if (::close(memfd_) != 0) {
      char buf[256];
      std::snprintf(buf, sizeof(buf),
                    "bufferManager close fd:%d failed, error=%s", memfd_,
                    std::strerror(errno));
      SHMIPC_WARN(buf);
    } else {
      char buf[64];
      std::snprintf(buf, sizeof(buf), "bufferManager close fd:%d", memfd_);
      SHMIPC_INFO(buf);
    }
  }
}

}  // namespace shmipc::buffer
