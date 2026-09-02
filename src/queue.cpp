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

#include "shmipc/queue.hpp"

#include "shmipc/memfd_compat.hpp"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <system_error>

#include "shmipc/consts.hpp"
#include "shmipc/log.hpp"
#include "shmipc/util/dev_shm.hpp"

namespace shmipc {

namespace {

// The queue stores its 64-bit head/tail counters at offsets that are not
// 8-byte aligned (historical format), so access them via memcpy to stay
// strictly conforming; compilers lower this to a single mov on x86_64.
std::int64_t load_unaligned_i64(const void* p) {
  std::int64_t v;
  std::memcpy(&v, p, sizeof(v));
  return v;
}

void store_unaligned_i64(void* p, std::int64_t v) {
  std::memcpy(p, &v, sizeof(v));
}

void store_u32_at(std::uint8_t* base, std::size_t offset, std::uint32_t v) {
  std::uint32_t tmp = v;  // native endian, mirrors the Rust raw stores
  std::memcpy(base + offset, &tmp, sizeof(tmp));
}

std::uint32_t load_u32_at(const std::uint8_t* base, std::size_t offset) {
  std::uint32_t tmp;
  std::memcpy(&tmp, base + offset, sizeof(tmp));
  return tmp;
}

Result<MemoryMap> mmap_fd(int fd, std::size_t len) {
  void* ptr = ::mmap(nullptr, len, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  if (ptr == MAP_FAILED) {
    return std::unexpected(Error::from_errno(errno));
  }
  return MemoryMap{ptr, len};
}

}  // namespace

std::size_t count_queue_mem_size(std::uint32_t queue_cap) {
  return kQueueHeaderLength + kQueueElementLen * static_cast<std::size_t>(queue_cap);
}

// ---------------------------------------------------------------------------
// Queue
// ---------------------------------------------------------------------------

Queue Queue::create_from_bytes(std::uint8_t* data, std::uint32_t cap) {
  store_u32_at(data, 0, cap);
  Queue q = mapping_from_bytes(data);
  // Due to the previous shmipc specification, the head and tail of the queue
  // are not aligned to an 8 byte boundary.
  q.store_head_unaligned(0);
  q.store_tail_unaligned(0);
  q.working_flag_->store(0, std::memory_order_seq_cst);
  return q;
}

Queue Queue::mapping_from_bytes(std::uint8_t* data) {
  Queue q;
  q.cap_ = static_cast<std::int64_t>(load_u32_at(data, 0));
  const std::size_t queue_start_offset = kQueueHeaderLength;
  const std::size_t queue_end_offset =
      kQueueHeaderLength +
      kQueueElementLen * static_cast<std::size_t>(q.cap_);
#if defined(__aarch64__)
  // a queueManager has two queues; a queue's head and tail should align to an
  // 8 byte boundary on aarch64.
  q.working_flag_ =
      reinterpret_cast<std::atomic<std::uint32_t>*>(data + 4);
  q.head_ = reinterpret_cast<std::int64_t*>(data + 8);
  q.tail_ = reinterpret_cast<std::int64_t*>(data + 16);
#else
  // TODO: unaligned head and tail (x86_64 layout).
  q.working_flag_ =
      reinterpret_cast<std::atomic<std::uint32_t>*>(data + 20);
  q.head_ = reinterpret_cast<std::int64_t*>(data + 4);
  q.tail_ = reinterpret_cast<std::int64_t*>(data + 12);
#endif
  q.queue_bytes_on_memory_ = data + queue_start_offset;
  q.len_ = queue_end_offset - queue_start_offset;
  return q;
}

std::int64_t Queue::load_head_unaligned() const {
  return load_unaligned_i64(head_);
}

std::int64_t Queue::load_tail_unaligned() const {
  return load_unaligned_i64(tail_);
}

void Queue::store_head_unaligned(std::int64_t v) const {
  store_unaligned_i64(head_, v);
}

void Queue::store_tail_unaligned(std::int64_t v) const {
  store_unaligned_i64(tail_, v);
}

Result<Unit> Queue::put(const QueueElement& element) const {
  const std::lock_guard<std::mutex> guard(*lock_);
  if (load_tail_unaligned() - load_head_unaligned() >= cap_) {
    return std::unexpected(Err::kQueueFull);
  }
  const std::size_t queue_offset = static_cast<std::size_t>(
      (load_tail_unaligned() % cap_) * static_cast<std::int64_t>(kQueueElementLen));
  auto* base = const_cast<std::uint8_t*>(queue_bytes_on_memory_);
  store_u32_at(base, queue_offset, element.seq_id);
  store_u32_at(base, queue_offset + 4, element.offset_in_shm_buf);
  store_u32_at(base, queue_offset + 8, element.status);
  store_tail_unaligned(load_tail_unaligned() + 1);
  return Unit{};
}

Result<QueueElement> Queue::pop() const {
  if (load_head_unaligned() >= load_tail_unaligned()) {
    return std::unexpected(Err::kQueueEmpty);
  }
  const std::size_t queue_offset = static_cast<std::size_t>(
      (load_head_unaligned() % cap_) * static_cast<std::int64_t>(kQueueElementLen));
  QueueElement element;
  element.seq_id = load_u32_at(queue_bytes_on_memory_, queue_offset);
  element.offset_in_shm_buf = load_u32_at(queue_bytes_on_memory_, queue_offset + 4);
  element.status = load_u32_at(queue_bytes_on_memory_, queue_offset + 8);
  store_head_unaligned(load_head_unaligned() + 1);
  return element;
}

bool Queue::consumer_is_working() const {
  return working_flag_->load(std::memory_order_seq_cst) > 0;
}

bool Queue::mark_working() const {
  std::uint32_t expected = 0;
  return working_flag_->compare_exchange_strong(
      expected, 1, std::memory_order_seq_cst, std::memory_order_seq_cst);
}

bool Queue::mark_not_working() const {
  working_flag_->store(0, std::memory_order_seq_cst);
  if (size() == 0) {
    return true;
  }
  working_flag_->store(1, std::memory_order_seq_cst);
  return false;
}

// ---------------------------------------------------------------------------
// MemoryMap
// ---------------------------------------------------------------------------

void MemoryMap::unmap() {
  if (ptr_ != nullptr) {
    ::munmap(ptr_, len_);
    ptr_ = nullptr;
    len_ = 0;
  }
}

// ---------------------------------------------------------------------------
// QueueManager
// ---------------------------------------------------------------------------

QueueManager::~QueueManager() = default;

Result<QueueManager> QueueManager::create_with_memfd(
    const std::string& queue_path, std::uint32_t queue_cap) {
  const std::string name = "shmipc" + queue_path;
  const int memfd = memfd_create(name.c_str(), 0);
  if (memfd < 0) {
    return std::unexpected(Error::from_errno(errno));
  }

  const std::size_t mem_size = count_queue_mem_size(queue_cap) * kQueueCount;
  if (::ftruncate(memfd, static_cast<off_t>(mem_size)) != 0) {
    const int err = errno;
    ::close(memfd);
    return std::unexpected(Error::other(
        "create_queue_manager_with_memfd truncate share memory failed: " +
        std::string(std::strerror(err))));
  }

  auto mem = mmap_fd(memfd, mem_size);
  if (!mem) {
    ::close(memfd);
    return std::unexpected(mem.error());
  }
  std::memset(mem->data(), 0, mem_size);

  QueueManager qm;
  qm.path_ = queue_path;
  qm.send_queue = Queue::create_from_bytes(mem->data(), queue_cap);
  qm.recv_queue =
      Queue::create_from_bytes(mem->data() + mem_size / 2, queue_cap);
  qm.mem_ = std::move(*mem);
  qm.map_type_ = MemMapType::kMemFd;
  qm.memfd_ = memfd;
  return qm;
}

Result<QueueManager> QueueManager::create_with_file(const std::string& shm_path,
                                                    std::uint32_t queue_cap) {
  // ignore mkdir error
  const std::filesystem::path path(shm_path);
  std::error_code ec;
  std::filesystem::create_directories(
      path.has_parent_path() ? path.parent_path() : std::filesystem::path("/"),
      ec);
  std::filesystem::permissions(shm_path, std::filesystem::perms::owner_all |
                                              std::filesystem::perms::group_all |
                                              std::filesystem::perms::others_all,
                               ec);
  if (std::filesystem::exists(shm_path)) {
    return std::unexpected(Error::other("queue was existed, path:" + shm_path));
  }

  const std::size_t mem_size = count_queue_mem_size(queue_cap) * kQueueCount;
  if (!can_create_on_dev_shm(mem_size, shm_path)) {
    char buf[320];
    std::snprintf(buf, sizeof(buf),
                  "err: share memory had not left space, path:%s, size:%zu",
                  shm_path.c_str(), mem_size);
    return std::unexpected(Error::other(buf));
  }

  const int fd = ::open(shm_path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0777);
  if (fd < 0) {
    return std::unexpected(Error::from_errno(errno));
  }
  if (::fchmod(fd, 0777) != 0) {
    const int err = errno;
    ::close(fd);
    return std::unexpected(Error::from_errno(err));
  }
  if (::ftruncate(fd, static_cast<off_t>(mem_size)) != 0) {
    const int err = errno;
    ::close(fd);
    return std::unexpected(Error::from_errno(err));
  }

  auto mem = mmap_fd(fd, mem_size);
  ::close(fd);  // the mapping keeps the file alive
  if (!mem) {
    return std::unexpected(mem.error());
  }
  std::memset(mem->data(), 0, mem_size);

  QueueManager qm;
  qm.path_ = shm_path;
  qm.send_queue = Queue::create_from_bytes(mem->data(), queue_cap);
  qm.recv_queue =
      Queue::create_from_bytes(mem->data() + mem_size / 2, queue_cap);
  qm.mem_ = std::move(*mem);
  qm.map_type_ = MemMapType::kDevShmFile;
  qm.memfd_ = 0;
  return qm;
}

Result<QueueManager> QueueManager::mapping_with_memfd(
    const std::string& queue_path, int memfd) {
  struct stat st {};
  if (::fstat(memfd, &st) != 0) {
    return std::unexpected(Error::from_errno(errno));
  }
  const std::size_t mapping_size = static_cast<std::size_t>(st.st_size);
#if defined(__aarch64__)
  // a queueManager has two queues; a queue's head and tail should align to an
  // 8 byte boundary
  if (mapping_size % 16 != 0) {
    return std::unexpected(
        Error::other("the memory size of queue should be a multiple of 16"));
  }
#endif

  auto mem = mmap_fd(memfd, mapping_size);
  if (!mem) {
    return std::unexpected(mem.error());
  }

  QueueManager qm;
  qm.path_ = queue_path;
  // The peer's receive half is our send half, and vice versa.
  qm.send_queue = Queue::mapping_from_bytes(mem->data() + mapping_size / 2);
  qm.recv_queue = Queue::mapping_from_bytes(mem->data());
  qm.mem_ = std::move(*mem);
  qm.map_type_ = MemMapType::kMemFd;
  qm.memfd_ = memfd;
  return qm;
}

Result<QueueManager> QueueManager::mapping_with_file(
    const std::string& shm_path) {
  const int fd = ::open(shm_path.c_str(), O_RDWR);
  if (fd < 0) {
    return std::unexpected(Error::from_errno(errno));
  }
  if (::fchmod(fd, 0777) != 0) {
    const int err = errno;
    ::close(fd);
    return std::unexpected(Error::from_errno(err));
  }
  struct stat st {};
  if (::fstat(fd, &st) != 0) {
    const int err = errno;
    ::close(fd);
    return std::unexpected(Error::from_errno(err));
  }
  const std::size_t mapping_size = static_cast<std::size_t>(st.st_size);
#if defined(__aarch64__)
  if (mapping_size % 16 != 0) {
    ::close(fd);
    return std::unexpected(
        Error::other("the memory size of queue should be a multiple of 16"));
  }
#endif

  auto mem = mmap_fd(fd, mapping_size);
  ::close(fd);
  if (!mem) {
    return std::unexpected(mem.error());
  }

  QueueManager qm;
  qm.path_ = shm_path;
  qm.send_queue = Queue::mapping_from_bytes(mem->data() + mapping_size / 2);
  qm.recv_queue = Queue::mapping_from_bytes(mem->data());
  qm.mem_ = std::move(*mem);
  qm.map_type_ = MemMapType::kDevShmFile;
  qm.memfd_ = 0;
  return qm;
}

void QueueManager::unmap() {
  if (map_type_ == MemMapType::kDevShmFile) {
    if (::unlink(path_.c_str()) != 0) {
      char buf[512];
      std::snprintf(buf, sizeof(buf), "queueManager remove file:%s failed, error=%s",
                    path_.c_str(), std::strerror(errno));
      SHMIPC_WARN(buf);
    } else {
      SHMIPC_INFO("queueManager remove file:" + path_);
    }
  } else {
    if (::close(memfd_) != 0) {
      char buf[256];
      std::snprintf(buf, sizeof(buf), "queueManager close fd:%d failed, error=%s",
                    memfd_, std::strerror(errno));
      SHMIPC_WARN(buf);
    } else {
      char buf[64];
      std::snprintf(buf, sizeof(buf), "queueManager close fd:%d", memfd_);
      SHMIPC_INFO(buf);
    }
  }
}

}  // namespace shmipc
