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
// C++ translation of shmipc-rs `src/config.rs`.

#pragma once

#include <chrono>
#include <cstdint>
#include <string>
#include <vector>

#include "shmipc/error.hpp"

namespace shmipc {

enum class MemMapType : std::uint8_t {
  kDevShmFile = 0,
  kMemFd = 1,  // default, mirrors Rust `#[default]`
};

// Mirror of Rust `StateType` (consts.rs).
enum class StateType : std::uint32_t {
  kDefault = 0,
  kHotRestart,
  kHotRestartDone,
};

struct SizePercentPair {
  std::uint32_t size = 0;
  std::uint32_t percent = 0;

  friend bool operator==(const SizePercentPair&, const SizePercentPair&) =
      default;
};

// Config is used to tune the shmipc session.
struct Config {
  // connection_write_timeout is meant to be a "safety value" timeout after
  // which we will suspect a problem with the underlying connection and close
  // it. This is only applied to writes, where there's generally an
  // expectation that things will move along quickly.
  std::chrono::nanoseconds connection_write_timeout = std::chrono::seconds(10);

  std::chrono::nanoseconds connection_read_timeout{};  // 0 == None
  std::chrono::nanoseconds connection_timeout{};       // 0 == None

  // Timeout during server and client exchange config phase.
  std::chrono::nanoseconds initialize_timeout = std::chrono::milliseconds(1000);

  // The max number of pending requests.
  std::uint32_t queue_cap = 8192;

  // Share memory path of the underlying queue.
  std::string queue_path = "/dev/shm/shmipc_queue";

  // The capacity of buffer in share memory.
  std::uint32_t share_memory_buffer_cap = 32 * 1024 * 1024;

  // The share memory path prefix of buffer.
  std::string share_memory_path_prefix = "/dev/shm/shmipc";

  // Guess request or response's size for improving performance.
  // Default mirrors DEFAULT_BUFFER_SLICE_SIZES in consts.rs.
  std::vector<SizePercentPair> buffer_slice_sizes;

  // mmap map type, kDevShmFile or kMemFd (server set).
  MemMapType mem_map_type = MemMapType::kMemFd;

  // Client rebuild session interval.
  std::chrono::nanoseconds rebuild_interval = std::chrono::seconds(60);

  std::size_t max_stream_num = 4096;

  Config();  // fills buffer_slice_sizes with the defaults

  // Mirror of `Config::verify()`: validates and aligns slice sizes (4-byte)
  // and queue_cap (8-byte). Mutates on purpose, like the Rust version.
  Result<Unit> verify();
};

}  // namespace shmipc
