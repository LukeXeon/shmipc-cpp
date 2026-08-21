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

#include "shmipc/config.hpp"

#include <cstdio>

#include "shmipc/buffer/layout.hpp"

namespace shmipc {

Config::Config() {
  // Mirror DEFAULT_BUFFER_SLICE_SIZES (consts.rs).
  buffer_slice_sizes = {
      {8192 - buffer::kBufferHeaderSize, 50},
      {32 * 1024 - buffer::kBufferHeaderSize, 30},
      {128 * 1024 - buffer::kBufferHeaderSize, 20},
  };
}

Result<Unit> Config::verify() {
  if (share_memory_buffer_cap < (1u << 20)) {
    char buf[128];
    std::snprintf(buf, sizeof(buf),
                  "share memory size is too small:%u, must greater than %u",
                  share_memory_buffer_cap, 1u << 20);
    return std::unexpected(Error::other(buf));
  }
  if (buffer_slice_sizes.empty()) {
    return std::unexpected(Error::other("buffer_slice_sizes could not be nil"));
  }

  std::uint32_t sum = 0;
  for (auto& pair : buffer_slice_sizes) {
    sum += pair.percent;
    if (pair.size > share_memory_buffer_cap) {
      char buf[160];
      std::snprintf(buf, sizeof(buf),
                    "buffer_slice_sizes's size:%u couldn't greater than share_"
                    "memory_buffer_cap:%u",
                    pair.size, share_memory_buffer_cap);
      return std::unexpected(Error::other(buf));
    }

    const std::uint32_t aligned = (pair.size + 3) & ~3u;
    if (aligned != pair.size) {
      pair.size = aligned;
    }
  }

  if (sum != 100) {
    return std::unexpected(
        Error::other("the sum of buffer_slice_sizes's percent should be 100"));
  }

  const std::uint32_t aligned_queue_cap = (queue_cap + 7) & ~7u;
  if (aligned_queue_cap != queue_cap) {
    queue_cap = aligned_queue_cap;
  }

  if (share_memory_path_prefix.empty() || queue_path.empty()) {
    return std::unexpected(
        Error::other("buffer path or queue path could not be nil"));
  }

  // OS/arch checks are enforced at CMake configure time for this port
  // (mirrors the #[cfg] guards in Config::verify).

  return Unit{};
}

}  // namespace shmipc
