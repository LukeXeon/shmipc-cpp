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
// C++ translation of shmipc-rs `src/util/buf_reader.rs` (itself a modified
// copy of tokio's BufReader). The tokio AsyncRead polling is replaced by
// blocking reads on the fd, matching the thread-mirrored concurrency model.

#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

#include "shmipc/error.hpp"

namespace shmipc {

inline constexpr std::size_t kDefaultBufSize = 8 * 1024;

class BufReader {
 public:
  explicit BufReader(int fd, std::size_t capacity = kDefaultBufSize)
      : fd_(fd), buf_(capacity, 0), cap_(capacity) {}

  BufReader(const BufReader&) = delete;
  BufReader& operator=(const BufReader&) = delete;

  int fd() const noexcept { return fd_; }

  // Mirrors `fill_buf_at_least`: returns a view with at least `len` bytes,
  // blocking on the fd until enough data arrived. Error when the peer hits
  // EOF before `len` bytes ("invalid eof", Rust io::ErrorKind::InvalidData).
  Result<std::span<const std::uint8_t>> fill_buf_at_least(std::size_t len);

  // Mirrors `consume`.
  void consume(std::size_t amt) {
    pos_ = pos_ + amt < len_ ? pos_ + amt : len_;
  }

  // Mirrors `buffer()`: currently buffered data, no fd read attempted.
  std::span<const std::uint8_t> buffer() const noexcept {
    return {buf_.data() + pos_, len_ - pos_};
  }

  // Mirrors `compact()`: move remaining data to the front.
  void compact();

  void clear() {
    pos_ = 0;
    len_ = 0;
  }

 private:
  int fd_;
  std::vector<std::uint8_t> buf_;
  std::size_t pos_ = 0;
  std::size_t len_ = 0;  // the current valid index
  std::size_t cap_;
};

}  // namespace shmipc
