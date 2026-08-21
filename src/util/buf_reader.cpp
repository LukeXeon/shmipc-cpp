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

#include "shmipc/util/buf_reader.hpp"

#include <unistd.h>

#include <cassert>
#include <cerrno>
#include <cstring>

namespace shmipc {

Result<std::span<const std::uint8_t>> BufReader::fill_buf_at_least(
    std::size_t len) {
  if (len == 0) {
    return std::span<const std::uint8_t>{};
  }

  if ((len_ - pos_) >= len) {
    return std::span<const std::uint8_t>(buf_.data() + pos_, len_ - pos_);
  }

  assert(len <= cap_);
  // If the requested length is larger than the free space after pos, compact.
  if (len > (cap_ - pos_)) {
    compact();
  }

  while ((len_ - pos_) < len) {
    const ssize_t size = ::read(fd_, buf_.data() + len_, cap_ - len_);
    if (size < 0) {
      if (errno == EINTR) {
        continue;
      }
      return std::unexpected(Error::from_errno(errno));
    }
    if (size == 0) {
      return std::unexpected(Error::io("invalid eof"));
    }
    len_ += static_cast<std::size_t>(size);
  }
  return std::span<const std::uint8_t>(buf_.data() + pos_, len_ - pos_);
}

void BufReader::compact() {
  if (len_ == pos_) {
    pos_ = 0;
    len_ = 0;
    return;
  }

  const std::size_t remaining = len_ - pos_;
  std::memmove(buf_.data(), buf_.data() + pos_, remaining);
  pos_ = 0;
  len_ = remaining;
}

}  // namespace shmipc
