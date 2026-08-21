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
// C++ translation of shmipc-rs `src/util/shmbuf_reader.rs`. The tokio
// ReadBuf is replaced by a plain span + out parameter.

#pragma once

#include <algorithm>
#include <cstddef>
#include <span>

#include "shmipc/buffer/buf.hpp"

namespace shmipc {

class ShmBufReader {
 public:
  explicit ShmBufReader(buffer::Buf buf) : buf_(std::move(buf)) {}

  // Fills `out` with as much unconsumed data as fits. Returns true when all
  // data is consumed (mirrors the Rust return value).
  bool read(std::span<std::uint8_t> out, std::size_t& filled) {
    filled = 0;
    if (consumed_ >= buf_.size()) {
      return true;
    }
    const auto inner = buf_.span().subspan(consumed_);
    if (inner.size() <= out.size()) {
      std::copy(inner.begin(), inner.end(), out.begin());
      filled = inner.size();
      consumed_ += inner.size();
      return true;
    }
    const std::size_t read_len = out.size();
    std::copy(inner.begin(), inner.begin() + read_len, out.begin());
    filled = read_len;
    consumed_ += read_len;
    return false;
  }

  std::size_t consumed() const noexcept { return consumed_; }

 private:
  buffer::Buf buf_;
  std::size_t consumed_ = 0;
};

}  // namespace shmipc
