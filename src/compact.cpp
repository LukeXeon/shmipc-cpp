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

#include "shmipc/compact.hpp"

#include <utility>

namespace shmipc {

IoResult<std::size_t> StreamExt::read(std::span<std::uint8_t> buf) {
  if (buf.empty()) {
    return std::size_t{0};
  }
  while (true) {
    if (reader_.has_value()) {
      std::size_t filled = 0;
      if (reader_->read(buf, filled)) {
        reader_.reset();
      }
      return filled;
    }
    auto data = inner_.read();
    if (!data) {
      return std::unexpected(to_io_error_code(data.error()));
    }
    reader_.emplace(std::move(*data));
    // loop to consume from the freshly acquired buffer
  }
}

IoResult<std::size_t> StreamExt::write(std::span<const std::uint8_t> buf) {
  auto n = inner_.write_bytes(buf);
  if (!n) {
    return std::unexpected(to_io_error_code(n.error()));
  }
  return *n;
}

IoResult<Unit> StreamExt::flush() {
  auto r = inner_.flush(true);
  if (!r) {
    if (r.error() == Err::kStreamClosed) {
      return Unit{};
    }
    return std::unexpected(to_io_error_code(r.error()));
  }
  return Unit{};
}

IoResult<std::size_t> StreamExt::read_exact(std::span<std::uint8_t> buf) {
  std::size_t filled = 0;
  while (filled < buf.size()) {
    auto n = read(buf.subspan(filled));
    if (!n) {
      return std::unexpected(n.error());
    }
    if (*n == 0) {
      return std::unexpected(
          std::make_error_code(std::errc::broken_pipe));  // EOF
    }
    filled += *n;
  }
  return filled;
}

IoResult<Unit> StreamExt::write_all(std::span<const std::uint8_t> buf) {
  std::size_t written = 0;
  while (written < buf.size()) {
    auto n = write(buf.subspan(written));
    if (!n) {
      return std::unexpected(n.error());
    }
    written += *n;
  }
  return Unit{};
}

IoResult<Unit> StreamExt::shutdown() {
  // Finish any pending flush first (poll_shutdown drives the flush future
  // to completion in the Rust version; here flush is synchronous).
  if (auto r = flush(); !r) {
    return r;
  }
  auto r = inner_.close();
  if (!r) {
    return std::unexpected(to_io_error_code(r.error()));
  }
  return Unit{};
}

}  // namespace shmipc
