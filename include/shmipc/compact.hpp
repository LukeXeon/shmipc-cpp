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
// C++ translation of shmipc-rs `src/compact.rs` (StreamExt).
//
// The Rust type adapts Stream to tokio's AsyncRead/AsyncWrite; with the
// blocking stream API the adapter becomes a small synchronous facade with
// the same read/write/flush/shutdown semantics (read returns whatever the
// current shm buffer holds, flush maps StreamClosed to success, shutdown
// flushes then closes).

#pragma once

#include <cstddef>
#include <optional>
#include <span>

#include "shmipc/error.hpp"
#include "shmipc/stream.hpp"
#include "shmipc/util/shmbuf_reader.hpp"

namespace shmipc {

class StreamExt {
 public:
  explicit StreamExt(Stream inner) : inner_(std::move(inner)) {}

  const Stream& inner() const noexcept { return inner_; }
  Stream& inner_mut() noexcept { return inner_; }
  Stream into_inner() && { return std::move(inner_); }

  // AsyncRead::poll_read equivalent: fills `buf` from the current shm
  // buffer; returns the number of bytes written.
  IoResult<std::size_t> read(std::span<std::uint8_t> buf);

  // AsyncWrite::poll_write equivalent.
  IoResult<std::size_t> write(std::span<const std::uint8_t> buf);

  // AsyncWrite::poll_flush equivalent (flush with end_stream=true);
  // StreamClosed is reported as success like the Rust version.
  IoResult<Unit> flush();

  // AsyncWrite::poll_shutdown equivalent: finish a pending flush, then
  // close the stream.
  IoResult<Unit> shutdown();

  // AsyncReadExt::read_exact equivalent.
  IoResult<std::size_t> read_exact(std::span<std::uint8_t> buf);

  // AsyncWriteExt::write_all equivalent.
  IoResult<Unit> write_all(std::span<const std::uint8_t> buf);

 private:
  Stream inner_;
  std::optional<ShmBufReader> reader_;
};

}  // namespace shmipc
