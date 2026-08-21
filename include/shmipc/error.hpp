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
// C++ translation of shmipc-rs `src/error.rs`.

#pragma once

#include <cstddef>
#include <expected>
#include <string>
#include <system_error>

namespace shmipc {

// Mirror of Rust `enum Error` (error.rs). `kIo` carries an errno-derived
// message; `kOther` carries a free-form message (Rust `anyhow::Error`).
// `kStreamHasUnreadData` / `kStreamHasPendingData` carry a size payload.
enum class Err {
  kInvalidVersion,  // received a frame with an invalid version
  kInvalidMsgType,  // received a frame with an invalid message type
  kSessionShutdown, // there is a shutdown during an operation
  kStreamsExhausted, // no more stream ids to issue
  kDuplicateStream, // duplicate stream is opened inbound
  kTimeout,         // reached an IO deadline
  kStreamClosed,    // using a closed stream
  kStreamReset,     // the peer reset the stream
  kConnectionWriteTimeout, // "safety valve" timeout writing to the underlying
                           // stream connection
  kConnectionTimeout,
  kKeepAliveTimeout,   // a missed keepalive caused the stream close
  kEndOfStream,        // the stream is ended, user shouldn't read from it
  kSessionUnhealthy,   // the session is overloaded; retry after 60 seconds
  kNotEnoughData,      // real read size < expected read size
  kNoMoreBuffer,       // the share memory is busy, no more buffer to allocate
  kSizeTooLarge,       // the allocated size exceeded
  kBrokenBuffer,       // the share memory's layout had broken
  kShareMemoryHadNotLeftSpace,
  kStreamCallbackHadExisted,
  kExchangeConfig,        // message type error during exchange config phase
  kExchangeConfigTimeout, // client exchange config timeout
  kOSNonSupported,        // shmipc just support linux OS now
  kArchNonSupported,      // shmipc just support amd64 or arm64 arch
  kHotRestartInProgress,  // ensure once hot restart succeed
  kInHandshakeStage,      // session in handshake stage, try again later
  kFileNameTooLong,       // file name max len 255
  kQueueEmpty,
  kQueueFull,
  kStreamPoolFull,
  kStreamHasUnreadData,  // payload: unread byte size
  kStreamHasPendingData, // payload: pending slice count
  kIo,
  kOther,
};

// Returns the same message text as Rust's `thiserror` Display impls.
const char* err_message(Err kind) noexcept;

class Error {
 public:
  Error() = default;

  // Non-explicit on purpose: allows `return Err::kQueueFull;` where
  // `Result<T>` (std::expected) is expected, mirroring Rust `From` impls.
  Error(Err kind) : kind_(kind) {}  // NOLINT(google-explicit-constructor)

  Error(Err kind, std::string message)
      : kind_(kind), message_(std::move(message)) {}

  static Error with_size(Err kind, std::size_t payload) {
    Error e(kind);
    e.size_payload_ = payload;
    return e;
  }

  static Error from_errno(int errnum);              // kind kIo
  static Error io(std::string message);             // kind kIo
  static Error other(std::string message);          // kind kOther

  Err kind() const noexcept { return kind_; }

  // For kStreamHasUnreadData / kStreamHasPendingData.
  std::size_t size_payload() const noexcept { return size_payload_; }

  // Display equivalent of the Rust error.
  std::string message() const;

  bool is(Err kind) const noexcept { return kind_ == kind; }

  friend bool operator==(const Error& lhs, const Error& rhs) noexcept {
    return lhs.kind_ == rhs.kind_;
  }
  friend bool operator==(const Error& lhs, Err rhs) noexcept {
    return lhs.kind_ == rhs;
  }
  friend bool operator==(Err lhs, const Error& rhs) noexcept {
    return rhs.kind_ == lhs;
  }

 private:
  Err kind_ = Err::kOther;
  std::size_t size_payload_ = 0;
  std::string message_; // only used by kIo / kOther
};

// Mirror of Rust `From<Error> for io::Error`: maps the shmipc error onto the
// closest std::errc so StreamExt-style IO facades can surface it.
std::error_code to_io_error_code(const Error& err);

// Result aliases. Rust `Result<T, E>` -> std::expected. C++23 std::expected
// does not allow `void` as the value type, so Unit stands in for `()`.
struct Unit {
  friend constexpr bool operator==(Unit, Unit) noexcept { return true; }
};

template <typename T>
using Result = std::expected<T, Error>;

template <typename T>
using IoResult = std::expected<T, std::error_code>;

}  // namespace shmipc
