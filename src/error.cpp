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

#include "shmipc/error.hpp"

#include <cerrno>
#include <cstdio>
#include <cstring>

namespace shmipc {

const char* err_message(Err kind) noexcept {
  switch (kind) {
    case Err::kInvalidVersion: return "invalid protocol version";
    case Err::kInvalidMsgType: return "invalid msg type";
    case Err::kSessionShutdown: return "session shutdown";
    case Err::kStreamsExhausted: return "streams exhausted";
    case Err::kDuplicateStream: return "duplicate stream initiated";
    case Err::kTimeout: return "i/o deadline reached";
    case Err::kStreamClosed: return "stream closed";
    case Err::kStreamReset: return "stream reset by peer";
    case Err::kConnectionWriteTimeout: return "connection write timeout";
    case Err::kConnectionTimeout: return "connection timeout";
    case Err::kKeepAliveTimeout: return "keepalive timeout";
    case Err::kEndOfStream: return "end of stream";
    case Err::kSessionUnhealthy:
      return "now the session is unhealthy, please retry later";
    case Err::kNotEnoughData:
      return "current buffer is not enough data to read";
    case Err::kNoMoreBuffer: return "share memory no more buffer";
    case Err::kSizeTooLarge: return "alloc size exceed";
    case Err::kBrokenBuffer: return "share memory's buffer had broken";
    case Err::kShareMemoryHadNotLeftSpace:
      return "share memory had not left space";
    case Err::kStreamCallbackHadExisted: return "stream callbacks had existed";
    case Err::kExchangeConfig: return "exchange config protocol invalid";
    case Err::kExchangeConfigTimeout: return "exchange config timeout";
    case Err::kOSNonSupported: return "shmipc just support linux OS now";
    case Err::kArchNonSupported:
      return "shmipc just support amd64 or arm64 arch";
    case Err::kHotRestartInProgress:
      return "hot restart in progress, try again later";
    case Err::kInHandshakeStage:
      return "session in handshake stage, try again later";
    case Err::kFileNameTooLong: return "share memory path prefix too long";
    case Err::kQueueEmpty: return "the queue is empty";
    case Err::kQueueFull: return "the queue is full";
    case Err::kStreamPoolFull: return "stream pool is full";
    case Err::kStreamHasUnreadData: return "stream had unread data";
    case Err::kStreamHasPendingData: return "stream had pending data";
    case Err::kIo: return "io error";
    case Err::kOther: return "other error";
  }
  return "unknown error";
}

Error Error::from_errno(int errnum) {
  char buf[256] = {0};
  // GNU strerror_r returns char*.
  const char* msg = strerror_r(errnum, buf, sizeof(buf));
  return Error(Err::kIo, std::string(msg ? msg : "unknown errno"));
}

Error Error::io(std::string message) {
  return Error(Err::kIo, std::move(message));
}

Error Error::other(std::string message) {
  return Error(Err::kOther, std::move(message));
}

std::string Error::message() const {
  switch (kind_) {
    case Err::kStreamHasUnreadData: {
      char buf[64];
      std::snprintf(buf, sizeof(buf), "stream had unread data, size: %zu",
                    size_payload_);
      return buf;
    }
    case Err::kStreamHasPendingData: {
      char buf[64];
      std::snprintf(buf, sizeof(buf),
                    "stream had pending data, pending slice len: %zu",
                    size_payload_);
      return buf;
    }
    case Err::kIo:
    case Err::kOther:
      return message_.empty() ? err_message(kind_) : message_;
    default:
      return err_message(kind_);
  }
}

std::error_code to_io_error_code(const Error& err) {
  // Mirrors `impl From<Error> for io::Error` in error.rs, mapped onto the
  // closest std::errc values.
  switch (err.kind()) {
    case Err::kInvalidVersion:
    case Err::kInvalidMsgType:
    case Err::kConnectionWriteTimeout:
      return std::make_error_code(std::errc::invalid_argument);
    case Err::kSessionShutdown:
    case Err::kStreamReset:
      return std::make_error_code(std::errc::connection_reset);
    case Err::kStreamsExhausted:
    case Err::kSessionUnhealthy:
    case Err::kNotEnoughData:
    case Err::kNoMoreBuffer:
    case Err::kShareMemoryHadNotLeftSpace:
    case Err::kQueueEmpty:
    case Err::kQueueFull:
    case Err::kStreamPoolFull:
    case Err::kHotRestartInProgress:
    case Err::kInHandshakeStage:
      return std::make_error_code(std::errc::resource_unavailable_try_again);
    case Err::kDuplicateStream:
    case Err::kStreamCallbackHadExisted:
      return std::make_error_code(std::errc::already_connected);
    case Err::kTimeout:
    case Err::kConnectionTimeout:
    case Err::kKeepAliveTimeout:
    case Err::kExchangeConfigTimeout:
      return std::make_error_code(std::errc::timed_out);
    case Err::kStreamClosed:
      return std::make_error_code(std::errc::connection_aborted);
    case Err::kEndOfStream:
      return std::make_error_code(std::errc::broken_pipe);
    case Err::kSizeTooLarge:
      return std::make_error_code(std::errc::not_enough_memory);
    case Err::kBrokenBuffer:
      return std::make_error_code(std::errc::broken_pipe);
    case Err::kOSNonSupported:
    case Err::kArchNonSupported:
      return std::make_error_code(std::errc::not_supported);
    case Err::kFileNameTooLong:
      return std::make_error_code(std::errc::invalid_argument);
    case Err::kIo:
      return std::make_error_code(std::errc::io_error);
    default:
      return std::make_error_code(std::errc::io_error);
  }
}

}  // namespace shmipc
