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
// C++ translation of shmipc-rs `src/transport.rs`.
//
// The tokio traits (TransportStream/TransportConnect/TransportListen/
// TransportListener with async fn) become blocking fd-based types in the
// thread-mirrored model:
//  - `TransportStream::into_split()` mirrors tokio's owned halves: the read
//    half keeps the original fd, the write half owns a dup() of it, so each
//    can be closed independently (the socket closes when both are gone).
//  - `connect()`/`listen()`/`accept()` are synchronous.

#pragma once

#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <utility>

#include "shmipc/error.hpp"

namespace shmipc {

// Owned read half (mirrors tokio OwnedReadHalf).
class ReadHalf {
 public:
  explicit ReadHalf(int fd) : fd_(fd) {}
  ~ReadHalf();
  ReadHalf(const ReadHalf&) = delete;
  ReadHalf& operator=(const ReadHalf&) = delete;
  ReadHalf(ReadHalf&& other) noexcept : fd_(other.fd_) { other.fd_ = -1; }

  int fd() const noexcept { return fd_; }

 private:
  int fd_;
};

// Owned write half (mirrors tokio OwnedWriteHalf).
class WriteHalf {
 public:
  explicit WriteHalf(int fd) : fd_(fd) {}
  ~WriteHalf();
  WriteHalf(const WriteHalf&) = delete;
  WriteHalf& operator=(const WriteHalf&) = delete;
  WriteHalf(WriteHalf&& other) noexcept : fd_(other.fd_) { other.fd_ = -1; }

  int fd() const noexcept { return fd_; }

  // Blocking write of the full buffer (write_loop semantics). SIGPIPE is
  // suppressed via MSG_NOSIGNAL.
  Result<Unit> write_all(std::span<const std::uint8_t> data) const;

  // Mirrors dropping the write half (shutdown(SHUT_WR)).
  void shutdown_write() const;

 private:
  int fd_;
};

// Mirror of trait `TransportStream`.
class TransportStream {
 public:
  virtual ~TransportStream() = default;
  virtual int as_raw_fd() const noexcept = 0;
  virtual std::pair<std::unique_ptr<ReadHalf>, std::unique_ptr<WriteHalf>>
  into_split() = 0;
};

// Addresses ----------------------------------------------------------------

struct UnixAddr {
  std::string path;
  static Result<UnixAddr> from_pathname(std::string path);
};

struct TcpAddr {
  std::string host;
  std::uint16_t port = 0;
  // Parses "host:port" (IPv4) or "[host]:port".
  static Result<TcpAddr> parse(const std::string& s);
  std::string to_string() const;
};

// Connected streams ---------------------------------------------------------

class UnixStream : public TransportStream {
 public:
  explicit UnixStream(int fd) : fd_(fd) {}
  ~UnixStream() override;
  UnixStream(const UnixStream&) = delete;
  UnixStream& operator=(const UnixStream&) = delete;
  UnixStream(UnixStream&& other) noexcept : fd_(other.fd_) { other.fd_ = -1; }

  int as_raw_fd() const noexcept override { return fd_; }
  std::pair<std::unique_ptr<ReadHalf>, std::unique_ptr<WriteHalf>>
  into_split() override;

 private:
  int fd_;
};

class TcpStream : public TransportStream {
 public:
  explicit TcpStream(int fd) : fd_(fd) {}
  ~TcpStream() override;
  TcpStream(const TcpStream&) = delete;
  TcpStream& operator=(const TcpStream&) = delete;
  TcpStream(TcpStream&& other) noexcept : fd_(other.fd_) { other.fd_ = -1; }

  int as_raw_fd() const noexcept override { return fd_; }
  std::pair<std::unique_ptr<ReadHalf>, std::unique_ptr<WriteHalf>>
  into_split() override;

 private:
  int fd_;
};

// Listeners ------------------------------------------------------------------

class UnixListener {
 public:
  using StreamT = UnixStream;

  explicit UnixListener(int fd) : fd_(fd) {}
  ~UnixListener();
  UnixListener(const UnixListener&) = delete;
  UnixListener(UnixListener&& other) noexcept : fd_(other.fd_) {
    other.fd_ = -1;
  }

  int fd() const noexcept { return fd_; }
  Result<UnixStream> accept() const;

 private:
  int fd_;
};

class TcpListener {
 public:
  using StreamT = TcpStream;

  explicit TcpListener(int fd) : fd_(fd) {}
  ~TcpListener();
  TcpListener(const TcpListener&) = delete;
  TcpListener(TcpListener&& other) noexcept : fd_(other.fd_) {
    other.fd_ = -1;
  }

  int fd() const noexcept { return fd_; }
  Result<TcpStream> accept() const;

 private:
  int fd_;
};

// Connectors / binders (mirror Default*Connect / Default*Listen) ------------

struct DefaultUnixConnect {
  using Stream = UnixStream;
  using Address = UnixAddr;
  Result<UnixStream> connect(const UnixAddr& addr) const;
};

struct DefaultTcpConnect {
  using Stream = TcpStream;
  using Address = TcpAddr;
  Result<TcpStream> connect(const TcpAddr& addr) const;
};

struct DefaultUnixListen {
  using Listener = UnixListener;
  using Address = UnixAddr;
  Result<UnixListener> listen(const UnixAddr& addr) const;
};

struct DefaultTcpListen {
  using Listener = TcpListener;
  using Address = TcpAddr;
  Result<TcpListener> listen(const TcpAddr& addr) const;
};

}  // namespace shmipc
