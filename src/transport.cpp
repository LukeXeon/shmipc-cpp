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

#include "shmipc/transport.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

namespace shmipc {

namespace {

std::pair<std::unique_ptr<ReadHalf>, std::unique_ptr<WriteHalf>> split_fd(
    int fd) {
  const int dup_fd = ::dup(fd);
  if (dup_fd < 0) {
    return {nullptr, nullptr};
  }
  return {std::make_unique<ReadHalf>(fd), std::make_unique<WriteHalf>(dup_fd)};
}

}  // namespace

ReadHalf::~ReadHalf() {
  if (fd_ >= 0) {
    ::close(fd_);
  }
}

WriteHalf::~WriteHalf() {
  if (fd_ >= 0) {
    ::close(fd_);
  }
}

Result<Unit> WriteHalf::write_all(std::span<const std::uint8_t> data) const {
  std::size_t written = 0;
  while (written < data.size()) {
    const ssize_t n = ::send(fd_, data.data() + written,
                             data.size() - written, MSG_NOSIGNAL);
    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
      return std::unexpected(Error::from_errno(errno));
    }
    written += static_cast<std::size_t>(n);
  }
  return Unit{};
}

void WriteHalf::shutdown_write() const {
  if (fd_ >= 0) {
    ::shutdown(fd_, SHUT_WR);
  }
}

// ---------------------------------------------------------------------------
// Addresses
// ---------------------------------------------------------------------------

Result<UnixAddr> UnixAddr::from_pathname(std::string path) {
  if (path.empty()) {
    return std::unexpected(Error::io("empty unix socket path"));
  }
  if (path.size() >= sizeof(sockaddr_un{}.sun_path)) {
    return std::unexpected(Error::io("unix socket path too long"));
  }
  return UnixAddr{std::move(path)};
}

Result<TcpAddr> TcpAddr::parse(const std::string& s) {
  const auto colon = s.rfind(':');
  if (colon == std::string::npos || colon + 1 >= s.size()) {
    return std::unexpected(Error::io("invalid tcp address: " + s));
  }
  std::string host = s.substr(0, colon);
  if (host.size() >= 2 && host.front() == '[' && host.back() == ']') {
    host = host.substr(1, host.size() - 2);
  }
  int port = 0;
  try {
    port = std::stoi(s.substr(colon + 1));
  } catch (...) {
    return std::unexpected(Error::io("invalid tcp port in: " + s));
  }
  if (port < 0 || port > 65535) {
    return std::unexpected(Error::io("tcp port out of range in: " + s));
  }
  return TcpAddr{host, static_cast<std::uint16_t>(port)};
}

std::string TcpAddr::to_string() const {
  return host + ":" + std::to_string(port);
}

// ---------------------------------------------------------------------------
// Streams / listeners
// ---------------------------------------------------------------------------

UnixStream::~UnixStream() {
  if (fd_ >= 0) {
    ::close(fd_);
  }
}

std::pair<std::unique_ptr<ReadHalf>, std::unique_ptr<WriteHalf>>
UnixStream::into_split() {
  auto halves = split_fd(fd_);
  fd_ = -1;  // ownership moved into the halves
  return halves;
}

TcpStream::~TcpStream() {
  if (fd_ >= 0) {
    ::close(fd_);
  }
}

std::pair<std::unique_ptr<ReadHalf>, std::unique_ptr<WriteHalf>>
TcpStream::into_split() {
  auto halves = split_fd(fd_);
  fd_ = -1;
  return halves;
}

UnixListener::~UnixListener() {
  if (fd_ >= 0) {
    ::close(fd_);
  }
}

Result<UnixStream> UnixListener::accept() const {
  const int fd = ::accept(fd_, nullptr, nullptr);
  if (fd < 0) {
    return std::unexpected(Error::from_errno(errno));
  }
  return UnixStream(fd);
}

TcpListener::~TcpListener() {
  if (fd_ >= 0) {
    ::close(fd_);
  }
}

Result<TcpStream> TcpListener::accept() const {
  const int fd = ::accept(fd_, nullptr, nullptr);
  if (fd < 0) {
    return std::unexpected(Error::from_errno(errno));
  }
  return TcpStream(fd);
}

// ---------------------------------------------------------------------------
// Connect / listen
// ---------------------------------------------------------------------------

Result<UnixStream> DefaultUnixConnect::connect(const UnixAddr& addr) const {
  const int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
  if (fd < 0) {
    return std::unexpected(Error::from_errno(errno));
  }
  sockaddr_un sa{};
  sa.sun_family = AF_UNIX;
  std::strncpy(sa.sun_path, addr.path.c_str(), sizeof(sa.sun_path) - 1);
  if (::connect(fd, reinterpret_cast<sockaddr*>(&sa), sizeof(sa)) != 0) {
    const int err = errno;
    ::close(fd);
    return std::unexpected(Error::from_errno(err));
  }
  return UnixStream(fd);
}

Result<TcpStream> DefaultTcpConnect::connect(const TcpAddr& addr) const {
  const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    return std::unexpected(Error::from_errno(errno));
  }
  sockaddr_in sa{};
  sa.sin_family = AF_INET;
  sa.sin_port = htons(addr.port);
  if (::inet_pton(AF_INET, addr.host.c_str(), &sa.sin_addr) != 1) {
    // Try "localhost" style names minimally.
    if (addr.host == "localhost") {
      sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    } else {
      ::close(fd);
      return std::unexpected(Error::io("invalid ipv4 host: " + addr.host));
    }
  }
  if (::connect(fd, reinterpret_cast<sockaddr*>(&sa), sizeof(sa)) != 0) {
    const int err = errno;
    ::close(fd);
    return std::unexpected(Error::from_errno(err));
  }
  int one = 1;
  ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
  return TcpStream(fd);
}

Result<UnixListener> DefaultUnixListen::listen(const UnixAddr& addr) const {
  const int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
  if (fd < 0) {
    return std::unexpected(Error::from_errno(errno));
  }
  // Mirrors DefaultUnixListen: unlink any stale socket path first.
  ::unlink(addr.path.c_str());
  sockaddr_un sa{};
  sa.sun_family = AF_UNIX;
  std::strncpy(sa.sun_path, addr.path.c_str(), sizeof(sa.sun_path) - 1);
  if (::bind(fd, reinterpret_cast<sockaddr*>(&sa), sizeof(sa)) != 0 ||
      ::listen(fd, 512) != 0) {
    const int err = errno;
    ::close(fd);
    return std::unexpected(Error::from_errno(err));
  }
  return UnixListener(fd);
}

Result<TcpListener> DefaultTcpListen::listen(const TcpAddr& addr) const {
  const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    return std::unexpected(Error::from_errno(errno));
  }
  int one = 1;
  ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
  sockaddr_in sa{};
  sa.sin_family = AF_INET;
  sa.sin_port = htons(addr.port);
  if (addr.host.empty() || addr.host == "0.0.0.0") {
    sa.sin_addr.s_addr = htonl(INADDR_ANY);
  } else if (::inet_pton(AF_INET, addr.host.c_str(), &sa.sin_addr) != 1) {
    ::close(fd);
    return std::unexpected(Error::io("invalid ipv4 host: " + addr.host));
  }
  if (::bind(fd, reinterpret_cast<sockaddr*>(&sa), sizeof(sa)) != 0 ||
      ::listen(fd, 512) != 0) {
    const int err = errno;
    ::close(fd);
    return std::unexpected(Error::from_errno(err));
  }
  return TcpListener(fd);
}

}  // namespace shmipc
