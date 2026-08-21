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

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "shmipc/protocol/block_io.hpp"

#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstdio>
#include <cstring>

#include "shmipc/consts.hpp"
#include "shmipc/log.hpp"

namespace shmipc::protocol {

Result<Unit> block_read_full(int conn_fd, std::span<std::uint8_t> data) {
  std::size_t read_size = 0;
  while (read_size < data.size()) {
    const ssize_t n =
        ::read(conn_fd, data.data() + read_size, data.size() - read_size);
    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
      char buf[128];
      std::snprintf(buf, sizeof(buf),
                    "read_full failed, had read_size:%zu, reason:%s",
                    read_size, std::strerror(errno));
      return std::unexpected(Error::io(buf));
    }
    if (n == 0) {
      return std::unexpected(Error::io("EOF"));
    }
    read_size += static_cast<std::size_t>(n);
  }
  return Unit{};
}

Result<Unit> block_write_full(int conn_fd,
                              std::span<const std::uint8_t> data) {
  std::size_t written = 0;
  while (written < data.size()) {
    const ssize_t n =
        ::write(conn_fd, data.data() + written, data.size() - written);
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

Result<Unit> send_fd(int conn_fd, std::span<const int> fds) {
  std::uint8_t dummy = 0;
  std::uint8_t payload = 0;
  struct iovec iov {};
  iov.iov_base = &dummy;
  iov.iov_len = 0;

  // Control message storage.
  union {
    char buf[CMSG_SPACE(sizeof(int) * kMemfdCount)];
    struct cmsghdr align;
  } u{};

  struct msghdr msg {};
  msg.msg_iov = &iov;
  msg.msg_iovlen = 1;

  if (!fds.empty()) {
    int sock_type = 0;
    socklen_t optlen = sizeof(sock_type);
    if (::getsockopt(conn_fd, SOL_SOCKET, SO_TYPE, &sock_type, &optlen) != 0) {
      return std::unexpected(Error::from_errno(errno));
    }
    if (sock_type != SOCK_DGRAM) {
      payload = 0;
      iov.iov_base = &payload;
      iov.iov_len = 1;
    }

    msg.msg_control = u.buf;
    msg.msg_controllen = sizeof(u.buf);
    struct cmsghdr* cmsg = CMSG_FIRSTHDR(&msg);
    cmsg->cmsg_level = SOL_SOCKET;
    cmsg->cmsg_type = SCM_RIGHTS;
    cmsg->cmsg_len = CMSG_LEN(sizeof(int) * fds.size());
    std::memcpy(CMSG_DATA(cmsg), fds.data(), sizeof(int) * fds.size());
  }

  const ssize_t n = ::sendmsg(conn_fd, &msg, MSG_NOSIGNAL);
  if (n < 0) {
    return std::unexpected(Error::from_errno(errno));
  }
  return Unit{};
}

Result<std::vector<int>> block_read_out_of_bound_for_fd(int conn_fd) {
  std::uint8_t byte = 0;
  struct iovec iov {};
  iov.iov_base = nullptr;
  iov.iov_len = 0;

  int sock_type = 0;
  socklen_t optlen = sizeof(sock_type);
  if (::getsockopt(conn_fd, SOL_SOCKET, SO_TYPE, &sock_type, &optlen) != 0) {
    return std::unexpected(Error::from_errno(errno));
  }
  if (sock_type != SOCK_DGRAM) {
    iov.iov_base = &byte;
    iov.iov_len = 1;
  }

  union {
    char buf[CMSG_SPACE(sizeof(int) * kMemfdCount)];
    struct cmsghdr align;
  } u{};

  struct msghdr msg {};
  msg.msg_iov = &iov;
  msg.msg_iovlen = 1;
  msg.msg_control = u.buf;
  msg.msg_controllen = sizeof(u.buf);

  const ssize_t n = ::recvmsg(conn_fd, &msg, 0);
  if (n < 0) {
    return std::unexpected(Error::io(
        std::string("try recv fd from peer failed, reason:") +
        std::strerror(errno)));
  }
  SHMIPC_INFO("recvmsg finished");

  for (struct cmsghdr* cmsg = CMSG_FIRSTHDR(&msg); cmsg != nullptr;
       cmsg = CMSG_NXTHDR(&msg, cmsg)) {
    if (cmsg->cmsg_level == SOL_SOCKET && cmsg->cmsg_type == SCM_RIGHTS) {
      const std::size_t count =
          (cmsg->cmsg_len - CMSG_LEN(0)) / sizeof(int);
      std::vector<int> fds(count);
      std::memcpy(fds.data(), CMSG_DATA(cmsg), count * sizeof(int));
      return fds;
    }
  }
  return std::unexpected(Error::other("parse socket control message ret is nil"));
}

}  // namespace shmipc::protocol
