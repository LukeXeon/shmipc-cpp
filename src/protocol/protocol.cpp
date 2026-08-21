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

#include "shmipc/protocol/protocol.hpp"

#include <sys/socket.h>
#include <unistd.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>

#include "shmipc/consts.hpp"
#include "shmipc/log.hpp"
#include "shmipc/protocol/adapter.hpp"

namespace shmipc::protocol {

bool protocol_trace_mode() noexcept {
  static const bool enabled = [] {
    const char* v = std::getenv("SHMIPC_PROTOCOL_TRACE");
    return v != nullptr && v[0] != '\0';
  }();
  return enabled;
}

void protocol_trace(const Header& h, std::span<const std::uint8_t> body,
                    bool send) {
  if (!protocol_trace_mode()) {
    return;
  }
  const std::string body_str(body.begin(), body.end());
  if (send) {
    SHMIPC_WARN("send, header:" + h.to_string() + " body:" + body_str);
  } else {
    SHMIPC_WARN("recv, header:" + h.to_string() + " body:" + body_str);
  }
}

namespace {

// Applies SO_RCVTIMEO/SO_SNDTIMEO for the duration of the handshake and
// restores blocking behavior afterwards (the Rust version relies on the
// tokio timeout wrapper around spawn_blocking).
class HandshakeTimeoutGuard {
 public:
  HandshakeTimeoutGuard(int fd, std::chrono::nanoseconds timeout) : fd_(fd) {
    struct timeval tv {};
    const auto secs =
        std::chrono::duration_cast<std::chrono::seconds>(timeout);
    const auto usecs = std::chrono::duration_cast<std::chrono::microseconds>(
                           timeout - secs)
                           .count();
    tv.tv_sec = static_cast<decltype(tv.tv_sec)>(secs.count());
    tv.tv_usec = static_cast<decltype(tv.tv_usec)>(usecs);
    // Ensure a non-zero timeout: zero would disable the timeout entirely.
    if (tv.tv_sec == 0 && tv.tv_usec == 0) {
      tv.tv_usec = 1;
    }
    ::setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    ::setsockopt(fd_, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
  }
  ~HandshakeTimeoutGuard() {
    struct timeval tv {};  // zero -> block indefinitely again
    ::setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    ::setsockopt(fd_, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
  }

 private:
  int fd_;
};

}  // namespace

Result<std::pair<std::shared_ptr<buffer::BufferManager>, QueueManager>>
init_manager(Config& config) {
  const std::string buffer_path =
      config.share_memory_path_prefix + kBufferPathSuffix;
  if (config.mem_map_type == MemMapType::kDevShmFile) {
    auto bm = buffer::BufferManager::get_with_file(
        buffer_path, config.share_memory_buffer_cap, true,
        config.buffer_slice_sizes);
    if (!bm) {
      std::error_code ec;
      std::filesystem::remove_all(buffer_path, ec);
      return std::unexpected(Error::other(
          "create share memory buffer manager failed, error=" +
          bm.error().message()));
    }
    auto qm = QueueManager::create_with_file(config.queue_path,
                                             config.queue_cap);
    if (!qm) {
      std::error_code ec;
      std::filesystem::remove_all(config.queue_path, ec);
      return std::unexpected(Error::other(
          "create share memory queue manager failed, error=" +
          qm.error().message()));
    }
    return std::pair{std::move(*bm), std::move(*qm)};
  }
  auto bm = buffer::BufferManager::get_with_memfd(
      buffer_path, 0, config.share_memory_buffer_cap, true,
      config.buffer_slice_sizes);
  if (!bm) {
    return std::unexpected(Error::other(
        "create share memory buffer manager failed, error=" +
        bm.error().message()));
  }
  auto qm =
      QueueManager::create_with_memfd(config.queue_path, config.queue_cap);
  if (!qm) {
    return std::unexpected(Error::other(
        "create share memory queue manager failed, error=" +
        qm.error().message()));
  }
  return std::pair{std::move(*bm), std::move(*qm)};
}

Result<std::uint8_t> init_client_protocol(std::string buffer_path,
                                          int buffer_memfd,
                                          std::string queue_path,
                                          int queue_memfd, int conn_fd,
                                          MemMapType mem_map_type,
                                          std::chrono::nanoseconds timeout) {
  SHMIPC_INFO("starting initializes shmipc client protocol");
  const HandshakeTimeoutGuard guard(conn_fd, timeout);
  const ClientProtocolAdapter adapter(conn_fd, mem_map_type,
                                      std::move(buffer_path),
                                      std::move(queue_path), buffer_memfd,
                                      queue_memfd);
  auto initializer = adapter.get_initializer();
  if (!initializer) {
    return std::unexpected(initializer.error());
  }
  if (auto r = initializer->init(); !r) {
    return std::unexpected(r.error());
  }
  return initializer->version();
}

Result<std::tuple<std::shared_ptr<buffer::BufferManager>, QueueManager,
                  std::uint8_t>>
init_server_protocol(int conn_fd, std::chrono::nanoseconds timeout) {
  SHMIPC_INFO("starting initializes shmipc server protocol");
  const HandshakeTimeoutGuard guard(conn_fd, timeout);
  const ServerProtocolAdapter adapter(conn_fd);
  auto initializer = adapter.get_initializer();
  if (!initializer) {
    return std::unexpected(initializer.error());
  }
  auto r = initializer->init();
  if (!r) {
    return std::unexpected(r.error());
  }
  if (!r->has_value()) {
    return std::unexpected(
        Error::other("server protocol init returned no managers"));
  }
  return std::tuple{std::move(r->value().buffer_manager),
                    std::move(r->value().queue_manager),
                    initializer->version()};
}

}  // namespace shmipc::protocol
