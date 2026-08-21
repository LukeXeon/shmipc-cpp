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

#include "shmipc/listener.hpp"

#include <sys/socket.h>

#include <mutex>
#include <utility>

namespace shmipc {

Listener::~Listener() {
  if (accept_thread_.joinable()) {
    close();
  }
}

IoResult<Stream> Listener::accept() {
  auto res = stream_rx_.recv();
  if (!res.has_value()) {
    return std::unexpected(
        std::make_error_code(std::errc::connection_aborted));
  }
  return std::move(*res);
}

void Listener::close() {
  std::vector<Session> sessions;
  if (sessions_mtx_ != nullptr) {
    const std::lock_guard<std::mutex> lock(*sessions_mtx_);
    sessions = *sessions_;
  }
  for (auto& session : sessions) {
    session.close();
  }
  if (listener_fd_ >= 0) {
    // Unblock the accept loop (mirrors aborting the tokio accept task).
    ::shutdown(listener_fd_, SHUT_RDWR);
    listener_fd_ = -1;
  }
  if (accept_thread_.joinable()) {
    accept_thread_.join();
  }
  stream_rx_.stop();
}

}  // namespace shmipc
