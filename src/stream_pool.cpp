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

#include "shmipc/stream_pool.hpp"

#include "shmipc/log.hpp"

namespace shmipc {

Result<Unit> StreamPool::push(Stream stream) {
  {
    const std::lock_guard<std::mutex> lock(mtx_);
    if (streams_.size() < capacity_) {
      streams_.push_back(std::move(stream));
      return Unit{};
    }
  }
  stream.safe_close_notify();
  if (auto r = stream.close(); !r) {
    SHMIPC_WARN("stream pool close rejected stream: " + r.error().message());
  }
  return std::unexpected(Err::kStreamPoolFull);
}

std::optional<Stream> StreamPool::pop() {
  const std::lock_guard<std::mutex> lock(mtx_);
  if (streams_.empty()) {
    return std::nullopt;
  }
  Stream s = std::move(streams_.front());
  streams_.pop_front();
  return s;
}

void StreamPool::close() {
  while (auto s = pop()) {
    s->safe_close_notify();
    (void)s->close();
  }
}

}  // namespace shmipc
