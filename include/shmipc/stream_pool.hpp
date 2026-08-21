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
// C++ translation of shmipc-rs `src/session/pool.rs`.

#pragma once

#include <cstddef>
#include <deque>
#include <mutex>
#include <optional>

#include "shmipc/error.hpp"
#include "shmipc/stream.hpp"

namespace shmipc {

class StreamPool {
 public:
  explicit StreamPool(std::size_t pool_capacity)
      : capacity_(pool_capacity) {}

  StreamPool(const StreamPool&) = delete;
  StreamPool& operator=(const StreamPool&) = delete;

  Result<Unit> push(Stream stream);
  std::optional<Stream> pop();
  void close();

 private:
  mutable std::mutex mtx_;
  std::deque<Stream> streams_;
  std::size_t capacity_;
};

}  // namespace shmipc
