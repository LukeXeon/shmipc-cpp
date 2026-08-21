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
// C++ translation of shmipc-rs `src/session/config.rs`.

#pragma once

#include <chrono>
#include <cstddef>

#include "shmipc/config.hpp"

namespace shmipc {

class SessionManagerConfig {
 public:
  SessionManagerConfig() = default;

  const Config& config() const noexcept { return config_; }
  Config& config_mut() noexcept { return config_; }

  SessionManagerConfig& with_config(Config config) {
    config_ = std::move(config);
    return *this;
  }

  std::size_t session_num() const noexcept { return session_num_; }
  SessionManagerConfig& with_session_num(std::size_t session_num) {
    session_num_ = session_num;
    return *this;
  }

  std::chrono::nanoseconds stream_max_idle_time() const noexcept {
    return stream_max_idle_time_;
  }
  SessionManagerConfig& with_stream_max_idle_time(
      std::chrono::nanoseconds stream_max_idle_time) {
    stream_max_idle_time_ = stream_max_idle_time;
    return *this;
  }

 private:
  Config config_;
  std::size_t session_num_ = 1;
  std::chrono::nanoseconds stream_max_idle_time_ = std::chrono::seconds(30);
};

}  // namespace shmipc
