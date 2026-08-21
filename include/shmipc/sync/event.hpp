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
// Manual-reset event used where shmipc-rs waits on `tokio::sync::Notify`
// for shutdown signaling (a persistent flag fits the shutdown use case: no
// wake-ups may be lost between the notifier and a late waiter).

#pragma once

#include <chrono>
#include <condition_variable>
#include <mutex>

namespace shmipc {

class Event {
 public:
  void set() {
    {
      const std::lock_guard<std::mutex> lock(mtx_);
      set_ = true;
    }
    cv_.notify_all();
  }

  bool is_set() const {
    const std::lock_guard<std::mutex> lock(mtx_);
    return set_;
  }

  void wait() const {
    std::unique_lock<std::mutex> lock(mtx_);
    cv_.wait(lock, [this] { return set_; });
  }

  // Returns true when the event is set, false on timeout.
  template <typename Rep, typename Period>
  bool wait_for(const std::chrono::duration<Rep, Period>& timeout) const {
    std::unique_lock<std::mutex> lock(mtx_);
    return cv_.wait_for(lock, timeout, [this] { return set_; });
  }

 private:
  mutable std::mutex mtx_;
  mutable std::condition_variable cv_;
  bool set_ = false;
};

}  // namespace shmipc
