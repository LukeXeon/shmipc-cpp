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
// Thread-mirrored equivalent of the tokio mpsc channels used by shmipc-rs.
//
// Semantics mirrored from tokio:
//  - bounded/unbounded MPMC queue protected by a mutex + condition variables;
//  - `stop()` closes the channel: pending/future `push` fail, `pop` drains
//    remaining items then returns nullopt (tokio recv returns None once all
//    senders are dropped and the queue is empty; stopping is the explicit
//    shutdown path used by Session::close);
//  - `push_timeout` mirrors `tokio::time::timeout(.., send(..))`.

#pragma once

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>

namespace shmipc {

template <typename T>
class Channel {
 public:
  // cap == 0 means unbounded.
  explicit Channel(std::size_t cap) : cap_(cap) {}

  Channel(const Channel&) = delete;
  Channel& operator=(const Channel&) = delete;

  bool push(T value) {
    std::unique_lock<std::mutex> lock(mtx_);
    not_full_.wait(lock, [this] { return stopped_ || !is_full(); });
    if (stopped_) {
      return false;
    }
    queue_.push_back(std::move(value));
    not_empty_.notify_one();
    return true;
  }

  bool push_timeout(T value, std::chrono::nanoseconds timeout) {
    std::unique_lock<std::mutex> lock(mtx_);
    if (!not_full_.wait_for(lock, timeout,
                            [this] { return stopped_ || !is_full(); })) {
      return false;
    }
    if (stopped_) {
      return false;
    }
    queue_.push_back(std::move(value));
    not_empty_.notify_one();
    return true;
  }

  std::optional<T> pop() {
    std::unique_lock<std::mutex> lock(mtx_);
    not_empty_.wait(lock, [this] { return stopped_ || !queue_.empty(); });
    if (queue_.empty()) {
      return std::nullopt;
    }
    T value = std::move(queue_.front());
    queue_.pop_front();
    not_full_.notify_one();
    return value;
  }

  // Close the channel and wake all blocked senders/receivers.
  void stop() {
    {
      const std::lock_guard<std::mutex> lock(mtx_);
      stopped_ = true;
    }
    not_empty_.notify_all();
    not_full_.notify_all();
  }

  bool stopped() const {
    const std::lock_guard<std::mutex> lock(mtx_);
    return stopped_;
  }

 private:
  bool is_full() const {
    return cap_ != 0 && queue_.size() >= cap_;
  }

  mutable std::mutex mtx_;
  std::condition_variable not_full_;
  std::condition_variable not_empty_;
  std::deque<T> queue_;
  std::size_t cap_;
  bool stopped_ = false;
};

template <typename T>
class Sender {
 public:
  Sender() = default;
  explicit Sender(std::shared_ptr<Channel<T>> channel)
      : channel_(std::move(channel)) {}

  bool send(T value) const { return channel_->push(std::move(value)); }
  bool send_timeout(T value, std::chrono::nanoseconds timeout) const {
    return channel_->push_timeout(std::move(value), timeout);
  }
  explicit operator bool() const noexcept { return channel_ != nullptr; }

 private:
  std::shared_ptr<Channel<T>> channel_;
};

template <typename T>
class Receiver {
 public:
  Receiver() = default;
  explicit Receiver(std::shared_ptr<Channel<T>> channel)
      : channel_(std::move(channel)) {}

  std::optional<T> recv() const { return channel_->pop(); }
  void stop() const {
    if (channel_) {
      channel_->stop();
    }
  }
  explicit operator bool() const noexcept { return channel_ != nullptr; }

 private:
  std::shared_ptr<Channel<T>> channel_;
};

template <typename T>
std::pair<Sender<T>, Receiver<T>> make_channel(std::size_t cap) {
  auto channel = std::make_shared<Channel<T>>(cap);
  return {Sender<T>(channel), Receiver<T>(channel)};
}

}  // namespace shmipc
