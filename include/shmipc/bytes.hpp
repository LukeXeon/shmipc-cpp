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
// Minimal equivalent of the `bytes::Bytes` type used by shmipc-rs:
// a cheaply cloneable, immutable, reference-counted byte buffer.

#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <span>
#include <vector>

namespace shmipc {

class Bytes {
 public:
  Bytes() = default;

  explicit Bytes(std::vector<std::uint8_t> data)
      : storage_(std::make_shared<std::vector<std::uint8_t>>(std::move(data))),
        data_(storage_->data()),
        size_(storage_->size()) {}

  // Wrap an existing span by copying it (mirrors Bytes::copy_from_slice).
  static Bytes copy_from(std::span<const std::uint8_t> src) {
    return Bytes(std::vector<std::uint8_t>(src.begin(), src.end()));
  }

  // Take ownership of a vector (zero-copy).
  static Bytes from_vec(std::vector<std::uint8_t> v) {
    return Bytes(std::move(v));
  }

  const std::uint8_t* data() const noexcept { return data_; }
  std::size_t size() const noexcept { return size_; }
  bool empty() const noexcept { return size_ == 0; }

  std::span<const std::uint8_t> span() const noexcept {
    return {data_, size_};
  }

  std::uint8_t operator[](std::size_t i) const noexcept { return data_[i]; }

  friend bool operator==(const Bytes& lhs, const Bytes& rhs) noexcept {
    return lhs.size() == rhs.size() &&
           (lhs.empty() || std::memcmp(lhs.data(), rhs.data(), lhs.size()) == 0);
  }

 private:
  std::shared_ptr<std::vector<std::uint8_t>> storage_;
  const std::uint8_t* data_ = nullptr;
  std::size_t size_ = 0;
};

}  // namespace shmipc
