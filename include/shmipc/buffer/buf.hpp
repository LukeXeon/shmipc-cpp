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
// C++ translation of shmipc-rs `src/buffer/buf.rs` (`enum Buf<'shm>`).
//
// Shm variant: a non-owning view into shared memory, valid until the owning
// LinkedBuffer recycles it (same lifetime contract as the Rust borrow).
// Exm variant: owned immutable bytes (bytes::Bytes equivalent).

#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>
#include <variant>

#include "shmipc/bytes.hpp"

namespace shmipc::buffer {

class Buf {
 public:
  Buf() : data_(Bytes{}) {}

  static Buf shm(std::span<const std::uint8_t> view) {
    Buf b;
    b.data_ = view;
    return b;
  }

  static Buf exm(Bytes bytes) {
    Buf b;
    b.data_ = std::move(bytes);
    return b;
  }

  bool is_shm() const noexcept {
    return std::holds_alternative<std::span<const std::uint8_t>>(data_);
  }
  bool is_exm() const noexcept { return !is_shm(); }

  std::size_t size() const noexcept {
    if (is_shm()) {
      return std::get<std::span<const std::uint8_t>>(data_).size();
    }
    return std::get<Bytes>(data_).size() - offset_;
  }
  bool empty() const noexcept { return size() == 0; }

  std::span<const std::uint8_t> span() const noexcept {
    if (is_shm()) {
      return std::get<std::span<const std::uint8_t>>(data_);
    }
    return std::get<Bytes>(data_).span().subspan(offset_);
  }

  const std::uint8_t* data() const noexcept { return span().data(); }

  // Mirror of the `bytes::Buf` trait implementation.
  std::size_t remaining() const noexcept { return size(); }
  std::span<const std::uint8_t> chunk() const noexcept { return span(); }
  void advance(std::size_t cnt) {
    if (is_shm()) {
      auto& s = std::get<std::span<const std::uint8_t>>(data_);
      s = s.subspan(cnt);
    } else {
      offset_ += cnt;
    }
  }

  friend bool operator==(const Buf& lhs, const Buf& rhs) noexcept {
    const auto a = lhs.span();
    const auto b = rhs.span();
    return a.size() == b.size() &&
           (a.empty() || std::memcmp(a.data(), b.data(), a.size()) == 0);
  }

 private:
  std::variant<std::span<const std::uint8_t>, Bytes> data_;
  std::size_t offset_ = 0;  // advance cursor for the owned variant
};

}  // namespace shmipc::buffer
