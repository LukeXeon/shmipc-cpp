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
// C++ translation of shmipc-rs `src/protocol/header.rs`.
//
// Header is a non-owning view over an 8-byte big-endian frame header:
// length u32 | magic u16 | version u8 | msg_type u8.

#pragma once

#include <cstdint>
#include <span>
#include <string>

#include "shmipc/protocol/event.hpp"

namespace shmipc::protocol {

class Header {
 public:
  Header() = default;
  explicit Header(std::uint8_t* p) noexcept : p_(p) {}

  std::uint8_t* ptr() const noexcept { return p_; }
  explicit operator bool() const noexcept { return p_ != nullptr; }

  std::uint32_t length() const noexcept;
  std::uint16_t magic() const noexcept;
  std::uint8_t version() const noexcept;
  EventType msg_type() const noexcept;

  void encode(std::uint32_t length, std::uint8_t version,
              EventType msg_type) noexcept;

  std::span<const std::uint8_t> as_slice() const noexcept;

  // Display equivalent.
  std::string to_string() const;

 private:
  std::uint8_t* p_ = nullptr;
};

}  // namespace shmipc::protocol
