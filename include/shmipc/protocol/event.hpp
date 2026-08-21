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
// C++ translation of shmipc-rs `src/protocol/event.rs`.

#pragma once

#include <cstdint>
#include <span>
#include <string>
#include <vector>

#include "shmipc/error.hpp"

namespace shmipc::protocol {

class Header;  // forward decl; full definition in protocol/header.hpp

// EventType for internal implementations. Mirrors the Rust newtype
// EventType(u8); unknown values are preserved.
enum class EventType : std::uint8_t {
  kShareMemoryByFilePath = 0,
  // notify peer start consume
  kPolling = 1,
  // stream level event notify peer stream close
  kStreamClose = 2,
  kFallbackData = 3,
  // exchange proto version
  kExchangeProtoVersion = 4,
  // query the mem map type supported by the server
  kShareMemoryByMemfd = 5,
  // when server mapping share memory success, give the ack to client
  kAckShareMemory = 6,
  kAckReadyRecvFd = 7,
  kHotRestart = 8,
  kHotRestartAck = 9,
};

inline constexpr EventType kMinEventType = EventType::kShareMemoryByFilePath;
inline constexpr EventType kMaxEventType = EventType::kHotRestartAck;

std::string event_type_name(EventType t);

// One pre-encoded polling event header per protocol version
// (POLLING_EVENT_WITH_VERSION in Rust).
const std::vector<std::vector<std::uint8_t>>& polling_event_with_version();

// header | seq_id | status
class FallbackDataEvent {
 public:
  explicit FallbackDataEvent(std::uint8_t* p) noexcept : p_(p) {}

  void encode(std::uint32_t length, std::uint8_t version, std::uint32_t seq_id,
              std::uint32_t status) noexcept;

  std::span<const std::uint8_t> as_slice() const noexcept;

 private:
  std::uint8_t* p_;
};

Result<Unit> check_event_valid(const Header& hdr);

}  // namespace shmipc::protocol
