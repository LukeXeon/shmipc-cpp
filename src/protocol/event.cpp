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

#include "shmipc/protocol/event.hpp"

#include <cstdio>
#include <cstring>

#include "shmipc/consts.hpp"
#include "shmipc/log.hpp"
#include "shmipc/protocol/header.hpp"

namespace shmipc::protocol {

namespace {

inline void store_be_u16(std::uint8_t* p, std::uint16_t v) {
  p[0] = static_cast<std::uint8_t>(v >> 8);
  p[1] = static_cast<std::uint8_t>(v);
}

inline void store_be_u32(std::uint8_t* p, std::uint32_t v) {
  p[0] = static_cast<std::uint8_t>(v >> 24);
  p[1] = static_cast<std::uint8_t>(v >> 16);
  p[2] = static_cast<std::uint8_t>(v >> 8);
  p[3] = static_cast<std::uint8_t>(v);
}

inline std::uint16_t load_be_u16(const std::uint8_t* p) {
  return static_cast<std::uint16_t>((p[0] << 8) | p[1]);
}

inline std::uint32_t load_be_u32(const std::uint8_t* p) {
  return (static_cast<std::uint32_t>(p[0]) << 24) |
         (static_cast<std::uint32_t>(p[1]) << 16) |
         (static_cast<std::uint32_t>(p[2]) << 8) |
         static_cast<std::uint32_t>(p[3]);
}

}  // namespace

// ---------------------------------------------------------------------------
// Header
// ---------------------------------------------------------------------------

std::uint32_t Header::length() const noexcept { return load_be_u32(p_); }

std::uint16_t Header::magic() const noexcept { return load_be_u16(p_ + 4); }

std::uint8_t Header::version() const noexcept { return p_[6]; }

EventType Header::msg_type() const noexcept {
  return static_cast<EventType>(p_[7]);
}

void Header::encode(std::uint32_t length, std::uint8_t version,
                    EventType msg_type) noexcept {
  store_be_u32(p_, length);
  store_be_u16(p_ + 4, kMagicNumber);
  p_[6] = version;
  p_[7] = static_cast<std::uint8_t>(msg_type);
}

std::span<const std::uint8_t> Header::as_slice() const noexcept {
  return {p_, kHeaderSize};
}

std::string Header::to_string() const {
  char buf[160];
  std::snprintf(buf, sizeof(buf),
                "Header { length: %u, magic: %u, version: %u, msg_type: %s }",
                length(), magic(), version(),
                event_type_name(msg_type()).c_str());
  return buf;
}

// ---------------------------------------------------------------------------
// EventType
// ---------------------------------------------------------------------------

std::string event_type_name(EventType t) {
  switch (t) {
    case EventType::kShareMemoryByFilePath: return "ShareMemoryByFilePath";
    case EventType::kPolling: return "Polling";
    case EventType::kStreamClose: return "StreamClose";
    case EventType::kFallbackData: return "FallbackData";
    case EventType::kExchangeProtoVersion: return "ExchangeProtoVersion";
    case EventType::kShareMemoryByMemfd: return "ShareMemoryByMemfd";
    case EventType::kAckShareMemory: return "AckShareMemory";
    case EventType::kAckReadyRecvFd: return "AckReadyRecvFD";
    case EventType::kHotRestart: return "HotRestart";
    case EventType::kHotRestartAck: return "HotRestartAck";
  }
  return "UNSET" + std::to_string(static_cast<int>(t));
}

const std::vector<std::vector<std::uint8_t>>& polling_event_with_version() {
  static const auto* events = [] {
    auto* v = new std::vector<std::vector<std::uint8_t>>();
    v->reserve(kMaxSupportProtoVersion + 1);
    for (std::uint8_t i = 0; i <= kMaxSupportProtoVersion; ++i) {
      std::vector<std::uint8_t> buf(kHeaderSize, 0);
      Header(buf.data()).encode(static_cast<std::uint32_t>(kHeaderSize), i,
                                EventType::kPolling);
      v->push_back(std::move(buf));
    }
    return v;
  }();
  return *events;
}

// ---------------------------------------------------------------------------
// FallbackDataEvent
// ---------------------------------------------------------------------------

void FallbackDataEvent::encode(std::uint32_t length, std::uint8_t version,
                               std::uint32_t seq_id,
                               std::uint32_t status) noexcept {
  store_be_u32(p_, length);
  store_be_u16(p_ + 4, kMagicNumber);
  p_[6] = version;
  p_[7] = static_cast<std::uint8_t>(EventType::kFallbackData);
  store_be_u32(p_ + 8, seq_id);
  store_be_u32(p_ + 12, status);
}

std::span<const std::uint8_t> FallbackDataEvent::as_slice() const noexcept {
  return {p_, kHeaderSize + 8};
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

Result<Unit> check_event_valid(const Header& hdr) {
  // Verify the magic & version
  if (hdr.magic() != kMagicNumber || hdr.version() == 0) {
    char buf[96];
    std::snprintf(buf, sizeof(buf), "shmipc: Invalid magic or version %u %u",
                  hdr.magic(), hdr.version());
    SHMIPC_ERROR(buf);
    return std::unexpected(Err::kInvalidVersion);
  }
  const EventType mt = hdr.msg_type();
  if (mt < kMinEventType || mt > kMaxEventType) {
    SHMIPC_ERROR("shmipc, invalid protocol header: " + hdr.to_string());
    return std::unexpected(Err::kInvalidMsgType);
  }
  return Unit{};
}

}  // namespace shmipc::protocol
