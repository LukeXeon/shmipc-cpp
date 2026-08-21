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
// C++ translation of shmipc-rs `src/consts.rs`. MemMapType / StateType /
// SizePercentPair live in config.hpp (Rust declares them across consts.rs and
// config.rs with a module cycle; C++ keeps them in one leaf header).

#pragma once

#include <chrono>
#include <cstdint>

namespace shmipc {

inline constexpr std::uint8_t kProtoVersion = 2;
inline constexpr std::uint8_t kMaxSupportProtoVersion = 3;
inline constexpr std::uint16_t kMagicNumber = 0x7758;

inline constexpr const char* kMemfdCreateName = "shmipc";
inline constexpr const char* kBufferPathSuffix = "_buffer";

inline constexpr std::size_t kMemfdDataLen = 4;
inline constexpr std::size_t kMemfdCount = 2;

inline constexpr const char* kUnixNetwork = "unix";

inline constexpr std::chrono::nanoseconds kHotRestartCheckTimeout =
    std::chrono::seconds(2);
inline constexpr std::chrono::nanoseconds kHotRestartCheckInterval =
    std::chrono::milliseconds(100);

inline constexpr std::chrono::nanoseconds kSessionRebuildInterval =
    std::chrono::seconds(60);

inline constexpr std::size_t kEpochIdLen = 8;

// linux file name max length
inline constexpr std::size_t kFileNameMaxLen = 255;
// The buffer path will concatenate epoch information and end with
// `_epoch_{epochId uint64}_{randId uint64}`: 1+5+1+20+1+20
inline constexpr std::size_t kEpochInfoMaxLen = 7 + 20 + 1 + 20;
// `_queue_{sessionId int}`
inline constexpr std::size_t kQueueInfoMaxLen = 7 + 20;

inline constexpr std::uint32_t kDefaultQueueCap = 8192;
inline constexpr const char* kDefaultQueuePath = "/dev/shm/shmipc_queue";
inline constexpr std::uint32_t kDefaultShareMemoryCap = 32 * 1024 * 1024;
inline constexpr std::int64_t kDefaultSingleBufferSize = 4096;

inline constexpr std::size_t kQueueElementLen = 12;
inline constexpr std::size_t kQueueCount = 2;

inline constexpr std::size_t kSizeOfLength = 4;
inline constexpr std::size_t kSizeOfMagic = 2;
inline constexpr std::size_t kSizeOfVersion = 1;
inline constexpr std::size_t kSizeOfType = 1;

inline constexpr std::size_t kHeaderSize =
    kSizeOfLength + kSizeOfMagic + kSizeOfVersion + kSizeOfType;

}  // namespace shmipc
