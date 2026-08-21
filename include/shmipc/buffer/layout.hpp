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
// Shared-memory layout constants from shmipc-rs `src/buffer/manager.rs`,
// extracted into a leaf header so that `consts.hpp`, `slice.hpp`, `list.hpp`
// and `manager.hpp` can all use them without a dependency cycle.

#pragma once

#include <cstdint>

namespace shmipc::buffer {

// BufferHeader layout: cap 4 | size 4 | start 4 | next 4 | flag 4
inline constexpr std::uint32_t kBufferHeaderSize = 4 + 4 + 4 + 4 + 4;
inline constexpr std::uint32_t kBufferCapOffset = 0;
inline constexpr std::uint32_t kBufferSizeOffset = kBufferCapOffset + 4;
inline constexpr std::uint32_t kBufferDataStartOffset = kBufferSizeOffset + 4;
inline constexpr std::uint32_t kNextBufferOffset = kBufferDataStartOffset + 4;
inline constexpr std::uint32_t kBufferFlagOffset = kNextBufferOffset + 4;

inline constexpr std::uint32_t kBufferManagerHeaderSize = 8;
inline constexpr std::uint32_t kBmCapOffset = 4;

inline constexpr std::uint8_t kHasNextBufferFlag = 1 << 0;
inline constexpr std::uint8_t kSliceInUsedFlag = 1 << 1;

}  // namespace shmipc::buffer
