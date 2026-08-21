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
// Minimal stand-in for the `tracing` crate used by shmipc-rs. Level is
// controlled by the SHMIPC_LOG environment variable: "off", "error", "warn",
// "info" (default), "debug", "trace".

#pragma once

#include <string>

namespace shmipc::log {

enum class Level { kOff = 0, kError, kWarn, kInfo, kDebug, kTrace };

// Reads SHMIPC_LOG once; defaults to info.
Level threshold() noexcept;

inline bool enabled(Level level) noexcept { return level <= threshold(); }

// Thread-safe line output: "<level> <message>\n" to stderr.
void write(Level level, const std::string& message);

}  // namespace shmipc::log

#define SHMIPC_LOG_IMPL(lvl, expr)                                     \
  do {                                                                 \
    if (::shmipc::log::enabled(lvl)) {                                 \
      ::shmipc::log::write((lvl), (expr));                             \
    }                                                                  \
  } while (0)

#define SHMIPC_ERROR(expr) SHMIPC_LOG_IMPL(::shmipc::log::Level::kError, (expr))
#define SHMIPC_WARN(expr) SHMIPC_LOG_IMPL(::shmipc::log::Level::kWarn, (expr))
#define SHMIPC_INFO(expr) SHMIPC_LOG_IMPL(::shmipc::log::Level::kInfo, (expr))
#define SHMIPC_DEBUG(expr) SHMIPC_LOG_IMPL(::shmipc::log::Level::kDebug, (expr))
#define SHMIPC_TRACE(expr) SHMIPC_LOG_IMPL(::shmipc::log::Level::kTrace, (expr))
