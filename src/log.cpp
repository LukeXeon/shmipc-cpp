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

#include "shmipc/log.hpp"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <string_view>

namespace shmipc::log {

namespace {

std::mutex g_log_mutex;

Level parse_level(const char* value) {
  if (value == nullptr) {
    return Level::kInfo;
  }
  const std::string_view v(value);
  if (v == "off") return Level::kOff;
  if (v == "error") return Level::kError;
  if (v == "warn") return Level::kWarn;
  if (v == "info") return Level::kInfo;
  if (v == "debug") return Level::kDebug;
  if (v == "trace") return Level::kTrace;
  return Level::kInfo;
}

}  // namespace

Level threshold() noexcept {
  static const Level level = parse_level(std::getenv("SHMIPC_LOG"));
  return level;
}

void write(Level level, const std::string& message) {
  static constexpr const char* kNames[] = {"OFF", "ERROR", "WARN",
                                           "INFO", "DEBUG", "TRACE"};
  const std::lock_guard<std::mutex> lock(g_log_mutex);
  std::fprintf(stderr, "%s %s\n", kNames[static_cast<int>(level)],
               message.c_str());
}

}  // namespace shmipc::log
