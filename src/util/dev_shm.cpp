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

#include "shmipc/util/dev_shm.hpp"

#include <sys/statvfs.h>

#include "shmipc/log.hpp"

namespace shmipc {

bool can_create_on_dev_shm(std::uint64_t size, const std::string& path) {
  if (path.find("/dev/shm") != std::string::npos) {
    struct statvfs st {};
    if (::statvfs("/dev/shm", &st) != 0) {
      SHMIPC_WARN(
          "could not read /dev/shm free size, can_create_on_dev_shm default "
          "return false");
      return false;
    }
    // fs2::free_space computes f_bavail * f_frsize.
    const std::uint64_t free_bytes =
        static_cast<std::uint64_t>(st.f_bavail) *
        static_cast<std::uint64_t>(st.f_frsize);
    return free_bytes >= size;
  }
  return true;
}

}  // namespace shmipc
