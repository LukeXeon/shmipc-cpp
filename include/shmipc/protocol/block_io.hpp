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
// C++ translation of shmipc-rs `src/protocol/block_io.rs`.

#pragma once

#include <cstdint>
#include <span>
#include <vector>

#include "shmipc/error.hpp"

namespace shmipc::protocol {

Result<Unit> block_read_full(int conn_fd, std::span<std::uint8_t> data);

Result<Unit> block_write_full(int conn_fd, std::span<const std::uint8_t> data);

// Send file descriptors over a unix socket using SCM_RIGHTS.
Result<Unit> send_fd(int conn_fd, std::span<const int> fds);

// Receive file descriptors (memfds) out-of-band from the peer.
Result<std::vector<int>> block_read_out_of_bound_for_fd(int conn_fd);

}  // namespace shmipc::protocol
