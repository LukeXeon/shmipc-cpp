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
// C++ translation of shmipc-rs `src/protocol/mod.rs`.
//
// The tokio timeout/spawn_blocking wrappers around the blocking handshake
// are replaced by socket-level timeouts (SO_RCVTIMEO/SO_SNDTIMEO) applied
// for the duration of the handshake, matching the thread-mirrored model.

#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <tuple>

#include "shmipc/buffer/manager.hpp"
#include "shmipc/config.hpp"
#include "shmipc/protocol/header.hpp"
#include "shmipc/queue.hpp"

namespace shmipc::protocol {

// Mirrors PROTOCOL_TRACE_MODE (env SHMIPC_PROTOCOL_TRACE).
bool protocol_trace_mode() noexcept;

void protocol_trace(const Header& h, std::span<const std::uint8_t> body,
                    bool send);

// Create the shared-memory buffer manager + queue manager according to the
// config (client side). Mutates config.buffer_slice_sizes like the Rust
// version (sizes are aligned in BufferManager::create).
Result<std::pair<std::shared_ptr<buffer::BufferManager>, QueueManager>>
init_manager(Config& config);

// Run the client-side handshake on the connection fd. Returns the
// negotiated protocol version.
Result<std::uint8_t> init_client_protocol(std::string buffer_path,
                                          int buffer_memfd,
                                          std::string queue_path,
                                          int queue_memfd, int conn_fd,
                                          MemMapType mem_map_type,
                                          std::chrono::nanoseconds timeout);

// Run the server-side handshake on the connection fd. Returns the buffer
// manager, queue manager and negotiated protocol version.
Result<std::tuple<std::shared_ptr<buffer::BufferManager>, QueueManager,
                  std::uint8_t>>
init_server_protocol(int conn_fd, std::chrono::nanoseconds timeout);

}  // namespace shmipc::protocol
