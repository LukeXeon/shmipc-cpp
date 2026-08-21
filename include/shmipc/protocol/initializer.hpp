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
// C++ translation of shmipc-rs `src/protocol/initializer/{mod,v2,v3}.rs`.

#pragma once

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <variant>

#include "shmipc/buffer/manager.hpp"
#include "shmipc/config.hpp"
#include "shmipc/protocol/header.hpp"
#include "shmipc/queue.hpp"

namespace shmipc::protocol {

// What a successful server-side initialization yields (Rust:
// `Option<(Arc<BufferManager>, QueueManager)>`; clients get nullopt).
struct Managers {
  std::shared_ptr<buffer::BufferManager> buffer_manager;
  QueueManager queue_manager;
};

using InitResult = Result<std::optional<Managers>>;

// --- V2 ------------------------------------------------------------------

struct V2Client {
  int conn_fd = -1;
  std::string buffer_path;
  std::string queue_path;

  InitResult init() const;
  static constexpr std::uint8_t version() { return 2; }
};

// `first_event_storage` owns the bytes of the first received event header;
// the Rust version leaks them via mem::forget (see adapter.rs), this port
// keeps them owned.
struct V2Server {
  int conn_fd = -1;
  std::vector<std::uint8_t> first_event_storage;
  Header first_event;

  InitResult init() const;
  static constexpr std::uint8_t version() { return 2; }
};

// --- V3 ------------------------------------------------------------------

struct V3Client {
  int conn_fd = -1;
  MemMapType mem_map_type = MemMapType::kMemFd;
  std::string buffer_path;
  std::string queue_path;
  int buffer_fd = -1;
  int queue_fd = -1;

  InitResult init() const;
  static constexpr std::uint8_t version() { return 3; }
};

struct V3Server {
  int conn_fd = -1;
  std::vector<std::uint8_t> first_event_storage;
  Header first_event;

  InitResult init() const;
  static constexpr std::uint8_t version() { return 3; }
};

// Mirrors `enum ProtocolInitializer`.
class ProtocolInitializer {
 public:
  ProtocolInitializer() = default;
  explicit ProtocolInitializer(V2Client c) : inner_(std::move(c)) {}
  explicit ProtocolInitializer(V2Server s) : inner_(std::move(s)) {}
  explicit ProtocolInitializer(V3Client c) : inner_(std::move(c)) {}
  explicit ProtocolInitializer(V3Server s) : inner_(std::move(s)) {}

  InitResult init() const;
  std::uint8_t version() const;

 private:
  std::variant<V2Client, V2Server, V3Client, V3Server> inner_;
};

// --- Shared handshake helpers ---------------------------------------------

Result<Unit> handle_exchange_version(int conn_fd);

InitResult handle_share_memory_by_memfd(int conn_fd, const Header& h,
                                        std::uint8_t version);

Result<Unit> send_memfd_to_peer(int conn_fd, const std::string& buffer_path,
                                int buffer_fd, const std::string& queue_path,
                                int queue_fd, std::uint8_t version);

Result<Header> wait_event_header(int conn_fd, EventType expect_event_type,
                                 std::span<std::uint8_t> buf);

Result<Header> block_read_event_header(int conn_fd,
                                       std::span<std::uint8_t> buf);

InitResult handle_share_memory_by_file_path(int conn_fd, const Header& hdr);

Result<Unit> send_share_memory_by_file_path(int conn_fd,
                                            const std::string& buffer_path,
                                            const std::string& queue_path,
                                            std::uint8_t version);

// Metadata body codec (Rust: extract_shm_metadata / generate_shm_metadata).
// Body layout: queue_path_len u16 BE | queue_path | buffer_path_len u16 BE |
// buffer_path.
std::pair<std::string, std::string> extract_shm_metadata(
    std::span<const std::uint8_t> body);  // -> (buffer_path, queue_path)

std::vector<std::uint8_t> generate_shm_metadata(EventType event_type,
                                                const std::string& buffer_path,
                                                const std::string& queue_path,
                                                std::uint8_t version);

}  // namespace shmipc::protocol
