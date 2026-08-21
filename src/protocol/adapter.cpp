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

#include "shmipc/protocol/adapter.hpp"

#include <algorithm>
#include <cstdio>

#include "shmipc/consts.hpp"
#include "shmipc/protocol/block_io.hpp"
#include "shmipc/protocol/protocol.hpp"

namespace shmipc::protocol {

Result<ProtocolInitializer> ClientProtocolAdapter::get_initializer() const {
  // temporarily ensure version compatibility.
  if (mem_map_type_ == MemMapType::kDevShmFile) {
    return ProtocolInitializer(V2Client{conn_fd_, buffer_path_, queue_path_});
  }
  // send version to peer
  std::uint8_t buf[kHeaderSize] = {0};
  Header h(buf);
  const std::uint8_t client_version = kMaxSupportProtoVersion;
  h.encode(static_cast<std::uint32_t>(kHeaderSize), client_version,
           EventType::kExchangeProtoVersion);
  protocol_trace(h, {}, true);
  if (auto r = block_write_full(conn_fd_, h.as_slice()); !r) {
    return std::unexpected(r.error());
  }
  // recv peer's version
  std::uint8_t recv_buf[kHeaderSize] = {0};
  auto recv_header =
      wait_event_header(conn_fd_, EventType::kExchangeProtoVersion, recv_buf);
  if (!recv_header) {
    return std::unexpected(Error::other(
        "protrocol_initializer_v3 client_init failed, reason:" +
        recv_header.error().message()));
  }
  const std::uint8_t server_version = recv_header->version();
  switch (std::min(client_version, server_version)) {
    case 2:
      return ProtocolInitializer(V2Client{conn_fd_, buffer_path_, queue_path_});
    case 3:
      return ProtocolInitializer(V3Client{conn_fd_, mem_map_type_,
                                          buffer_path_, queue_path_, buffer_fd_,
                                          queue_fd_});
    default: {
      char msg[160];
      std::snprintf(msg, sizeof(msg),
                    "not support the protocol version:%d, max_support_version "
                    "is %d",
                    std::min(client_version, server_version),
                    kMaxSupportProtoVersion);
      return std::unexpected(Error::other(msg));
    }
  }
}

Result<ProtocolInitializer> ServerProtocolAdapter::get_initializer() const {
  // ensure version compatibility
  std::vector<std::uint8_t> buf(kHeaderSize, 0);
  auto h = block_read_event_header(conn_fd_, buf);
  if (!h) {
    return std::unexpected(h.error());
  }
  // The header is a view into `buf`; the V2/V3Server structs take ownership
  // of the buffer (the Rust version leaks it via mem::forget).
  switch (h->version()) {
    case 2: {
      V2Server server{conn_fd_, std::move(buf), Header()};
      server.first_event = Header(server.first_event_storage.data());
      return ProtocolInitializer(std::move(server));
    }
    case 3: {
      V3Server server{conn_fd_, std::move(buf), Header()};
      server.first_event = Header(server.first_event_storage.data());
      return ProtocolInitializer(std::move(server));
    }
    default: {
      char msg[160];
      std::snprintf(msg, sizeof(msg),
                    "not support the protocol version:%d, max_support_version "
                    "is %d",
                    h->version(), kMaxSupportProtoVersion);
      return std::unexpected(Error::other(msg));
    }
  }
}

}  // namespace shmipc::protocol
