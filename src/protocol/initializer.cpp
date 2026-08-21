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

#include "shmipc/protocol/initializer.hpp"

#include <array>
#include <cstdio>
#include <cstring>

#include "shmipc/consts.hpp"
#include "shmipc/log.hpp"
#include "shmipc/protocol/block_io.hpp"
#include "shmipc/protocol/protocol.hpp"

namespace shmipc::protocol {

namespace {

inline std::uint16_t load_be_u16(const std::uint8_t* p) {
  return static_cast<std::uint16_t>((p[0] << 8) | p[1]);
}

inline void store_be_u16(std::uint8_t* p, std::uint16_t v) {
  p[0] = static_cast<std::uint8_t>(v >> 8);
  p[1] = static_cast<std::uint8_t>(v);
}

}  // namespace

// ---------------------------------------------------------------------------
// ProtocolInitializer dispatch
// ---------------------------------------------------------------------------

InitResult ProtocolInitializer::init() const {
  return std::visit([](const auto& v) { return v.init(); }, inner_);
}

std::uint8_t ProtocolInitializer::version() const {
  return std::visit(
      [](const auto& v) -> std::uint8_t {
        using T = std::decay_t<decltype(v)>;
        return T::version();
      },
      inner_);
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

Result<Unit> handle_exchange_version(int conn_fd) {
  std::uint8_t buf[kHeaderSize] = {0};
  Header resp_header(buf);
  resp_header.encode(static_cast<std::uint32_t>(kHeaderSize),
                     kMaxSupportProtoVersion,
                     EventType::kExchangeProtoVersion);
  protocol_trace(resp_header, {}, true);
  return block_write_full(conn_fd, resp_header.as_slice());
}

InitResult handle_share_memory_by_memfd(int conn_fd, const Header& h,
                                        std::uint8_t version) {
  SHMIPC_INFO("recv memfd, header:" + h.to_string());
  // 1. recv shm metadata
  std::vector<std::uint8_t> body(h.length() - kHeaderSize);
  if (auto r = block_read_full(conn_fd, body); !r) {
    return std::unexpected(Error::other("read shm metadata failed, reason:" +
                                        r.error().message()));
  }
  const auto [buffer_path, queue_path] = extract_shm_metadata(body);

  // 2. send AckReadyRecvFD
  std::uint8_t ack_buf[kHeaderSize] = {0};
  Header ack(ack_buf);
  ack.encode(static_cast<std::uint32_t>(kHeaderSize), version,
             EventType::kAckReadyRecvFd);
  SHMIPC_INFO("response typeAckReadyRecvFD");
  if (auto r = block_write_full(conn_fd, ack.as_slice()); !r) {
    return std::unexpected(Error::other(
        "send ack TypeAckReadyRecvFD failed reason:" + r.error().message()));
  }
  SHMIPC_INFO("TypeAckReadyRecvFD send finished");

  // 3. recv fd
  SHMIPC_INFO("send ack finished");
  auto fds = block_read_out_of_bound_for_fd(conn_fd);
  if (!fds) {
    return std::unexpected(fds.error());
  }
  if (fds->size() < kMemfdCount) {
    char buf[64];
    std::snprintf(buf, sizeof(buf), "ParseUnixRights len fds:%zu", fds->size());
    SHMIPC_WARN(buf);
    return std::unexpected(
        Error::other("the number of memfd received is wrong"));
  }

  const int buffer_fd = (*fds)[0];
  const int queue_fd = (*fds)[1];
  {
    char buf[320];
    std::snprintf(buf, sizeof(buf),
                  "recv memfd, buffer_path:%s queue_path:%s buffer_fd:%d "
                  "queue_fd:%d",
                  buffer_path.c_str(), queue_path.c_str(), buffer_fd, queue_fd);
    SHMIPC_INFO(buf);
  }

  // 4. mapping share memory
  auto qm = QueueManager::mapping_with_memfd(queue_path, queue_fd);
  if (!qm) {
    return std::unexpected(qm.error());
  }
  std::vector<SizePercentPair> empty_pairs;
  auto bm = buffer::BufferManager::get_with_memfd(buffer_path, buffer_fd, 0,
                                                  false, empty_pairs);
  if (!bm) {
    qm->unmap();
    return std::unexpected(bm.error());
  }
  SHMIPC_INFO("handle_share_memory_by_memfd done");
  return std::optional(Managers{std::move(*bm), std::move(*qm)});
}

Result<Unit> send_memfd_to_peer(int conn_fd, const std::string& buffer_path,
                                int buffer_fd, const std::string& queue_path,
                                int queue_fd, std::uint8_t version) {
  auto event = generate_shm_metadata(EventType::kShareMemoryByMemfd,
                                     buffer_path, queue_path, version);
  const Header h(event.data());
  {
    char buf[256];
    std::snprintf(buf, sizeof(buf),
                  "send_memfd_to_peer buffer fd:%d queue fd:%d header:%s",
                  buffer_fd, queue_fd, h.to_string().c_str());
    SHMIPC_INFO(buf);
  }
  protocol_trace(h,
                 std::span<const std::uint8_t>(event.data() + kHeaderSize,
                                               event.size() - kHeaderSize),
                 true);

  if (auto r = block_write_full(conn_fd, event); !r) {
    return r;
  }
  std::uint8_t buf[kHeaderSize] = {0};
  if (auto r = wait_event_header(conn_fd, EventType::kAckReadyRecvFd, buf);
      !r) {
    return std::unexpected(r.error());
  }
  return send_fd(conn_fd, std::array<int, 2>{buffer_fd, queue_fd});
}

Result<Header> wait_event_header(int conn_fd, EventType expect_event_type,
                                 std::span<std::uint8_t> buf) {
  auto h = block_read_event_header(conn_fd, buf);
  if (!h) {
    return std::unexpected(h.error());
  }
  if (h->msg_type() != expect_event_type) {
    char msg[160];
    std::snprintf(msg, sizeof(msg), "expect event_type:%d %s, but:%s",
                  static_cast<int>(expect_event_type),
                  event_type_name(expect_event_type).c_str(),
                  event_type_name(h->msg_type()).c_str());
    return std::unexpected(Error::other(msg));
  }
  return h;
}

Result<Header> block_read_event_header(int conn_fd,
                                       std::span<std::uint8_t> buf) {
  if (auto r = block_read_full(conn_fd, buf); !r) {
    return std::unexpected(r.error());
  }
  Header h(buf.data());
  if (auto r = check_event_valid(h); !r) {
    return std::unexpected(r.error());
  }
  protocol_trace(h, {}, false);
  return h;
}

InitResult handle_share_memory_by_file_path(int conn_fd, const Header& hdr) {
  SHMIPC_INFO("handle_share_memory_by_file_path head:" + hdr.to_string());
  std::vector<std::uint8_t> body(hdr.length() - kHeaderSize);
  if (auto r = block_read_full(conn_fd, body); !r) {
    const std::string msg = r.error().message();
    if (msg != "EOF" && msg.find("closed") == std::string::npos &&
        msg.find("reset by peer") == std::string::npos) {
      SHMIPC_ERROR("shmipc: failed to read pathlen: " + msg);
    }
    return std::unexpected(r.error());
  }
  const auto [buffer_path, queue_path] = extract_shm_metadata(body);

  auto qm = QueueManager::mapping_with_file(queue_path);
  if (!qm) {
    char buf[512];
    std::snprintf(buf, sizeof(buf),
                  "handle_share_memory_by_file_path mappingQueueManager "
                  "failed,queuePathLen:%zu path:%s err=%s",
                  queue_path.size(), queue_path.c_str(),
                  qm.error().message().c_str());
    return std::unexpected(Error::other(buf));
  }

  std::vector<SizePercentPair> empty_pairs;
  auto bm = buffer::BufferManager::get_with_file(buffer_path, 0, false,
                                                 empty_pairs);
  if (!bm) {
    qm->unmap();
    char buf[512];
    std::snprintf(buf, sizeof(buf),
                  "handle_share_memory_by_file_path mapping_buffer_manager "
                  "failed, buffer_path_len:%zu path:%s err=%s",
                  buffer_path.size(), buffer_path.c_str(),
                  bm.error().message().c_str());
    return std::unexpected(Error::other(buf));
  }

  return std::optional(Managers{std::move(*bm), std::move(*qm)});
}

std::pair<std::string, std::string> extract_shm_metadata(
    std::span<const std::uint8_t> body) {
  std::size_t offset = 0;
  const std::uint16_t queue_path_len = load_be_u16(body.data() + offset);
  offset += 2;
  std::string queue_path(body.begin() + offset,
                         body.begin() + offset + queue_path_len);
  offset += queue_path_len;

  const std::uint16_t buffer_path_len = load_be_u16(body.data() + offset);
  offset += 2;
  std::string buffer_path(body.begin() + offset,
                          body.begin() + offset + buffer_path_len);
  return {buffer_path, queue_path};
}

Result<Unit> send_share_memory_by_file_path(int conn_fd,
                                            const std::string& buffer_path,
                                            const std::string& queue_path,
                                            std::uint8_t version) {
  auto data = generate_shm_metadata(EventType::kShareMemoryByFilePath,
                                    buffer_path, queue_path, version);
  const Header h(data.data());
  protocol_trace(h,
                 std::span<const std::uint8_t>(data.data() + kHeaderSize,
                                               data.size() - kHeaderSize),
                 true);
  return block_write_full(conn_fd, data);
}

std::vector<std::uint8_t> generate_shm_metadata(EventType event_type,
                                                const std::string& buffer_path,
                                                const std::string& queue_path,
                                                std::uint8_t version) {
  std::vector<std::uint8_t> data(kHeaderSize + 2 + buffer_path.size() + 2 +
                                 queue_path.size());
  Header h(data.data());
  h.encode(static_cast<std::uint32_t>(data.size()), version, event_type);
  std::size_t offset = kHeaderSize;
  // queue share memory path
  store_be_u16(data.data() + offset,
               static_cast<std::uint16_t>(queue_path.size()));
  offset += 2;
  std::memcpy(data.data() + offset, queue_path.data(), queue_path.size());
  offset += queue_path.size();
  // buffer share memory path
  store_be_u16(data.data() + offset,
               static_cast<std::uint16_t>(buffer_path.size()));
  offset += 2;
  std::memcpy(data.data() + offset, buffer_path.data(), buffer_path.size());
  return data;
}

// ---------------------------------------------------------------------------
// V2
// ---------------------------------------------------------------------------

InitResult V2Client::init() const {
  auto r = send_share_memory_by_file_path(conn_fd, buffer_path, queue_path, 2);
  if (!r) {
    return std::unexpected(r.error());
  }
  // The Rust version returns Ok(None) from send_share_memory_by_file_path.
  return std::nullopt;
}

InitResult V2Server::init() const {
  if (first_event.msg_type() != EventType::kShareMemoryByFilePath) {
    char buf[192];
    std::snprintf(buf, sizeof(buf),
                  "ProtocolInitializerV2 expect first event is:%d(%s),but:%s",
                  static_cast<int>(EventType::kShareMemoryByFilePath),
                  event_type_name(EventType::kShareMemoryByFilePath).c_str(),
                  event_type_name(first_event.msg_type()).c_str());
    return std::unexpected(Error::other(buf));
  }
  return handle_share_memory_by_file_path(conn_fd, first_event);
}

// ---------------------------------------------------------------------------
// V3
// ---------------------------------------------------------------------------

InitResult V3Client::init() const {
  Result<Unit> r =
      mem_map_type == MemMapType::kDevShmFile
          ? send_share_memory_by_file_path(conn_fd, buffer_path, queue_path, 3)
          : send_memfd_to_peer(conn_fd, buffer_path, buffer_fd, queue_path,
                               queue_fd, 3);
  if (!r) {
    return std::unexpected(r.error());
  }
  std::uint8_t buf[kHeaderSize] = {0};
  if (auto ack = wait_event_header(conn_fd, EventType::kAckShareMemory, buf);
      !ack) {
    return std::unexpected(ack.error());
  }
  return std::nullopt;
}

InitResult V3Server::init() const {
  if (EventType::kExchangeProtoVersion != first_event.msg_type()) {
    char buf[192];
    std::snprintf(buf, sizeof(buf),
                  "ProtocolInitializerV3 expect first event is:%d(%s),but:%s",
                  static_cast<int>(EventType::kExchangeProtoVersion),
                  event_type_name(EventType::kExchangeProtoVersion).c_str(),
                  event_type_name(first_event.msg_type()).c_str());
    return std::unexpected(Error::other(buf));
  }

  // 1. exchange version
  if (auto r = handle_exchange_version(conn_fd); !r) {
    return std::unexpected(Error::other(
        "ProtocolInitializerV3 exchange_version failed, reason:" +
        r.error().message()));
  }

  // 2. recv and mapping share memory
  std::uint8_t buf[kHeaderSize] = {0};
  auto h = block_read_event_header(conn_fd, buf);
  if (!h) {
    return std::unexpected(Error::other(
        "ProtocolInitializerV3 block_read_event_header failed, reason:" +
        h.error().message()));
  }
  InitResult r;
  if (h->msg_type() == EventType::kShareMemoryByFilePath) {
    r = handle_share_memory_by_file_path(conn_fd, *h);
  } else if (h->msg_type() == EventType::kShareMemoryByMemfd) {
    r = handle_share_memory_by_memfd(conn_fd, *h, 3);
  } else {
    char msg[220];
    std::snprintf(
        msg, sizeof(msg),
        "expect event type is TypeShareMemoryByFilePath or "
        "TypeShareMemoryByMemfd, but:%d(%s)",
        static_cast<int>(h->msg_type()),
        event_type_name(h->msg_type()).c_str());
    return std::unexpected(Error::other(msg));
  }
  if (!r) {
    return r;
  }

  // 3. ack share memory
  std::uint8_t resp_buf[kHeaderSize] = {0};
  Header resp_header(resp_buf);
  resp_header.encode(static_cast<std::uint32_t>(kHeaderSize), 3,
                     EventType::kAckShareMemory);
  protocol_trace(resp_header, {}, true);
  if (auto wr = block_write_full(conn_fd, resp_header.as_slice()); !wr) {
    return std::unexpected(wr.error());
  }
  return r;
}

}  // namespace shmipc::protocol
