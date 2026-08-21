// Protocol tests. The Rust crate has no unit tests in src/protocol, so in
// addition to codec round-trips this exercises the V2/V3 handshakes over a
// socketpair the same way session establishment does.

#include <gtest/gtest.h>

#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <string>
#include <thread>

#include "shmipc/buffer/manager.hpp"
#include "shmipc/config.hpp"
#include "shmipc/consts.hpp"
#include "shmipc/protocol/adapter.hpp"
#include "shmipc/protocol/block_io.hpp"
#include "shmipc/protocol/initializer.hpp"
#include "shmipc/protocol/protocol.hpp"

using namespace shmipc;
using namespace shmipc::protocol;
using namespace std::chrono_literals;

namespace {

std::string unique_suffix() {
  static std::atomic<std::uint64_t> counter{0};
  return std::to_string(::getpid()) + "_" +
         std::to_string(counter.fetch_add(1));
}

struct SocketPair {
  int client_fd = -1;
  int server_fd = -1;
  ~SocketPair() {
    if (client_fd >= 0) ::close(client_fd);
    if (server_fd >= 0) ::close(server_fd);
  }
};

SocketPair make_socketpair() {
  std::array<int, 2> fds{};
  EXPECT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds.data()), 0);
  return {fds[0], fds[1]};
}

}  // namespace

TEST(Protocol, HeaderEncodeDecode) {
  std::uint8_t buf[kHeaderSize] = {0};
  Header h(buf);
  h.encode(4096, 3, EventType::kPolling);

  EXPECT_EQ(h.length(), 4096u);
  EXPECT_EQ(h.magic(), kMagicNumber);
  EXPECT_EQ(h.version(), 3);
  EXPECT_EQ(h.msg_type(), EventType::kPolling);

  // Big-endian layout check.
  EXPECT_EQ(buf[0], 0x00);
  EXPECT_EQ(buf[1], 0x00);
  EXPECT_EQ(buf[2], 0x10);
  EXPECT_EQ(buf[3], 0x00);

  EXPECT_TRUE(check_event_valid(h).has_value());
  EXPECT_NE(h.to_string().find("Polling"), std::string::npos);
}

TEST(Protocol, CheckEventValidRejectsBadFrames) {
  std::uint8_t buf[kHeaderSize] = {0};
  Header h(buf);
  h.encode(kHeaderSize, 3, EventType::kPolling);
  buf[4] = 0x12;  // corrupt magic
  auto r = check_event_valid(h);
  ASSERT_FALSE(r.has_value());
  EXPECT_EQ(r.error(), Err::kInvalidVersion);

  h.encode(kHeaderSize, 0, EventType::kPolling);  // version 0
  r = check_event_valid(h);
  ASSERT_FALSE(r.has_value());
  EXPECT_EQ(r.error(), Err::kInvalidVersion);

  h.encode(kHeaderSize, 2, static_cast<EventType>(99));
  r = check_event_valid(h);
  ASSERT_FALSE(r.has_value());
  EXPECT_EQ(r.error(), Err::kInvalidMsgType);
}

TEST(Protocol, PollingEventWithVersion) {
  const auto& events = polling_event_with_version();
  ASSERT_EQ(events.size(), static_cast<std::size_t>(kMaxSupportProtoVersion) + 1);
  for (std::size_t i = 0; i < events.size(); ++i) {
    Header h(const_cast<std::uint8_t*>(events[i].data()));
    EXPECT_EQ(h.length(), kHeaderSize);
    EXPECT_EQ(h.msg_type(), EventType::kPolling);
    EXPECT_EQ(h.version(), i);
    EXPECT_EQ(h.magic(), kMagicNumber);
  }
}

TEST(Protocol, ShmMetadataRoundTrip) {
  const std::string buffer_path = "/dev/shm/buf_xyz";
  const std::string queue_path = "/dev/shm/q_xyz";
  const auto data = generate_shm_metadata(EventType::kShareMemoryByFilePath,
                                          buffer_path, queue_path, 3);
  Header h(const_cast<std::uint8_t*>(data.data()));
  EXPECT_EQ(h.length(), data.size());
  EXPECT_EQ(h.msg_type(), EventType::kShareMemoryByFilePath);
  EXPECT_EQ(h.version(), 3);

  const auto [bp, qp] =
      extract_shm_metadata(std::span(data.data() + kHeaderSize,
                                     data.size() - kHeaderSize));
  EXPECT_EQ(bp, buffer_path);
  EXPECT_EQ(qp, queue_path);
}

TEST(Protocol, BlockReadWriteFull) {
  auto sp = make_socketpair();
  std::vector<std::uint8_t> data(4096);
  for (std::size_t i = 0; i < data.size(); ++i) {
    data[i] = static_cast<std::uint8_t>(i * 7);
  }
  std::vector<std::uint8_t> received(data.size());

  std::thread writer([&] {
    ASSERT_TRUE(block_write_full(sp.client_fd, data).has_value());
  });
  ASSERT_TRUE(block_read_full(sp.server_fd, received).has_value());
  writer.join();
  EXPECT_EQ(data, received);
}

TEST(Protocol, V2HandshakeOverSocketPair) {
  Config config;
  config.mem_map_type = MemMapType::kDevShmFile;
  config.share_memory_path_prefix =
      "/dev/shm/shmipc_v2_test_" + unique_suffix();
  config.queue_path = config.share_memory_path_prefix + "_queue_0";
  config.share_memory_buffer_cap = 4 << 20;

  // Client side: create shm first, then run the handshake.
  auto managers = init_manager(config);
  ASSERT_TRUE(managers.has_value());
  auto& [client_bm, client_qm] = *managers;

  auto sp = make_socketpair();
  std::optional<Result<std::tuple<std::shared_ptr<buffer::BufferManager>,
                                  QueueManager, std::uint8_t>>>
      server_result;
  std::thread server([&] {
    server_result = init_server_protocol(sp.server_fd, 5s);
  });

  const auto client_version = init_client_protocol(
      client_bm->path(), client_bm->memfd(), client_qm.path(),
      client_qm.memfd(), sp.client_fd, config.mem_map_type, 5s);
  server.join();

  ASSERT_TRUE(client_version.has_value()) << client_version.error().message();
  EXPECT_EQ(*client_version, 2);
  ASSERT_TRUE(server_result.has_value());
  ASSERT_TRUE(server_result->has_value())
      << server_result->error().message();
  auto& [server_bm, server_qm, server_version] = **server_result;
  EXPECT_EQ(server_version, 2);

  // The mapped managers communicate through the shared queue.
  ASSERT_TRUE(client_qm.send_queue.put({77, 0, 0}).has_value());
  auto ele = server_qm.recv_queue.pop();
  ASSERT_TRUE(ele.has_value());
  EXPECT_EQ(ele->seq_id, 77u);

  client_qm.unmap();
}

TEST(Protocol, V3MemfdHandshakeOverSocketPair) {
  Config config;
  config.mem_map_type = MemMapType::kMemFd;
  config.share_memory_path_prefix =
      "/dev/shm/shmipc_v3_test_" + unique_suffix();
  config.queue_path = config.share_memory_path_prefix + "_queue_0";
  config.share_memory_buffer_cap = 4 << 20;

  auto managers = init_manager(config);
  ASSERT_TRUE(managers.has_value());
  auto& [client_bm, client_qm] = *managers;

  auto sp = make_socketpair();
  std::optional<Result<std::tuple<std::shared_ptr<buffer::BufferManager>,
                                  QueueManager, std::uint8_t>>>
      server_result;
  std::thread server([&] {
    server_result = init_server_protocol(sp.server_fd, 5s);
  });

  const auto client_version = init_client_protocol(
      client_bm->path(), client_bm->memfd(), client_qm.path(),
      client_qm.memfd(), sp.client_fd, config.mem_map_type, 5s);
  server.join();

  ASSERT_TRUE(client_version.has_value()) << client_version.error().message();
  EXPECT_EQ(*client_version, 3);
  ASSERT_TRUE(server_result.has_value());
  ASSERT_TRUE(server_result->has_value())
      << server_result->error().message();
  auto& [server_bm, server_qm, server_version] = **server_result;
  EXPECT_EQ(server_version, 3);

  // Both directions of the queue work through the mapped memfds.
  ASSERT_TRUE(client_qm.send_queue.put({1, 2, 3}).has_value());
  auto ele = server_qm.recv_queue.pop();
  ASSERT_TRUE(ele.has_value());
  EXPECT_EQ(ele->seq_id, 1u);

  ASSERT_TRUE(server_qm.send_queue.put({9, 8, 7}).has_value());
  ele = client_qm.recv_queue.pop();
  ASSERT_TRUE(ele.has_value());
  EXPECT_EQ(ele->seq_id, 9u);

  // Buffer slices allocated by the client are visible to the server.
  auto slice = client_bm->alloc_shm_buffer(4096);
  ASSERT_TRUE(slice.has_value());
  const std::string payload = "hello shm";
  slice->append(std::span(reinterpret_cast<const std::uint8_t*>(payload.data()),
                          payload.size()));
  slice->update();
  auto view = server_bm->read_buffer_slice(slice->offset_in_shm);
  ASSERT_TRUE(view.has_value());
  EXPECT_EQ(view->size(), payload.size());

  client_qm.unmap();
}

TEST(Protocol, FallbackDataEventEncode) {
  std::uint8_t buf[16] = {0};
  FallbackDataEvent event(buf);
  event.encode(1024 + 16, 3, 42, 1);

  Header h(buf);
  EXPECT_EQ(h.length(), 1024u + 16);
  EXPECT_EQ(h.magic(), kMagicNumber);
  EXPECT_EQ(h.version(), 3);
  EXPECT_EQ(h.msg_type(), EventType::kFallbackData);
  EXPECT_EQ(event.as_slice().size(), kHeaderSize + 8);
  // seq_id / status big-endian
  EXPECT_EQ(buf[8], 0);
  EXPECT_EQ(buf[11], 42);
  EXPECT_EQ(buf[15], 1);
}
