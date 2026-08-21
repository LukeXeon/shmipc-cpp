// C++ translation of shmipc-rs examples/src/hello_world/greeter_client.rs

#include <cstdio>
#include <filesystem>
#include <span>
#include <string>

#include "shmipc/shmipc.hpp"

using namespace shmipc;

int main() {
  const auto dir = std::filesystem::current_path();
  const std::string uds_path = (dir / "../ipc_test.sock").string();

  SessionManagerConfig conf;
  conf.config_mut().mem_map_type = MemMapType::kMemFd;
  conf.config_mut().share_memory_path_prefix = "/dev/shm/client.ipc.shm";

  auto sm = SessionManager<DefaultUnixConnect>::create(
      conf, DefaultUnixConnect{}, UnixAddr::from_pathname(uds_path).value());
  if (!sm) {
    std::fprintf(stderr, "failed to create session manager, err: %s\n",
                 sm.error().message().c_str());
    return 1;
  }
  auto stream = sm->get_stream();
  if (!stream) {
    std::fprintf(stderr, "failed to get stream, err: %s\n",
                 stream.error().message().c_str());
    return 1;
  }
  const std::string request_msg = "client say hello world!!!";
  auto w = stream->write_bytes(std::span(
      reinterpret_cast<const std::uint8_t*>(request_msg.data()),
      request_msg.size()));
  if (!w) {
    std::fprintf(stderr, "failed to write, err: %s\n",
                 w.error().message().c_str());
    return 1;
  }
  std::printf("size: %zu\n", *w);
  auto f = stream->flush(true);
  if (!f) {
    std::fprintf(stderr, "failed to flush, err: %s\n",
                 f.error().message().c_str());
    return 1;
  }

  const std::string expected_resp = "server hello world!!!";
  auto resp_msg = stream->read_bytes(expected_resp.size());
  if (!resp_msg) {
    std::fprintf(stderr, "failed to read response, err: %s\n",
                 resp_msg.error().message().c_str());
    return 1;
  }
  std::printf("client stream receive response %s\n",
              std::string(resp_msg->span().begin(), resp_msg->span().end())
                  .c_str());
  sm->put_back(std::move(*stream));
  sm->close();
  return 0;
}
