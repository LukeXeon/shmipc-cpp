// C++ translation of shmipc-rs examples/src/hello_world/greeter_server.rs

#include <unistd.h>

#include <chrono>
#include <cstdio>
#include <filesystem>
#include <string>
#include <thread>

#include "shmipc/shmipc.hpp"

using namespace shmipc;

static void handle_stream(Stream stream) {
  const std::string expected_req = "client say hello world!!!";
  while (true) {
    auto req_msg = stream.read_bytes(expected_req.size());
    if (!req_msg) {
      std::fprintf(stderr, "failed to read msg, err: %s\n",
                   req_msg.error().message().c_str());
      break;
    }
    std::printf("server receive request %s\n",
                std::string(req_msg->span().begin(), req_msg->span().end())
                    .c_str());

    const std::string resp_msg = "server hello world!!!";
    auto w = stream.write_bytes(std::span(
        reinterpret_cast<const std::uint8_t*>(resp_msg.data()),
        resp_msg.size()));
    if (!w) {
      std::fprintf(stderr, "failed to write msg, err: %s\n",
                   w.error().message().c_str());
      break;
    }
    auto f = stream.flush(true);
    if (!f) {
      std::fprintf(stderr, "failed to flush, err: %s\n",
                   f.error().message().c_str());
      break;
    }
  }
  std::this_thread::sleep_for(std::chrono::seconds(1));
  (void)stream.close();
}

int main() {
  const auto dir = std::filesystem::current_path();
  const std::string uds_path = (dir / "../ipc_test.sock").string();
  ::unlink(uds_path.c_str());

  auto ln = Listener::start(DefaultUnixListen{},
                            UnixAddr::from_pathname(uds_path).value(),
                            Config());
  if (!ln) {
    std::fprintf(stderr, "failed to listen, err: %s\n",
                 ln.error().message().c_str());
    return 1;
  }

  while (true) {
    auto stream = ln->accept();
    if (!stream) {
      std::fprintf(stderr, "failed to accept conn, err: %s\n",
                   stream.error().message().c_str());
      continue;
    }
    std::thread(handle_stream, std::move(*stream)).detach();
  }
}
