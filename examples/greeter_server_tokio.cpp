// C++ translation of shmipc-rs examples/src/hello_world/greeter_server_tokio.rs
// (uses the StreamExt IO facade, the tokio AsyncRead/AsyncWrite adapter).

#include <unistd.h>

#include <chrono>
#include <cstdio>
#include <filesystem>
#include <string>
#include <thread>
#include <vector>

#include "shmipc/shmipc.hpp"

using namespace shmipc;

static void handle_stream(Stream stream) {
  const std::string expected_req = "client say hello world!!!";
  std::vector<std::uint8_t> buf(4096);
  StreamExt conn(std::move(stream));

  while (true) {
    auto r = conn.read_exact(std::span(buf.data(), expected_req.size()));
    if (!r) {
      std::fprintf(stderr, "failed to read msg, err: %s\n",
                   r.error().message().c_str());
      break;
    }
    std::printf("read %zu\n", *r);
    std::printf("server receive request %s\n",
                std::string(buf.begin(), buf.begin() + expected_req.size())
                    .c_str());

    const std::string resp_msg = "server hello world!!!";
    auto w = conn.write_all(std::span(
        reinterpret_cast<const std::uint8_t*>(resp_msg.data()),
        resp_msg.size()));
    if (!w) {
      std::fprintf(stderr, "failed to write msg, err: %s\n",
                   w.error().message().c_str());
      break;
    }
    if (auto f = conn.flush(); !f) {
      std::fprintf(stderr, "failed to flush, err: %s\n",
                   f.error().message().c_str());
      break;
    }
  }
  std::this_thread::sleep_for(std::chrono::seconds(1));
  auto s = conn.shutdown();
  if (!s) {
    std::fprintf(stderr, "failed to shutdown, err: %s\n",
                 s.error().message().c_str());
  }
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
