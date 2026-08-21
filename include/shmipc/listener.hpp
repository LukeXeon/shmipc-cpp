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
// C++ translation of shmipc-rs `src/listener.rs`.

#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "shmipc/config.hpp"
#include "shmipc/error.hpp"
#include "shmipc/log.hpp"
#include "shmipc/session.hpp"
#include "shmipc/stream.hpp"
#include "shmipc/sync/channel.hpp"
#include "shmipc/transport.hpp"

namespace shmipc {

class Listener {
 public:
  Listener() = default;
  Listener(const Listener&) = delete;
  Listener& operator=(const Listener&) = delete;
  Listener(Listener&&) noexcept = default;
  Listener& operator=(Listener&&) noexcept = default;
  ~Listener();

  // Mirrors `Listener::new` (L: TransportListen equivalent).
  template <typename L>
  static Result<Listener> start(const L& listen,
                                const typename L::Address& addr,
                                Config config);

  // Accept a ShmIPC connection Stream.
  // NOTE: after using the Stream, you MUST explicitly call stream.close()
  // for releasing it, otherwise it will cause a resource leak.
  IoResult<Stream> accept();

  // Close all sessions and stop the accept loop.
  void close();

 private:
  Receiver<IoResult<Stream>> stream_rx_;
  std::shared_ptr<std::vector<Session>> sessions_;
  std::shared_ptr<std::mutex> sessions_mtx_;
  std::thread accept_thread_;
  int listener_fd_ = -1;
};

// ---------------------------------------------------------------------------
// Template implementation
// ---------------------------------------------------------------------------

namespace detail {

template <typename ListenerT>
void accept_loop(ListenerT listener, Config config,
                 Sender<IoResult<Stream>> stream_tx,
                 std::shared_ptr<std::vector<Session>> sessions,
                 std::shared_ptr<std::mutex> sessions_mtx) {
  while (true) {
    auto res = listener.accept();
    if (!res) {
      (void)stream_tx.send(IoResult<Stream>(
          std::unexpected(to_io_error_code(res.error()))));
      return;
    }

    auto [tx, rx] = make_channel<Stream>(config.max_stream_num);

    auto stream_ptr = std::make_unique<typename ListenerT::StreamT>(
        std::move(*res));
    auto session =
        Session::server(config, std::move(stream_ptr), std::move(tx));
    if (!session) {
      SHMIPC_WARN("[ShmIPC] failed to create session, err: " +
                  session.error().message());
      continue;
    }
    {
      const std::lock_guard<std::mutex> lock(*sessions_mtx);
      sessions->push_back(*session);
    }
    // Mirrors tokio::spawn(session.recv_loop(rx, stream_tx.clone())).
    std::thread([session = *session, rx = std::move(rx),
                 tx2 = stream_tx]() mutable {
      session.recv_loop(std::move(rx), std::move(tx2));
    }).detach();
  }
}

}  // namespace detail

template <typename L>
Result<Listener> Listener::start(const L& listen,
                                 const typename L::Address& addr,
                                 Config config) {
  auto listener = listen.listen(addr);
  if (!listener) {
    return std::unexpected(listener.error());
  }

  auto [tx, rx] = make_channel<IoResult<Stream>>(0);  // unbounded

  Listener ln;
  ln.stream_rx_ = std::move(rx);
  ln.sessions_ = std::make_shared<std::vector<Session>>();
  ln.sessions_mtx_ = std::make_shared<std::mutex>();
  ln.listener_fd_ = listener->fd();

  ln.accept_thread_ = std::thread(
      [l = std::move(*listener), cfg = std::move(config), tx2 = std::move(tx),
       sessions = ln.sessions_, mtx = ln.sessions_mtx_]() mutable {
        detail::accept_loop<std::decay_t<decltype(l)>>(
            std::move(l), std::move(cfg), std::move(tx2),
            std::move(sessions), std::move(mtx));
      });
  return ln;
}

}  // namespace shmipc
