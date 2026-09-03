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
// C++ translation of shmipc-rs `src/session/manager.rs`.
//
// SessionManager provides an implementation similar to a connection pool:
// a pair of connections (streams) between two processes. When get_stream()
// returns an error, you may need to fall back according to your own
// scenario, such as falling back to a unix domain socket.
//
// When a client process talks to one server process, share one
// SessionManager globally. For multiple server processes, keep a separate
// SessionManager per server with distinct queue_path /
// share_memory_path_prefix.

#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <vector>

/* [rosetta patch 0003 修订 M4 集中管理] compat 件统一收编主仓
 * shim/compat(单一事实源;本仓自带副本退役):
 *   - atomic_shared_ptr → compat/atomic_shared_ptr.h(NDK libc++ 缺口)
 *   - memfd_create      → 主仓 compat 库裸符号(API<30)
 * include 路径与 -include compat_decls.h 由主仓 CMake 注入。 */
#include "compat/atomic_shared_ptr.h"
#include "shmipc/consts.hpp"
#include "shmipc/error.hpp"
#include "shmipc/log.hpp"
#include "shmipc/session.hpp"
#include "shmipc/session_config.hpp"
#include "shmipc/stream.hpp"
#include "shmipc/sync/event.hpp"

namespace shmipc {

inline constexpr std::size_t kSessionRoundRobinThreshold = 32;

template <typename C>
class SessionManager {
 public:
  SessionManager() = default;

  // Create a new SessionManager (mirrors `SessionManager::new`).
  static Result<SessionManager> create(SessionManagerConfig sm_config,
                                       C connect,
                                       typename C::Address addr) {
    auto inner = std::make_shared<Inner>();
    inner->sm_config = std::move(sm_config);
    inner->connect = std::move(connect);
    inner->addr = std::move(addr);

    const std::size_t session_num = inner->sm_config.session_num();
    inner->sessions.reserve(session_num);
    for (std::size_t i = 0; i < session_num; ++i) {
      auto session = Session::client(i, 0, 0, inner->sm_config,
                                     inner->connect, inner->addr);
      if (!session) {
        return std::unexpected(session.error());
      }
      auto slot = std::make_unique<SessionSlot>();
      slot->session.store(std::make_shared<Session>(std::move(*session)));
      inner->sessions.push_back(std::move(slot));
    }

    SessionManager sm;
    sm.inner_ = inner;
    for (std::size_t i = 0; i < session_num; ++i) {
      inner->rebuild_threads.emplace_back([inner, i] {
        rebuild_session(inner, i);
      });
    }
    return sm;
  }

  // Mirror of the Rust ArcSwap slots.
  // [rosetta patch 0003] std::atomic<std::shared_ptr<T>> relies on the
  // C++20 partial specialization that libc++ only ships from LLVM 19;
  // on older libc++ (NDK) the member declaration itself fails to
  // instantiate. compat.hpp's mutex-based atomic_shared_ptr keeps the
  // load()/store() surface, so this member declaration and every use
  // site below stay byte-identical to upstream. SessionSlot is pure
  // in-process pool bookkeeping: it never touches the wire or the
  // shared-memory layout (the INV-10 compatibility surface is the
  // queue/buffer memfd protocol, untouched here).
  struct SessionSlot {
    rosetta::compat::atomic_shared_ptr<Session> session;
  };

  SessionManager(const SessionManager&) = default;
  SessionManager& operator=(const SessionManager&) = default;
  SessionManager(SessionManager&&) noexcept = default;
  SessionManager& operator=(SessionManager&&) noexcept = default;

  // Get a ShmIPC Stream from the pool.
  // NOTE: after using the Stream, you MUST explicitly call put_back() for
  // releasing it, otherwise it will cause a resource leak.
  Result<Stream> get_stream() const {
    const auto& sessions = inner_->sessions;
    if (sessions.empty()) {
      return std::unexpected(Err::kSessionUnhealthy);
    }
    const std::size_t i =
        ((inner_->count.fetch_add(1, std::memory_order_seq_cst) + 1) /
         kSessionRoundRobinThreshold) %
        sessions.size();
    return sessions[i]->session.load()->get_or_open_stream(i);
  }

  // Return an unused stream to the stream pool for next time using.
  void put_back(Stream stream) const {
    inner_->sessions[stream.session_id()]->session.load()
        ->put_or_close_stream(std::move(stream));
  }

  // Close all sessions in SessionManager.
  void close() const {
    inner_->shutdown_event.set();
    for (const auto& s : inner_->sessions) {
      s->session.load()->close();
    }
    for (auto& t : inner_->rebuild_threads) {
      if (t.joinable()) {
        t.join();
      }
    }
  }

  explicit operator bool() const noexcept { return inner_ != nullptr; }

 private:
  struct Inner {
    SessionManagerConfig sm_config;
    C connect;
    typename C::Address addr;
    // ArcSwap equivalent: each slot atomically swaps its Session handle.
    // Heap-held because std::atomic is not movable (vector requirement).
    std::vector<std::unique_ptr<SessionSlot>> sessions;
    std::atomic<std::size_t> count{0};
    std::uint64_t epoch = 0;
    std::uint64_t rand_id = 0;
    StateType state = StateType::kDefault;
    Event shutdown_event;
    std::vector<std::thread> rebuild_threads;

    ~Inner() {
      shutdown_event.set();
      for (auto& t : rebuild_threads) {
        if (t.joinable()) {
          t.join();
        }
      }
    }
  };

  static void rebuild_session(std::shared_ptr<Inner> inner, std::size_t i) {
    while (true) {
      const auto session = inner->sessions[i]->session.load();
      // Wait for the session's shutdown or the manager's shutdown.
      bool session_shutdown = false;
      while (!inner->shutdown_event.is_set()) {
        if (session->shared()->shutdown_event.wait_for(
                std::chrono::milliseconds(200))) {
          session_shutdown = true;
          break;
        }
      }
      if (!session_shutdown || inner->shutdown_event.is_set()) {
        return;
      }

      while (true) {
        std::this_thread::sleep_for(inner->sm_config.config().rebuild_interval);

        auto config = inner->sm_config;  // fresh clone per attempt
        auto new_session = Session::client(i, 0, 0, config, inner->connect,
                                           inner->addr);
        if (new_session) {
          inner->sessions[i]->session.store(
              std::make_shared<Session>(std::move(*new_session)));
          SHMIPC_INFO("rebuild session " + std::to_string(i) + " success");
          break;
        }
        SHMIPC_ERROR("rebuild session " + std::to_string(i) +
                     " error: " + new_session.error().message() +
                     ", retry after the rebuild interval");
      }
    }
  }

  std::shared_ptr<Inner> inner_;
};

}  // namespace shmipc
