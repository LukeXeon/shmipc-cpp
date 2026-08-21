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
// C++ translation of shmipc-rs `src/protocol/adapter.rs`.

#pragma once

#include <string>

#include "shmipc/config.hpp"
#include "shmipc/protocol/initializer.hpp"

namespace shmipc::protocol {

class ClientProtocolAdapter {
 public:
  ClientProtocolAdapter(int conn_fd, MemMapType mem_map_type,
                        std::string buffer_path, std::string queue_path,
                        int buffer_fd, int queue_fd)
      : conn_fd_(conn_fd),
        mem_map_type_(mem_map_type),
        buffer_path_(std::move(buffer_path)),
        queue_path_(std::move(queue_path)),
        buffer_fd_(buffer_fd),
        queue_fd_(queue_fd) {}

  Result<ProtocolInitializer> get_initializer() const;

 private:
  int conn_fd_;
  MemMapType mem_map_type_;
  std::string buffer_path_;
  std::string queue_path_;
  int buffer_fd_;
  int queue_fd_;
};

class ServerProtocolAdapter {
 public:
  explicit ServerProtocolAdapter(int conn_fd) : conn_fd_(conn_fd) {}

  Result<ProtocolInitializer> get_initializer() const;

 private:
  int conn_fd_;
};

}  // namespace shmipc::protocol
