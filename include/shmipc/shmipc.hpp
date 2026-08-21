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
// Umbrella header for the shmipc C++ library.

#pragma once

#include "shmipc/buffer/buf.hpp"
#include "shmipc/buffer/linked.hpp"
#include "shmipc/buffer/list.hpp"
#include "shmipc/buffer/manager.hpp"
#include "shmipc/buffer/slice.hpp"
#include "shmipc/bytes.hpp"
#include "shmipc/compact.hpp"
#include "shmipc/config.hpp"
#include "shmipc/consts.hpp"
#include "shmipc/error.hpp"
#include "shmipc/listener.hpp"
#include "shmipc/log.hpp"
#include "shmipc/queue.hpp"
#include "shmipc/session.hpp"
#include "shmipc/session_config.hpp"
#include "shmipc/session_manager.hpp"
#include "shmipc/stats.hpp"
#include "shmipc/stream.hpp"
#include "shmipc/stream_pool.hpp"
#include "shmipc/transport.hpp"
