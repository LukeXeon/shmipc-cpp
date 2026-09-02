# Copyright 2026
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

cmake_minimum_required(VERSION 3.20)
project(shmipc_cpp VERSION 0.1.1 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "Build type" FORCE)
endif()

option(SHMIPC_BUILD_TESTS "Build shmipc unit tests" ON)
option(SHMIPC_BUILD_EXAMPLES "Build shmipc examples" ON)
option(SHMIPC_BUILD_BENCH "Build shmipc benchmark" ON)
option(SHMIPC_ENABLE_ASAN "Enable AddressSanitizer" OFF)
option(SHMIPC_ENABLE_TSAN "Enable ThreadSanitizer" OFF)

if(NOT CMAKE_SYSTEM_NAME MATCHES "^(Linux|Android)$") # rosetta: NDK 下 CMAKE_SYSTEM_NAME = Android(Linux 内核)
  message(FATAL_ERROR "shmipc just support linux OS now")
endif()
if(NOT CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64|amd64|aarch64|arm64")
  message(FATAL_ERROR "shmipc just support amd64 or arm64 arch")
endif()

find_package(Threads REQUIRED)

set(SHMIPC_SANITIZER_FLAGS "")
if(SHMIPC_ENABLE_ASAN)
  set(SHMIPC_SANITIZER_FLAGS -fsanitize=address -fno-omit-frame-pointer)
endif()
if(SHMIPC_ENABLE_TSAN)
  set(SHMIPC_SANITIZER_FLAGS -fsanitize=thread)
endif()

# ---------------------------------------------------------------------------
# Library
# ---------------------------------------------------------------------------
add_library(shmipc
  src/error.cpp
  src/log.cpp
  src/config.cpp
  src/util/dev_shm.cpp
  src/util/buf_reader.cpp
  src/buffer/slice.cpp
  src/buffer/list.cpp
  src/buffer/manager.cpp
  src/buffer/linked.cpp
  src/queue.cpp
  src/protocol/event.cpp
  src/protocol/block_io.cpp
  src/protocol/initializer.cpp
  src/protocol/adapter.cpp
  src/protocol/protocol.cpp
  src/transport.cpp
  src/stream.cpp
  src/stream_pool.cpp
  src/session.cpp
  src/listener.cpp
  src/compact.cpp
)
add_library(shmipc::shmipc ALIAS shmipc)

target_include_directories(shmipc PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
)
target_link_libraries(shmipc PUBLIC Threads::Threads)
target_compile_options(shmipc PRIVATE
  -Wall -Wextra -Wpedantic
  ${SHMIPC_SANITIZER_FLAGS}
)
target_link_options(shmipc PUBLIC ${SHMIPC_SANITIZER_FLAGS})

# ---------------------------------------------------------------------------
# Tests (GoogleTest, vendored in third_party)
# ---------------------------------------------------------------------------
if(SHMIPC_BUILD_TESTS)
  enable_testing()
  set(BUILD_GMOCK OFF CACHE BOOL "" FORCE)
  set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
  set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
  add_subdirectory(third_party/googletest EXCLUDE_FROM_ALL)

  set(SHMIPC_TEST_TARGETS
    test_config
    test_slice
    test_buffer_list
    test_buffer_manager
    test_linked_buffer
    test_queue
    test_util
    test_protocol
    test_integration
  )
  foreach(t ${SHMIPC_TEST_TARGETS})
    add_executable(${t} tests/${t}.cpp)
    target_link_libraries(${t} PRIVATE shmipc::shmipc GTest::gtest_main)
    target_compile_options(${t} PRIVATE ${SHMIPC_SANITIZER_FLAGS})
    target_link_options(${t} PRIVATE ${SHMIPC_SANITIZER_FLAGS})
    add_test(NAME ${t} COMMAND ${t})
  endforeach()
  # Tests exercise shared memory and sockets; give them room to run.
  set_tests_properties(${SHMIPC_TEST_TARGETS} PROPERTIES TIMEOUT 300)
endif()

# ---------------------------------------------------------------------------
# Examples
# ---------------------------------------------------------------------------
if(SHMIPC_BUILD_EXAMPLES)
  foreach(ex greeter_server greeter_client greeter_server_tokio greeter_client_tokio)
    if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/examples/${ex}.cpp)
      add_executable(${ex} examples/${ex}.cpp)
      target_link_libraries(${ex} PRIVATE shmipc::shmipc)
    endif()
  endforeach()
endif()

# ---------------------------------------------------------------------------
# Benchmark
# ---------------------------------------------------------------------------
if(SHMIPC_BUILD_BENCH AND EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/bench/bench_ping_pong.cpp)
  add_executable(shmipc_bench bench/bench_ping_pong.cpp)
  target_link_libraries(shmipc_bench PRIVATE shmipc::shmipc)
endif()
