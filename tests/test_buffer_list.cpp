// C++ translation of shmipc-rs src/buffer/list.rs tests.

#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <thread>
#include <vector>

#include "shmipc/buffer/list.hpp"
#include "shmipc/buffer/manager.hpp"

using namespace shmipc;
using namespace shmipc::buffer;

TEST(BufferList, PutPop) {
  constexpr std::uint32_t kCapPerBuffer = 4096;
  constexpr std::uint32_t kBufferNum = 1000;
  auto mem = map_anonymous(count_buffer_list_mem_size(kBufferNum, kCapPerBuffer));
  ASSERT_TRUE(mem.has_value());

  auto l = BufferList::create(kBufferNum, kCapPerBuffer, mem->data(),
                              mem->size(), 0);
  ASSERT_TRUE(l.has_value());

  std::vector<BufferSlice> buffers;
  buffers.reserve(1024);
  const std::int64_t origin_size = l->remain();
  while (l->remain() > 0) {
    auto b = l->pop();
    ASSERT_TRUE(b.has_value());
    EXPECT_EQ(b->capacity(), kCapPerBuffer);
    EXPECT_EQ(b->size(), 0u);
    ASSERT_TRUE(b->buffer_header.has_value());
    EXPECT_FALSE(b->buffer_header->has_next());
    buffers.push_back(std::move(*b));
  }

  for (auto& buffer : buffers) {
    l->push(std::move(buffer));
  }

  buffers.clear();
  EXPECT_EQ(origin_size, l->remain());
  while (l->remain() > 0) {
    auto b = l->pop();
    ASSERT_TRUE(b.has_value());
    EXPECT_EQ(b->capacity(), kCapPerBuffer);
    EXPECT_EQ(b->size(), 0u);
    ASSERT_TRUE(b->buffer_header.has_value());
    EXPECT_FALSE(b->buffer_header->has_next());
    buffers.push_back(std::move(*b));
  }
}

TEST(BufferList, ConcurrentPutPop) {
  constexpr std::uint32_t kCapPerBuffer = 16;
  constexpr std::uint32_t kBufferNum = 100;
  auto mem_holder = std::make_shared<MemoryMap>();
  {
    auto mem = map_anonymous(count_buffer_list_mem_size(kBufferNum, kCapPerBuffer));
    ASSERT_TRUE(mem.has_value());
    *mem_holder = std::move(*mem);
  }
  auto l = std::make_shared<BufferList>();
  {
    auto created = BufferList::create(kBufferNum, kCapPerBuffer,
                                      mem_holder->data(), mem_holder->size(), 0);
    ASSERT_TRUE(created.has_value());
    *l = std::move(*created);
  }

  constexpr int kConcurrency = 10;
  std::vector<std::thread> threads;
  for (int t = 0; t < kConcurrency; ++t) {
    threads.emplace_back([l, kCapPerBuffer] {
      for (int i = 0; i < 10000; ++i) {
        auto b = l->pop();
        while (!b.has_value()) {
          std::this_thread::sleep_for(std::chrono::milliseconds(1));
          b = l->pop();
        }
        EXPECT_EQ(b->capacity(), kCapPerBuffer);
        EXPECT_EQ(b->size(), 0u);
        ASSERT_TRUE(b->buffer_header.has_value());
        EXPECT_FALSE(b->buffer_header->has_next())
            << "offset:" << b->offset_in_shm
            << " next:" << b->buffer_header->next_buffer_offset();
        l->push(std::move(*b));
      }
    });
  }
  for (auto& t : threads) {
    t.join();
  }
  EXPECT_EQ(static_cast<std::int32_t>(kBufferNum), l->size_load());
}

TEST(BufferList, CreateAndMappingFreeBufferList) {
  constexpr std::uint32_t kCapPerBuffer = 16;
  constexpr std::uint32_t kBufferNum = 10;
  auto mem = map_anonymous(count_buffer_list_mem_size(kBufferNum, kCapPerBuffer));
  ASSERT_TRUE(mem.has_value());

  auto l = BufferList::create(0, kCapPerBuffer, mem->data(), mem->size(), 0);
  ASSERT_FALSE(l.has_value());

  l = BufferList::create(kBufferNum + 1, kCapPerBuffer, mem->data(), mem->size(), 0);
  ASSERT_FALSE(l.has_value());

  auto ok = BufferList::create(kBufferNum, kCapPerBuffer, mem->data(),
                               mem->size(), 0);
  ASSERT_TRUE(ok.has_value());

  auto test_mem = map_anonymous(10);
  ASSERT_TRUE(test_mem.has_value());
  auto ml = BufferList::mapping(test_mem->data(), test_mem->size(), 0);
  ASSERT_FALSE(ml.has_value());

  ml = BufferList::mapping(mem->data(), mem->size(), 8);
  ASSERT_FALSE(ml.has_value());

  ml = BufferList::mapping(mem->data(), mem->size(), 0);
  ASSERT_TRUE(ml.has_value());
}

// The Rust original (#[should_panic]) overflows u32 arithmetic; the C++ port
// detects the overflow and returns an error instead of wrapping.
TEST(BufferList, CreateRejectsOverflowingDimensions) {
  auto mem = map_anonymous(1);
  ASSERT_TRUE(mem.has_value());
  auto r = BufferList::create(4294967295u, 4294967295u, mem->data(),
                              mem->size(), 4294967279u);
  ASSERT_FALSE(r.has_value());
}
