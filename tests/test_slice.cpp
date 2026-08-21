// C++ translation of shmipc-rs src/buffer/slice.rs tests.

#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <cstring>
#include <random>
#include <vector>

#include "shmipc/buffer/manager.hpp"
#include "shmipc/buffer/slice.hpp"
#include "shmipc/config.hpp"

using namespace shmipc;
using namespace shmipc::buffer;

TEST(BufferSlice, ReadWrite) {
  constexpr std::size_t kSize = 8192;

  std::array<std::uint8_t, kSize> buf{};
  BufferSlice slice(std::nullopt, std::span(buf.data(), buf.size()), 0, false);
  for (std::size_t i = 0; i < kSize; ++i) {
    const std::uint8_t b = static_cast<std::uint8_t>(i);
    const std::size_t n = slice.append(std::span(&b, 1));
    ASSERT_EQ(n, 1u);
  }
  const std::uint8_t extra = 10;
  ASSERT_EQ(slice.append(std::span(&extra, 1)), 0u);

  const auto data = slice.read(kSize * 10);
  ASSERT_EQ(data.size(), kSize);
  for (std::size_t i = 0; i < kSize; ++i) {
    EXPECT_EQ(data[i], static_cast<std::uint8_t>(i));
  }
}

TEST(BufferSlice, Skip) {
  constexpr std::size_t kSize = 8192;

  std::array<std::uint8_t, kSize> buf{};
  BufferSlice slice(std::nullopt, std::span(buf.data(), buf.size()), 0, false);
  std::array<std::uint8_t, kSize> zeros{};
  slice.append(zeros);
  std::size_t remain = slice.capacity();

  std::size_t n = slice.skip(10);
  remain -= n;
  EXPECT_EQ(remain, slice.size());

  n = slice.skip(100);
  remain -= n;
  EXPECT_EQ(remain, slice.size());

  (void)slice.skip(10000);
  EXPECT_EQ(0u, slice.size());
}

TEST(BufferSlice, Reserve) {
  constexpr std::size_t kSize = 8192;

  std::array<std::uint8_t, kSize> buf{};
  BufferSlice slice(std::nullopt, std::span(buf.data(), buf.size()), 0, false);
  auto data1 = slice.reserve(100);
  ASSERT_TRUE(data1.has_value());
  ASSERT_EQ(100u, data1->size());

  for (std::size_t i = 0; i < data1->size(); ++i) {
    (*data1)[i] = static_cast<std::uint8_t>(i);
  }
  // Copy the reserved view before the next reserve invalidates borrowing.
  std::vector<std::uint8_t> data1_copy(data1->begin(), data1->end());

  auto data2 = slice.reserve(kSize);
  ASSERT_FALSE(data2.has_value());

  const auto read_data = slice.read(100);
  ASSERT_EQ(100u, read_data.size());
  for (std::size_t i = 0; i < 100; ++i) {
    EXPECT_EQ(read_data[i], data1_copy[i]);
  }

  const auto read_more = slice.read(10000);
  EXPECT_EQ(read_more.size(), 0u);
}

TEST(BufferSlice, Update) {
  constexpr std::size_t kSize = 8192;

  std::array<std::uint8_t, kSize> buf{};
  std::array<std::uint8_t, kBufferHeaderSize> header{};
  {
    std::uint32_t cap = 8192;
    std::memcpy(header.data() + kBufferCapOffset, &cap, sizeof(cap));
  }
  BufferSlice slice(BufferHeader(header.data()),
                    std::span(buf.data(), buf.size()), 0, true);

  std::array<std::uint8_t, kSize> zeros{};
  const std::size_t n = slice.append(zeros);
  ASSERT_EQ(n, kSize);
  slice.update();
  ASSERT_TRUE(slice.buffer_header.has_value());
  EXPECT_EQ(kSize, static_cast<std::size_t>(slice.buffer_header->size()));
}

TEST(BufferSlice, LinkedNext) {
  constexpr std::size_t kSize = 8192;
  constexpr std::size_t kSliceNum = 100;

  auto mem = map_anonymous(10 << 20);
  ASSERT_TRUE(mem.has_value());
  auto bm_res = BufferManager::create(
      std::vector<SizePercentPair>{{8192, 100}}, "", std::move(*mem), 0);
  ASSERT_TRUE(bm_res.has_value());
  auto bm = std::make_shared<BufferManager>(std::move(*bm_res));

  std::vector<BufferSlice> slices;
  slices.reserve(kSliceNum);
  std::vector<std::vector<std::uint8_t>> write_data_array;
  write_data_array.reserve(kSliceNum);
  std::mt19937_64 rng(42);

  for (std::size_t i = 0; i < kSliceNum; ++i) {
    auto s = bm->alloc_shm_buffer(static_cast<std::uint32_t>(kSize));
    ASSERT_TRUE(s.has_value());
    std::vector<std::uint8_t> data(kSize);
    for (auto& b : data) {
      b = static_cast<std::uint8_t>(rng());
    }
    ASSERT_EQ(s->append(data), kSize);
    s->update();
    slices.push_back(std::move(*s));
    write_data_array.push_back(std::move(data));
  }

  for (std::size_t i = 0; i + 1 < slices.size(); ++i) {
    ASSERT_TRUE(slices[i].buffer_header.has_value());
    slices[i].buffer_header->link_next(slices[i + 1].offset_in_shm);
  }

  std::uint32_t next = slices[0].offset_in_shm;
  for (std::size_t i = 0; i < kSliceNum; ++i) {
    auto s = bm->read_buffer_slice(next);
    ASSERT_TRUE(s.has_value());
    EXPECT_EQ(s->capacity(), kSize);
    EXPECT_EQ(s->size(), kSize);
    const auto read_data = s->read(kSize);
    ASSERT_EQ(read_data.size(), kSize);
    for (std::size_t j = 0; j < kSize; ++j) {
      ASSERT_EQ(read_data[j], write_data_array[i][j]);
    }
    const bool is_last_slice = i == kSliceNum - 1;
    ASSERT_TRUE(s->buffer_header.has_value());
    EXPECT_EQ(s->buffer_header->has_next(), !is_last_slice);
    next = s->buffer_header->next_buffer_offset();
  }
}

namespace {
BufferSlice make_test_slice() {
  static std::vector<std::uint8_t> pool[256];
  static std::size_t idx = 0;
  auto& buf = pool[idx++ % 256];
  buf.assign(1024, 0);
  return BufferSlice(std::nullopt, std::span(buf.data(), buf.size()), 0, false);
}
}  // namespace

TEST(SliceList, PushPop) {
  // 1. twice push, twice pop
  SliceList l;
  l.push_back(make_test_slice());
  ASSERT_EQ(l.size(), 1u);
  EXPECT_EQ(l.front(), l.back());

  l.push_back(make_test_slice());
  ASSERT_EQ(l.size(), 2u);
  EXPECT_NE(l.front(), l.back());

  l.pop_front();
  ASSERT_EQ(l.size(), 1u);
  EXPECT_EQ(l.front(), l.back());

  l.pop_front();
  ASSERT_EQ(l.size(), 0u);
  EXPECT_EQ(l.front(), nullptr);
  EXPECT_EQ(l.back(), nullptr);

  // multi push and pop
  for (std::size_t i = 0; i < 100; ++i) {
    l.push_back(make_test_slice());
    EXPECT_EQ(l.size(), i + 1);
  }
  for (std::size_t i = 0; i < 100; ++i) {
    l.pop_front();
    EXPECT_EQ(l.size(), 100 - (i + 1));
  }
  EXPECT_EQ(l.size(), 0u);
  EXPECT_EQ(l.front(), nullptr);
  EXPECT_EQ(l.back(), nullptr);
}

TEST(SliceList, SplitFromWrite) {
  // 1. sliceList's size == 1
  {
    SliceList l;
    l.push_back(make_test_slice());
    l.set_write(l.front());
    ASSERT_FALSE(l.split_from_write().has_value());
    EXPECT_EQ(l.size(), 1u);
    EXPECT_EQ(l.front(), l.back());
    EXPECT_EQ(l.back(), l.write());
  }

  // 2. sliceList's size == 2, writeSlice's index is 0
  {
    SliceList l;
    l.push_back(make_test_slice());
    l.push_back(make_test_slice());
    l.set_write(l.front());
    l.split_from_write();
    EXPECT_EQ(l.size(), 1u);
    EXPECT_EQ(l.front(), l.back());
    EXPECT_EQ(l.back(), l.write());
  }

  // 2b. sliceList's size == 2, writeSlice's index is 1
  {
    SliceList l;
    l.push_back(make_test_slice());
    l.push_back(make_test_slice());
    l.set_write(l.back());
    ASSERT_FALSE(l.split_from_write().has_value());
    EXPECT_EQ(l.size(), 2u);
    EXPECT_EQ(l.back(), l.write());
  }

  // 3. sliceList's size == 100, writeSlice's index is 50
  {
    SliceList l;
    for (std::size_t i = 0; i < 100; ++i) {
      l.push_back(make_test_slice());
      if (i == 50) {
        l.set_write(l.back());
      }
    }
    l.split_from_write();
    EXPECT_EQ(l.back(), l.write());
    EXPECT_EQ(l.size(), 51u);
  }
}
