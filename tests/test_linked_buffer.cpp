// C++ translation of shmipc-rs src/buffer/linked.rs tests.

#include <gtest/gtest.h>

#include <cstring>
#include <filesystem>
#include <memory>
#include <random>
#include <span>
#include <string>
#include <vector>

#include "shmipc/buffer/linked.hpp"
#include "shmipc/buffer/manager.hpp"
#include "shmipc/config.hpp"
#include "shmipc/consts.hpp"

using namespace shmipc;
using namespace shmipc::buffer;

namespace {

std::shared_ptr<BufferManager> init_shm() {
  const char* shm_path = "/tmp/ipc.test";
  std::error_code ec;
  std::filesystem::remove(shm_path, ec);
  const std::size_t shm_size = 10 << 20;
  auto mem = map_anonymous(shm_size);
  EXPECT_TRUE(mem.has_value());
  auto bm = BufferManager::create(
      std::vector<SizePercentPair>{
          {4096, 70},
          {16 * 1024, 20},
          {64 * 1024, 10},
      },
      shm_path, std::move(*mem), 0);
  EXPECT_TRUE(bm.has_value());
  return std::make_shared<BufferManager>(std::move(*bm));
}

LinkedBuffer new_linked_buffer_with_slice(
    std::shared_ptr<BufferManager> manager, BufferSlice slice) {
  LinkedBuffer l(std::move(manager));
  l.slice_list_mut().push_back(std::move(slice));
  l.slice_list_mut().set_write(l.slice_list_mut().back());
  return l;
}

LinkedBuffer new_linked_buffer(std::shared_ptr<BufferManager> manager,
                               std::uint32_t size) {
  LinkedBuffer l(std::move(manager));
  l.alloc(size);
  l.slice_list_mut().set_write(l.slice_list_mut().front());
  return l;
}

}  // namespace

TEST(LinkedBuffer, ReleasePreviousRead) {
  auto bm = init_shm();
  auto slice = bm->alloc_shm_buffer(1024);
  ASSERT_TRUE(slice.has_value());
  LinkedBuffer buf = new_linked_buffer_with_slice(bm, std::move(*slice));
  const int slice_num = 100;
  for (int i = 0; i < slice_num * 4096; ++i) {
    const std::uint8_t b = static_cast<std::uint8_t>(i);
    auto r = buf.write_bytes(std::span(&b, 1));
    ASSERT_TRUE(r.has_value());
    ASSERT_EQ(*r, 1u);
  }
  buf.done(true);

  for (int i = 0; i < slice_num / 2; ++i) {
    auto r = buf.read_bytes(4096);
    ASSERT_TRUE(r.has_value());
    ASSERT_EQ(r->size(), 4096u);
  }
  ASSERT_EQ(static_cast<std::size_t>(slice_num / 2 - 1), buf.pinned_list_size());
  auto d = buf.discard(buf.len());
  ASSERT_TRUE(d.has_value());

  buf.release_previous_read_and_reserve();
  EXPECT_EQ(buf.pinned_list_size(), 0u);
  EXPECT_EQ(buf.len(), 0u);
  // the last slice shouldn't be released
  ASSERT_EQ(buf.slice_list().size(), 1u);
  ASSERT_NE(buf.slice_list().write(), nullptr);

  buf.release_previous_read();
  ASSERT_EQ(buf.slice_list().size(), 0u);
  ASSERT_EQ(buf.slice_list().write(), nullptr);
}

TEST(LinkedBuffer, FallbackWhenWrite) {
  auto mem = map_anonymous(10 * 1024);
  ASSERT_TRUE(mem.has_value());
  auto bm = std::make_shared<BufferManager>(
      std::move(*BufferManager::create(
          std::vector<SizePercentPair>{{1024, 100}}, "", std::move(*mem), 0)));

  auto buf = bm->alloc_shm_buffer(1024);
  ASSERT_TRUE(buf.has_value());
  LinkedBuffer writer = new_linked_buffer_with_slice(bm, std::move(*buf));
  const std::size_t data_size = 1024;
  std::mt19937_64 rng(123);
  std::vector<std::vector<std::uint8_t>> mock_data_array(
      100, std::vector<std::uint8_t>(data_size));
  for (auto& arr : mock_data_array) {
    for (auto& b : arr) {
      b = static_cast<std::uint8_t>(rng());
    }
  }
  for (std::size_t i = 0; i < mock_data_array.size(); ++i) {
    auto n = writer.write_bytes(mock_data_array[i]);
    ASSERT_TRUE(n.has_value());
    ASSERT_EQ(*n, data_size);
    EXPECT_EQ(writer.len(), data_size * (i + 1));
  }
  EXPECT_FALSE(writer.is_from_share_memory());

  writer.done(false);
  const std::size_t all = data_size * mock_data_array.size();
  ASSERT_EQ(writer.len(), all);

  for (std::size_t i = 0; i < mock_data_array.size(); ++i) {
    EXPECT_EQ(writer.len(), all - i * data_size);
    auto get = writer.read_bytes(data_size);
    ASSERT_TRUE(get.has_value());
    ASSERT_EQ(get->size(), data_size);
    EXPECT_TRUE(std::equal(mock_data_array[i].begin(), mock_data_array[i].end(),
                           get->span().begin()));
  }
}

TEST(LinkedBuffer, Reserve) {
  auto bm = init_shm();

  // alloc 3 buffer slices
  LinkedBuffer buffer = new_linked_buffer(bm, (64 + 64 + 64) * 1024);
  ASSERT_EQ(buffer.slice_list().size(), 3u);
  ASSERT_TRUE(buffer.is_from_share_memory());
  ASSERT_EQ(buffer.slice_list().front(), buffer.slice_list().write());

  // reserve a buf in the first slice
  auto ret = buffer.reserve(60 * 1024);
  ASSERT_TRUE(ret.has_value());
  ASSERT_EQ(ret->size(), 60u * 1024);
  ASSERT_EQ(buffer.slice_list().size(), 3u);
  ASSERT_TRUE(buffer.is_from_share_memory());
  ASSERT_EQ(buffer.slice_list().front(), buffer.slice_list().write());

  // reserve a buf in the second slice when the first one is not enough
  ret = buffer.reserve(6 * 1024);
  ASSERT_TRUE(ret.has_value());
  ASSERT_EQ(ret->size(), 6u * 1024);
  ASSERT_EQ(buffer.slice_list().size(), 3u);
  ASSERT_TRUE(buffer.is_from_share_memory());
  ASSERT_EQ(buffer.slice_list().front()->next(), buffer.slice_list().write());

  // reserve a buf in a newly allocated slice
  ret = buffer.reserve(128 * 1024);
  ASSERT_TRUE(ret.has_value());
  ASSERT_EQ(ret->size(), 128u * 1024);
  ASSERT_EQ(buffer.slice_list().size(), 4u);
  ASSERT_FALSE(buffer.is_from_share_memory());
  ASSERT_EQ(buffer.slice_list().back(), buffer.slice_list().write());
}

TEST(LinkedBuffer, Done) {
  auto bm = init_shm();
  const std::size_t mock_data_size = 128 * 1024;
  std::mt19937_64 rng(9);
  std::vector<std::uint8_t> mock_data(mock_data_size);
  for (auto& b : mock_data) {
    b = static_cast<std::uint8_t>(rng());
  }
  // alloc 3 buffer slices
  LinkedBuffer buffer = new_linked_buffer(bm, (64 + 64 + 64) * 1024);
  ASSERT_EQ(buffer.slice_list().size(), 3u);

  // write data to fill 2 slices, remove one
  auto w = buffer.write_bytes(mock_data);
  ASSERT_TRUE(w.has_value());
  buffer.done(true);
  ASSERT_EQ(buffer.slice_list().size(), 2u);
  auto get_bytes = buffer.read_bytes(mock_data_size);
  ASSERT_TRUE(get_bytes.has_value());
  ASSERT_EQ(get_bytes->size(), mock_data_size);
  EXPECT_TRUE(std::equal(mock_data.begin(), mock_data.end(),
                         get_bytes->span().begin()));
}

TEST(LinkedBuffer, ReadBytes) {
  auto manager = init_shm();

  const auto create_buffer_writer = [&] {
    auto buf = manager->alloc_shm_buffer(1024);
    EXPECT_TRUE(buf.has_value());
    return new_linked_buffer_with_slice(manager, std::move(*buf));
  };

  std::mt19937_64 rng(55);
  const auto write_and_read = [&](LinkedBuffer buf) {
    const std::size_t size = 1 << 21;
    std::vector<std::uint8_t> data(size, 0);
    while (buf.len() < size) {
      std::size_t one_write_size =
          static_cast<std::size_t>(rng() % (size / 10));
      if (buf.len() + one_write_size > size) {
        one_write_size = size - buf.len();
      }
      auto n = buf.write_bytes(
          std::span(data.data() + buf.len(), one_write_size));
      ASSERT_TRUE(n.has_value());
      ASSERT_EQ(*n, one_write_size);
    }
    buf.done(false);
    std::size_t read = 0;
    while (!buf.is_empty()) {
      std::size_t one_read_size =
          static_cast<std::size_t>(rng() % (size / 10000));
      if (read + one_read_size > buf.len()) {
        one_read_size = buf.len();
      }
      // do nothing
      (void)buf.peek(one_read_size);

      auto read_data = buf.read_bytes(one_read_size);
      ASSERT_TRUE(read_data.has_value());
      if (read_data->empty()) {
        ASSERT_EQ(one_read_size, 0u);
      } else {
        ASSERT_EQ(read_data->size(), one_read_size);
        EXPECT_TRUE(std::equal(data.begin() + read,
                               data.begin() + read + one_read_size,
                               read_data->span().begin()));
      }
      read += one_read_size;
    }
    ASSERT_EQ(read, static_cast<std::size_t>(1 << 21));
    ASSERT_TRUE(buf.read_bytes(0).has_value());
    buf.release_previous_read();
  };

  for (int i = 0; i < 100; ++i) {
    write_and_read(create_buffer_writer());
  }
}

TEST(LinkedBuffer, Discard) {
  auto manager = init_shm();

  const auto create_buffer_writer = [&] {
    auto buf = manager->alloc_shm_buffer(1024);
    EXPECT_TRUE(buf.has_value());
    return new_linked_buffer_with_slice(manager, std::move(*buf));
  };

  {
    LinkedBuffer writer = create_buffer_writer();
    const std::size_t capacity = writer.cap();
    std::vector<std::uint8_t> zeros(capacity, 0);
    auto w = writer.write_bytes(zeros);
    ASSERT_TRUE(w.has_value());
    auto n = writer.discard(capacity);
    ASSERT_TRUE(n.has_value());
    ASSERT_EQ(*n, capacity);
    EXPECT_EQ(writer.len(), 0u);
  }

  {
    LinkedBuffer writer = create_buffer_writer();
    const std::size_t origin_cap = writer.cap();
    std::vector<std::uint8_t> zeros(origin_cap, 0);
    ASSERT_TRUE(writer.write_bytes(zeros).has_value());
    std::vector<std::uint8_t> more(1024, 0);
    ASSERT_TRUE(writer.write_bytes(more).has_value());

    auto n = writer.discard(origin_cap);
    ASSERT_TRUE(n.has_value());
    ASSERT_EQ(*n, origin_cap);

    n = writer.discard(1024);
    ASSERT_TRUE(n.has_value());
    ASSERT_EQ(*n, 1024u);
  }
}

TEST(LinkedBuffer, ReadWrite) {
  auto manager = init_shm();

  const auto create_buffer_writer = [&] {
    auto buf = manager->alloc_shm_buffer(1024);
    EXPECT_TRUE(buf.has_value());
    return new_linked_buffer_with_slice(manager, std::move(*buf));
  };

  const std::string str = "hello";
  const std::span<const std::uint8_t> str_span(
      reinterpret_cast<const std::uint8_t*>(str.data()), str.size());
  {
    LinkedBuffer writer = create_buffer_writer();
    ASSERT_TRUE(writer.write_bytes(str_span).has_value());
    ASSERT_TRUE(writer.write_bytes(str_span).has_value());

    writer.done(false);

    auto get_str = writer.read_bytes(str.size());
    ASSERT_TRUE(get_str.has_value());
    ASSERT_EQ(get_str->size(), str.size());
    EXPECT_EQ(std::string(get_str->span().begin(), get_str->span().end()), str);

    auto get_bytes = writer.read_bytes(str.size());
    ASSERT_TRUE(get_bytes.has_value());
    EXPECT_TRUE(std::equal(str.begin(), str.end(), get_bytes->span().begin()));
  }

  {
    LinkedBuffer writer = create_buffer_writer();

    constexpr std::size_t kOneMsgSize = 1024;
    constexpr std::size_t kMsgNum = 10;
    std::vector<std::uint8_t> result(kOneMsgSize * kMsgNum, 0);
    std::mt19937_64 rng(31);
    for (std::size_t i = 0; i < kMsgNum; ++i) {
      std::vector<std::uint8_t> data(kOneMsgSize);
      for (auto& b : data) {
        b = static_cast<std::uint8_t>(rng());
      }
      std::memcpy(result.data() + i * kOneMsgSize, data.data(), data.size());
      auto n = writer.write_bytes(data);
      ASSERT_TRUE(n.has_value());
      ASSERT_EQ(*n, kOneMsgSize);
    }
    ASSERT_EQ(writer.len(), kOneMsgSize * kMsgNum);

    writer.done(false);
    ASSERT_EQ(writer.len(), kOneMsgSize * kMsgNum);

    auto peek1 = writer.peek(kOneMsgSize);
    ASSERT_TRUE(peek1.has_value());
    ASSERT_EQ(peek1->size(), kOneMsgSize);
    EXPECT_TRUE(std::equal(result.begin(), result.begin() + kOneMsgSize,
                           peek1->span().begin()));
    ASSERT_EQ(writer.len(), kOneMsgSize * kMsgNum);

    // cross two underlying slices
    auto peek2 = writer.peek(5 * kOneMsgSize);
    ASSERT_TRUE(peek2.has_value());
    ASSERT_EQ(peek2->size(), 5 * kOneMsgSize);
    EXPECT_TRUE(std::equal(result.begin(),
                           result.begin() + 5 * kOneMsgSize,
                           peek2->span().begin()));
    ASSERT_EQ(writer.len(), kMsgNum * kOneMsgSize);

    std::size_t remain = writer.len();
    for (std::size_t i = 0; i < kMsgNum; ++i) {
      remain -= kOneMsgSize;
      auto get_data = writer.read_bytes(1024);
      ASSERT_TRUE(get_data.has_value());
      ASSERT_EQ(get_data->size(), kOneMsgSize);
      EXPECT_EQ(writer.len(), remain);
    }
  }

  {
    LinkedBuffer writer = create_buffer_writer();
    for (std::size_t i = 0;
         i < static_cast<std::size_t>(2 * kDefaultSingleBufferSize); ++i) {
      const std::uint8_t b = static_cast<std::uint8_t>(i);
      ASSERT_TRUE(writer.write_bytes(std::span(&b, 1)).has_value());
    }

    writer.done(false);
    std::size_t count = 0;
    const std::size_t read_size = 10;
    while (true) {
      const std::size_t remain_len = writer.len();
      if (remain_len > read_size) {
        auto r = writer.read_bytes(read_size);
        ASSERT_TRUE(r.has_value());
        for (std::size_t j = 0; j < r->size(); ++j) {
          ASSERT_EQ(static_cast<std::uint8_t>(count), r->span()[j]);
          ++count;
        }
      } else if (remain_len > 0) {
        auto r = writer.read_bytes(writer.len());
        ASSERT_TRUE(r.has_value());
        for (std::size_t j = 0; j < r->size(); ++j) {
          ASSERT_EQ(static_cast<std::uint8_t>(count), r->span()[j]);
          ++count;
        }
      } else {
        break;
      }
    }
    ASSERT_EQ(count, static_cast<std::size_t>(2 * kDefaultSingleBufferSize));
  }
}
