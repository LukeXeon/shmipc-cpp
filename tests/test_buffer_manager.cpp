// C++ translation of shmipc-rs src/buffer/manager.rs tests.

#include <gtest/gtest.h>

#include <cstdio>
#include <memory>
#include <random>
#include <vector>

#include "shmipc/buffer/linked.hpp"
#include "shmipc/buffer/manager.hpp"
#include "shmipc/buffer/slice.hpp"
#include "shmipc/config.hpp"

using namespace shmipc;
using namespace shmipc::buffer;

namespace {
std::vector<std::uint8_t> random_bytes(std::size_t n, std::mt19937_64& rng) {
  std::vector<std::uint8_t> data(n);
  for (auto& b : data) {
    b = static_cast<std::uint8_t>(rng());
  }
  return data;
}
}  // namespace

TEST(BufferManager, CreateAndMapping) {
  ::unlink("/tmp/shm");
  std::vector<SizePercentPair> pairs = {
      {4096, 70},
      {16 * 1024, 20},
      {64 * 1024, 10},
  };

  // create
  auto bm1 = BufferManager::get_with_file("/tmp/shm", 32 << 20, true, pairs);
  ASSERT_TRUE(bm1.has_value());

  const auto allocate_func = [](const std::shared_ptr<BufferManager>& bm) {
    for (int i = 0; i < 10; ++i) {
      ASSERT_TRUE(bm->alloc_shm_buffer(4096).has_value());
      ASSERT_TRUE(bm->alloc_shm_buffer(16 * 1024).has_value());
      ASSERT_TRUE(bm->alloc_shm_buffer(64 * 1024).has_value());
    }
  };
  allocate_func(*bm1);

  // mapping (the global registry returns the same manager instance, exactly
  // like the Rust BUFFER_MANAGERS cache)
  auto bm2 = BufferManager::get_with_file("/tmp/shm", 32 << 20, false, pairs);
  ASSERT_TRUE(bm2.has_value());
  for (std::size_t i = 0; i < (*bm1)->lists().size(); ++i) {
    EXPECT_EQ((*bm1)->lists()[i].cap_per_buffer(),
              (*bm2)->lists()[i].cap_per_buffer());
    EXPECT_EQ((*bm1)->lists()[i].size_load(), (*bm2)->lists()[i].size_load());
    EXPECT_EQ((*bm1)->lists()[i].offset_in_shm(),
              (*bm2)->lists()[i].offset_in_shm());
  }

  allocate_func(*bm2);

  for (std::size_t i = 0; i < (*bm1)->lists().size(); ++i) {
    EXPECT_EQ((*bm1)->lists()[i].cap_per_buffer(),
              (*bm2)->lists()[i].cap_per_buffer());
    EXPECT_EQ((*bm1)->lists()[i].size_load(), (*bm2)->lists()[i].size_load());
    EXPECT_EQ((*bm1)->lists()[i].offset_in_shm(),
              (*bm2)->lists()[i].offset_in_shm());
  }
}

TEST(BufferManager, ReadBufferSlice) {
  ::unlink("/tmp/shm1");
  std::vector<SizePercentPair> pairs = {{4096, 100}};
  auto bm = BufferManager::get_with_file("/tmp/shm1", 1 << 20, true, pairs);
  ASSERT_TRUE(bm.has_value());

  auto s = (*bm)->alloc_shm_buffer(4096);
  ASSERT_TRUE(s.has_value());
  std::mt19937_64 rng(7);
  const auto data = random_bytes(4096, rng);
  ASSERT_EQ(4096u, s->append(data));
  ASSERT_EQ(4096u, s->size());
  s->update();

  auto s2 = (*bm)->read_buffer_slice(s->offset_in_shm);
  ASSERT_TRUE(s2.has_value());
  EXPECT_EQ(s->capacity(), s2->capacity());
  EXPECT_EQ(s->size(), s2->size());

  const auto get_data = s2->read(4096);
  ASSERT_EQ(get_data.size(), data.size());
  EXPECT_TRUE(std::equal(data.begin(), data.end(), get_data.begin()));

  auto s3 = (*bm)->read_buffer_slice(s->offset_in_shm + (1 << 20));
  ASSERT_FALSE(s3.has_value());

  auto s4 = (*bm)->read_buffer_slice(s->offset_in_shm + 4096);
  ASSERT_FALSE(s4.has_value());
}

TEST(BufferManager, AllocRecycle) {
  ::unlink("/tmp/shm11");
  std::vector<SizePercentPair> pairs = {
      {4096, 50},
      {8192, 50},
  };
  auto bm_res = BufferManager::get_with_file("/tmp/shm11", 1 << 20, true, pairs);
  ASSERT_TRUE(bm_res.has_value());
  auto bm = *bm_res;

  // The first buffers of each list are consumed by the list headers.
  EXPECT_EQ((1u << 20) - 4096 - 8192, bm->remain_size());

  const std::int64_t num_of_slice = bm->slice_size();
  std::vector<BufferSlice> buffers;
  buffers.reserve(1024);
  while (auto s = bm->alloc_shm_buffer(4096)) {
    buffers.push_back(std::move(*s));
  }
  for (auto& buffer : buffers) {
    bm->recycle_buffer(std::move(buffer));
  }

  // alloc_buffers, recycle_buffers
  SliceList slices;
  const std::int64_t size = bm->alloc_shm_buffers(slices, 256 * 1024);
  ASSERT_EQ(256 * 1024, size);
  LinkedBuffer linked_buffer_slices(bm);
  while (slices.size() > 0) {
    linked_buffer_slices.append_buffer_slice(std::move(*slices.pop_front()));
  }
  linked_buffer_slices.done(false);
  bm->recycle_buffers(
      std::move(*linked_buffer_slices.slice_list_mut().pop_front()));
  EXPECT_EQ(num_of_slice, bm->slice_size());
}

TEST(BufferManager, SkipDuplicateRecycle) {
  auto mem = map_anonymous(1 << 20);
  ASSERT_TRUE(mem.has_value());
  auto bm_res = BufferManager::create(
      std::vector<SizePercentPair>{{4096, 100}}, "", std::move(*mem), 0);
  ASSERT_TRUE(bm_res.has_value());
  auto bm = std::make_shared<BufferManager>(std::move(*bm_res));

  const auto& list = bm->lists()[0];
  const std::int32_t original_size = list.size_load();
  const std::int32_t original_counter = list.counter_load();

  auto slice = bm->alloc_shm_buffer(4096);
  ASSERT_TRUE(slice.has_value());
  const std::uint32_t offset = slice->offset_in_shm;
  auto alias = bm->read_buffer_slice(offset);
  ASSERT_TRUE(alias.has_value());

  EXPECT_EQ(original_size - 1, list.size_load());
  EXPECT_EQ(original_counter + 1, list.counter_load());

  bm->recycle_buffer(std::move(*slice));
  EXPECT_EQ(original_size, list.size_load());
  EXPECT_EQ(original_counter, list.counter_load());

  bm->recycle_buffer(std::move(*alias));
  EXPECT_EQ(original_size, list.size_load());
  EXPECT_EQ(original_counter, list.counter_load());
  EXPECT_TRUE(bm->check_buffer_returned());
}
