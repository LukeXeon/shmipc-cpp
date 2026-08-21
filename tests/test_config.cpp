// C++ translation of shmipc-rs src/config.rs tests.

#include <gtest/gtest.h>

#include "shmipc/config.hpp"

using namespace shmipc;

TEST(Config, VerifyAlignsBufferSliceSizesAndQueueCap) {
  Config config;
  config.queue_cap = 8193;
  config.buffer_slice_sizes = {
      {4097, 50},
      {8193, 50},
  };

  ASSERT_TRUE(config.verify().has_value());

  EXPECT_EQ(8200u, config.queue_cap);
  EXPECT_EQ(4100u, config.buffer_slice_sizes[0].size);
  EXPECT_EQ(8196u, config.buffer_slice_sizes[1].size);
}

TEST(Config, VerifyRejectsTooSmallShareMemory) {
  Config config;
  config.share_memory_buffer_cap = (1u << 20) - 1;
  auto r = config.verify();
  ASSERT_FALSE(r.has_value());
}

TEST(Config, VerifyRejectsPercentSumNot100) {
  Config config;
  config.buffer_slice_sizes = {{4096, 50}};
  auto r = config.verify();
  ASSERT_FALSE(r.has_value());
}

TEST(Config, DefaultMatchesRustDefaults) {
  Config config;
  EXPECT_EQ(8192u, config.queue_cap);
  EXPECT_EQ("/dev/shm/shmipc_queue", config.queue_path);
  EXPECT_EQ(32u * 1024 * 1024, config.share_memory_buffer_cap);
  EXPECT_EQ("/dev/shm/shmipc", config.share_memory_path_prefix);
  EXPECT_EQ(MemMapType::kMemFd, config.mem_map_type);
  EXPECT_EQ(4096u, config.max_stream_num);
  ASSERT_EQ(3u, config.buffer_slice_sizes.size());
  EXPECT_EQ(8192u - 20u, config.buffer_slice_sizes[0].size);
  EXPECT_EQ(50u, config.buffer_slice_sizes[0].percent);
  EXPECT_EQ(32u * 1024 - 20u, config.buffer_slice_sizes[1].size);
  EXPECT_EQ(30u, config.buffer_slice_sizes[1].percent);
  EXPECT_EQ(128u * 1024 - 20u, config.buffer_slice_sizes[2].size);
  EXPECT_EQ(20u, config.buffer_slice_sizes[2].percent);
  EXPECT_TRUE(config.verify().has_value());
}
