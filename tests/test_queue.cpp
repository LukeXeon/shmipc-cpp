// C++ translation of shmipc-rs src/queue.rs tests.

#include <gtest/gtest.h>

#include <unistd.h>

#include <chrono>
#include <memory>
#include <thread>
#include <vector>

#include "shmipc/consts.hpp"
#include "shmipc/queue.hpp"

using namespace shmipc;

namespace {

// Mirrors Rust test helper create_queue(cap): a queue over heap memory.
struct HeapQueue {
  std::unique_ptr<std::vector<std::uint8_t>> mem;
  Queue queue;
};

HeapQueue create_queue(std::uint32_t cap) {
  const std::size_t mem_size = count_queue_mem_size(cap);
  HeapQueue hq;
  hq.mem = std::make_unique<std::vector<std::uint8_t>>(mem_size, 0);
  hq.queue = Queue::create_from_bytes(hq.mem->data(), cap);
  return hq;
}

}  // namespace

TEST(QueueManager, CreateMapping) {
  const std::string path = "/tmp/ipc1.queue";
  ::unlink(path.c_str());

  auto qm1 = QueueManager::create_with_file(path, 8192);
  ASSERT_TRUE(qm1.has_value());
  auto qm2 = QueueManager::mapping_with_file(path);
  ASSERT_TRUE(qm2.has_value());

  ASSERT_TRUE(qm1->send_queue.put({0, 0, 0}).has_value());
  ASSERT_TRUE(qm2->recv_queue.pop().has_value());

  ASSERT_TRUE(qm2->send_queue.put({0, 0, 0}).has_value());
  ASSERT_TRUE(qm1->recv_queue.pop().has_value());
  qm1->unmap();
}

TEST(QueueManager, CreateMappingMemfd) {
  auto qm1 = QueueManager::create_with_memfd("/tmp/ipc1.memfd.queue", 8192);
  ASSERT_TRUE(qm1.has_value());
  auto qm2 = QueueManager::mapping_with_memfd("/tmp/ipc1.memfd.queue",
                                              qm1->memfd());
  ASSERT_TRUE(qm2.has_value());

  ASSERT_TRUE(qm1->send_queue.put({1, 2, 3}).has_value());
  auto ele = qm2->recv_queue.pop();
  ASSERT_TRUE(ele.has_value());
  EXPECT_EQ(ele->seq_id, 1u);
  EXPECT_EQ(ele->offset_in_shm_buf, 2u);
  EXPECT_EQ(ele->status, 3u);

  ASSERT_TRUE(qm2->send_queue.put({4, 5, 6}).has_value());
  ele = qm1->recv_queue.pop();
  ASSERT_TRUE(ele.has_value());
  EXPECT_EQ(ele->seq_id, 4u);
  qm1->unmap();
}

TEST(Queue, Operate) {
  HeapQueue hq = create_queue(8192);
  Queue& q = hq.queue;

  ASSERT_TRUE(q.is_empty());
  ASSERT_FALSE(q.is_full());
  ASSERT_EQ(q.size(), 0);

  std::int64_t put_count = 0;
  for (std::uint32_t i = 0; i < 8192; ++i) {
    ASSERT_TRUE(q.put({i, i, i}).has_value());
    ++put_count;
  }
  auto r_full = q.put({1, 1, 1});
  ASSERT_FALSE(r_full.has_value());
  EXPECT_EQ(r_full.error(), Err::kQueueFull);
  ASSERT_TRUE(q.is_full());
  ASSERT_FALSE(q.is_empty());
  ASSERT_EQ(q.size(), put_count);

  for (std::uint32_t i = 0; i < 8192; ++i) {
    auto e = q.pop();
    ASSERT_TRUE(e.has_value());
    EXPECT_EQ(e->seq_id, i);
    EXPECT_EQ(e->offset_in_shm_buf, i);
    EXPECT_EQ(e->status, i);
  }

  auto r_empty = q.pop();
  ASSERT_FALSE(r_empty.has_value());
  EXPECT_EQ(r_empty.error(), Err::kQueueEmpty);
  ASSERT_TRUE(q.is_empty());
  ASSERT_FALSE(q.is_full());
  ASSERT_EQ(q.size(), 0);

  ASSERT_FALSE(q.consumer_is_working());
  q.mark_working();
  ASSERT_TRUE(q.consumer_is_working());
  q.mark_not_working();
  ASSERT_FALSE(q.consumer_is_working());

  (void)q.put({1, 1, 1});
  q.mark_not_working();
  ASSERT_TRUE(q.consumer_is_working());
}

TEST(Queue, MultiProducerAndSingleConsumer) {
  auto holder = std::make_shared<HeapQueue>(create_queue(100000));
  std::vector<std::thread> producers;
  for (int t = 0; t < 100; ++t) {
    producers.emplace_back([holder] {
      for (int i = 0; i < 1000; ++i) {
        auto r = holder->queue.put({1, 1, 1});
        EXPECT_TRUE(r.has_value());
      }
    });
  }

  std::int64_t pop_count = 0;
  while (pop_count != 100000) {
    if (holder->queue.pop().has_value()) {
      ++pop_count;
    } else {
      std::this_thread::sleep_for(std::chrono::microseconds(1));
    }
  }
  for (auto& p : producers) {
    p.join();
  }
}
