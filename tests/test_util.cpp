// C++ translation of shmipc-rs src/util tests (mod.rs). The BufReader and
// ShmBufReader tests are added in the util phase once those files exist.

#include <gtest/gtest.h>

#include <sys/statvfs.h>
#include <unistd.h>

#include <array>
#include <vector>

#include "shmipc/bytes.hpp"
#include "shmipc/util/buf_reader.hpp"
#include "shmipc/util/dev_shm.hpp"
#include "shmipc/util/shmbuf_reader.hpp"

using namespace shmipc;

TEST(Util, CanCreateOnDevShm) {
  // Just on /dev/shm; other paths always return true.
  ASSERT_TRUE(can_create_on_dev_shm(UINT64_MAX, "sdffafds"));

  struct statvfs st {};
  ASSERT_EQ(::statvfs("/dev/shm", &st), 0);
  const std::uint64_t free_bytes =
      static_cast<std::uint64_t>(st.f_bavail) *
      static_cast<std::uint64_t>(st.f_frsize);
  ASSERT_TRUE(can_create_on_dev_shm(free_bytes, "/dev/shm/xxx"));
  ASSERT_FALSE(can_create_on_dev_shm(free_bytes + 1, "/dev/shm/yyy"));
}

// Mirrors `test_compact` from util/buf_reader.rs, driven through the fd API.
TEST(BufReader, Compact) {
  int fds[2];
  ASSERT_EQ(::pipe(fds), 0);
  std::vector<std::uint8_t> data = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  ASSERT_EQ(::write(fds[1], data.data(), data.size()),
            static_cast<ssize_t>(data.size()));

  BufReader reader(fds[0], 16);
  auto filled = reader.fill_buf_at_least(10);
  ASSERT_TRUE(filled.has_value());
  ASSERT_EQ(filled->size(), 10u);

  // Compact with nothing consumed keeps all data.
  reader.compact();
  ASSERT_EQ(reader.buffer().size(), 10u);
  EXPECT_EQ(reader.buffer()[0], 1);
  EXPECT_EQ(reader.buffer()[4], 5);

  // Consume 5 bytes, compact moves [6..10] to the front.
  reader.consume(5);
  reader.compact();
  ASSERT_EQ(reader.buffer().size(), 5u);
  EXPECT_EQ(reader.buffer()[0], 6);
  EXPECT_EQ(reader.buffer()[4], 10);

  ::close(fds[0]);
  ::close(fds[1]);
}

TEST(BufReader, FillBufAtLeastReadsAcrossCalls) {
  int fds[2];
  ASSERT_EQ(::pipe(fds), 0);
  std::vector<std::uint8_t> data(64);
  for (std::size_t i = 0; i < data.size(); ++i) {
    data[i] = static_cast<std::uint8_t>(i);
  }
  ASSERT_EQ(::write(fds[1], data.data(), 30), 30);
  ASSERT_EQ(::write(fds[1], data.data() + 30, 34), 34);

  BufReader reader(fds[0], 128);
  auto filled = reader.fill_buf_at_least(64);
  ASSERT_TRUE(filled.has_value());
  ASSERT_EQ(filled->size(), 64u);
  for (std::size_t i = 0; i < 64; ++i) {
    ASSERT_EQ((*filled)[i], static_cast<std::uint8_t>(i));
  }
  ::close(fds[0]);
  ::close(fds[1]);
}

namespace {
buffer::Buf shm_buf(std::size_t size) {
  std::vector<std::uint8_t> v(size, 0);
  return buffer::Buf::exm(Bytes::from_vec(std::move(v)));
}
}  // namespace

TEST(ShmBufReader, Nothing) {
  std::array<std::uint8_t, 64> buf{};
  ShmBufReader reader(shm_buf(0));
  std::size_t filled = 0;
  ASSERT_TRUE(reader.read(buf, filled));
  EXPECT_EQ(filled, 0u);
  EXPECT_EQ(reader.consumed(), 0u);
}

TEST(ShmBufReader, ReadOnce) {
  constexpr std::size_t kBufLen = 64;
  constexpr std::size_t kReadSize = 16;
  std::array<std::uint8_t, kBufLen> buf{};
  ShmBufReader reader(shm_buf(kReadSize));
  std::size_t filled = 0;
  ASSERT_TRUE(reader.read(buf, filled));
  EXPECT_EQ(filled, kReadSize);
  EXPECT_EQ(reader.consumed(), kReadSize);
}

TEST(ShmBufReader, ReadTwice) {
  constexpr std::size_t kBufLen = 64;
  constexpr std::size_t kReadSize = 16;
  constexpr std::size_t kDataLen = kBufLen + kReadSize;

  std::array<std::uint8_t, kBufLen> buf{};
  ShmBufReader reader(shm_buf(80));
  std::size_t filled = 0;
  ASSERT_FALSE(reader.read(buf, filled));
  EXPECT_EQ(filled, kBufLen);
  EXPECT_EQ(reader.consumed(), kBufLen);
  ASSERT_TRUE(reader.read(buf, filled));
  EXPECT_EQ(filled, kReadSize);
  EXPECT_EQ(reader.consumed(), kDataLen);
}
