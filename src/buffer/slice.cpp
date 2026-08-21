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

#include "shmipc/buffer/slice.hpp"

#include <algorithm>
#include <cassert>
#include <cstring>

namespace shmipc::buffer {

namespace {

std::uint32_t load_u32(const std::uint8_t* p) {
  std::uint32_t v;
  std::memcpy(&v, p, sizeof(v));
  return v;
}

void store_u32(std::uint8_t* p, std::uint32_t v) {
  std::memcpy(p, &v, sizeof(v));
}

}  // namespace

// ---------------------------------------------------------------------------
// BufferHeader
// ---------------------------------------------------------------------------

std::uint32_t BufferHeader::next_buffer_offset() const noexcept {
  return load_u32(p_ + kNextBufferOffset);
}

bool BufferHeader::has_next() const noexcept {
  return (p_[kBufferFlagOffset] & kHasNextBufferFlag) > 0;
}

void BufferHeader::clear_flag() const noexcept { p_[kBufferFlagOffset] = 0; }

void BufferHeader::set_in_used() const noexcept {
  p_[kBufferFlagOffset] |= kSliceInUsedFlag;
}

bool BufferHeader::is_in_used() const noexcept {
  return (p_[kBufferFlagOffset] & kSliceInUsedFlag) > 0;
}

void BufferHeader::link_next(std::uint32_t next) const noexcept {
  store_u32(p_ + kNextBufferOffset, next);
  p_[kBufferFlagOffset] |= kHasNextBufferFlag;
}

std::uint32_t BufferHeader::cap() const noexcept {
  return load_u32(p_ + kBufferCapOffset);
}

std::uint32_t BufferHeader::size() const noexcept {
  return load_u32(p_ + kBufferSizeOffset);
}

void BufferHeader::set_size(std::uint32_t size) const noexcept {
  store_u32(p_ + kBufferSizeOffset, size);
}

std::uint32_t BufferHeader::start() const noexcept {
  return load_u32(p_ + kBufferDataStartOffset);
}

void BufferHeader::set_start(std::uint32_t start) const noexcept {
  store_u32(p_ + kBufferDataStartOffset, start);
}

// ---------------------------------------------------------------------------
// BufferSlice
// ---------------------------------------------------------------------------

BufferSlice::BufferSlice(std::optional<BufferHeader> header,
                         std::span<std::uint8_t> data,
                         std::uint32_t offset_in_shm, bool is_from_shm)
    : offset_in_shm(offset_in_shm), is_from_shm(is_from_shm) {
  assert(!data.empty());
  this->data = data.data();
  const auto len = static_cast<std::uint32_t>(data.size());
  if (is_from_shm && header.has_value()) {
    cap = header->cap();
    start = header->start();
    read_index = start;
    write_index = start + header->size();
    buffer_header = header;
  } else {
    cap = len;
  }
}

BufferSlice BufferSlice::from_heap(std::vector<std::uint8_t> storage) {
  BufferSlice s;
  s.heap_storage_ =
      std::make_shared<std::vector<std::uint8_t>>(std::move(storage));
  s.data = s.heap_storage_->data();
  s.cap = static_cast<std::uint32_t>(s.heap_storage_->size());
  s.is_from_shm = false;
  return s;
}

void BufferSlice::update() const {
  if (!buffer_header.has_value()) {
    return;
  }
  buffer_header->set_size(static_cast<std::uint32_t>(size()));
  buffer_header->set_start(start);

  if (next_slice_ == nullptr) {
    return;
  }
  buffer_header->link_next(next_slice_->offset_in_shm);
}

void BufferSlice::reset() {
  if (buffer_header.has_value()) {
    buffer_header->set_size(0);
    buffer_header->set_start(0);
    buffer_header->clear_flag();
  }
  start = 0;
  write_index = 0;
  read_index = 0;
  next_slice_ = nullptr;
}

Result<std::span<std::uint8_t>> BufferSlice::reserve(std::size_t size) {
  const std::size_t start_pos = write_index;
  if (remain() >= size) {
    write_index += size;
    return std::span<std::uint8_t>(data + start_pos, size);
  }
  return std::unexpected(Err::kNoMoreBuffer);
}

std::size_t BufferSlice::append(std::span<const std::uint8_t> bytes) {
  if (bytes.empty()) {
    return 0;
  }
  const std::size_t copy_size = std::min(bytes.size(), remain());
  std::memcpy(data + write_index, bytes.data(), copy_size);
  write_index += copy_size;
  return copy_size;
}

std::span<const std::uint8_t> BufferSlice::read(std::size_t size) {
  size = std::min(size, this->size());
  std::span<const std::uint8_t> out(data + read_index, size);
  read_index += size;
  return out;
}

std::span<const std::uint8_t> BufferSlice::peek(std::size_t size) const {
  size = std::min(size, this->size());
  return {data + read_index, size};
}

std::size_t BufferSlice::skip(std::size_t size) {
  const std::size_t un_read = this->size();
  if (un_read > size) {
    read_index += size;
    return size;
  }
  read_index += un_read;
  return un_read;
}

bool operator==(const BufferSlice& lhs, const BufferSlice& rhs) noexcept {
  return lhs.buffer_header == rhs.buffer_header && lhs.data == rhs.data &&
         lhs.cap == rhs.cap && lhs.start == rhs.start &&
         lhs.offset_in_shm == rhs.offset_in_shm &&
         lhs.read_index == rhs.read_index &&
         lhs.write_index == rhs.write_index &&
         lhs.is_from_shm == rhs.is_from_shm &&
         lhs.next() == rhs.next();
}

// ---------------------------------------------------------------------------
// SliceList
// ---------------------------------------------------------------------------

void SliceList::push_back(BufferSlice s) {
  assert(s.next() == nullptr);
  auto node = std::make_unique<BufferSlice>(std::move(s));
  BufferSlice* raw = node.get();
  if (back_ != nullptr) {
    assert(len_ != 0);
    back_->set_next(std::move(node));
  } else {
    assert(len_ == 0);
    front_owner_ = std::move(node);
    front_ = raw;
  }
  back_ = raw;
  ++len_;
}

std::optional<BufferSlice> SliceList::pop_front() {
  if (front_ == nullptr) {
    assert(len_ == 0);
    return std::nullopt;
  }
  assert(len_ != 0);
  --len_;
  std::unique_ptr<BufferSlice> node = std::move(front_owner_);
  front_owner_ = node->take_next();
  front_ = front_owner_.get();
  node->set_next(nullptr);

  if (len_ == 0) {
    assert(front_ == nullptr);
    back_ = nullptr;
  }
  return std::optional<BufferSlice>(std::move(*node));
}

std::optional<BufferSlice> SliceList::split_from_write() {
  if (write_ == nullptr) {
    return std::nullopt;
  }
  std::unique_ptr<BufferSlice> next_chain = write_->take_next();
  back_ = write_;
  std::size_t next_list_size = 0;
  for (const BufferSlice* s = next_chain.get(); s != nullptr; s = s->next()) {
    ++next_list_size;
  }
  len_ -= next_list_size;
  if (next_chain == nullptr) {
    return std::nullopt;
  }
  return std::optional<BufferSlice>(std::move(*next_chain));
}

}  // namespace shmipc::buffer
