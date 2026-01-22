// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "generators/lineitem_row_generator.h"

#include <algorithm>
#include <map>
#include <mutex>
#include <vector>

#include "utils/constants.h"

namespace benchgen::tpch::internal {
namespace {

constexpr int64_t kLineItemIndexBlockSize = 4096;

struct LineItemIndex {
  int64_t total_orders = 0;
  int64_t block_size = kLineItemIndexBlockSize;
  std::vector<int64_t> block_prefix;
};

int64_t ScaleKey(double scale_factor) {
  if (scale_factor <= 0.0) {
    return 0;
  }
  if (scale_factor < 1.0) {
    return static_cast<int64_t>(scale_factor * 1000.0);
  }
  return static_cast<int64_t>(scale_factor) * 1000;
}

LineItemIndex BuildLineItemIndex(int64_t total_orders) {
  LineItemIndex index;
  if (total_orders <= 0) {
    return index;
  }
  index.total_orders = total_orders;
  int64_t blocks =
      (total_orders + index.block_size - 1) / index.block_size;
  index.block_prefix.resize(static_cast<size_t>(blocks + 1));
  index.block_prefix[0] = 0;

  RandomState rng;
  rng.Reset();

  int64_t total = 0;
  int64_t order_index = 0;
  for (int64_t block = 0; block < blocks; ++block) {
    int64_t block_sum = 0;
    int64_t count =
        std::min(index.block_size, total_orders - order_index);
    for (int64_t i = 0; i < count; ++i) {
      block_sum += rng.RandomInt(kOLcntMin, kOLcntMax, kOLcntSd);
    }
    total += block_sum;
    index.block_prefix[static_cast<size_t>(block + 1)] = total;
    order_index += count;
  }

  return index;
}

const LineItemIndex& GetLineItemIndex(double scale_factor,
                                      int64_t total_orders) {
  static std::mutex mutex;
  static std::map<int64_t, LineItemIndex> cache;
  int64_t key = ScaleKey(scale_factor);

  std::lock_guard<std::mutex> lock(mutex);
  auto it = cache.find(key);
  if (it != cache.end()) {
    return it->second;
  }
  auto result = cache.emplace(key, BuildLineItemIndex(total_orders));
  return result.first->second;
}

struct LineItemPosition {
  int64_t order_index = 0;
  int64_t offset_in_order = 0;
};

LineItemPosition FindPosition(const LineItemIndex& index,
                              int64_t target_offset) {
  LineItemPosition pos;
  if (index.total_orders <= 0 || index.block_prefix.empty()) {
    return pos;
  }

  auto it = std::upper_bound(index.block_prefix.begin(),
                             index.block_prefix.end(), target_offset);
  int64_t block =
      static_cast<int64_t>(it - index.block_prefix.begin()) - 1;
  if (block < 0) {
    block = 0;
  }
  int64_t block_start_order = block * index.block_size;
  int64_t offset_in_block = target_offset - index.block_prefix[block];

  RandomState rng;
  rng.Reset();
  rng.AdvanceStream(kOLcntSd, block_start_order);

  int64_t running = 0;
  int64_t count =
      std::min(index.block_size, index.total_orders - block_start_order);
  for (int64_t i = 0; i < count; ++i) {
    int64_t line_count = rng.RandomInt(kOLcntMin, kOLcntMax, kOLcntSd);
    if (offset_in_block < running + line_count) {
      pos.order_index = block_start_order + i;
      pos.offset_in_order = offset_in_block - running;
      return pos;
    }
    running += line_count;
  }

  pos.order_index = index.total_orders;
  pos.offset_in_order = 0;
  return pos;
}

int64_t CurrentOffset(const LineItemIndex& index, int64_t order_index,
                      int32_t line_index) {
  if (index.total_orders <= 0 || index.block_prefix.empty()) {
    return 0;
  }
  if (order_index <= 0) {
    return line_index;
  }
  int64_t block = order_index / index.block_size;
  int64_t in_block = order_index % index.block_size;
  int64_t offset = index.block_prefix[block];
  if (in_block == 0) {
    return offset + line_index;
  }

  RandomState rng;
  rng.Reset();
  rng.AdvanceStream(kOLcntSd, block * index.block_size);
  for (int64_t i = 0; i < in_block; ++i) {
    offset += rng.RandomInt(kOLcntMin, kOLcntMax, kOLcntSd);
  }
  return offset + line_index;
}

}  // namespace

LineItemRowGenerator::LineItemRowGenerator(double scale_factor,
                                           DbgenSeedMode seed_mode)
    : scale_factor_(scale_factor),
      order_generator_(scale_factor, seed_mode) {}

arrow::Status LineItemRowGenerator::Init() {
  auto status = order_generator_.Init();
  if (!status.ok()) {
    return status;
  }
  total_orders_ = order_generator_.total_rows();
  current_order_index_ = 1;
  current_line_index_ = 0;
  has_order_ = false;
  return arrow::Status::OK();
}

void LineItemRowGenerator::SkipRows(int64_t rows) {
  if (rows <= 0) {
    return;
  }
  if (current_order_index_ > total_orders_) {
    return;
  }

  const LineItemIndex& index =
      GetLineItemIndex(scale_factor_, total_orders_);
  if (index.total_orders <= 0 || index.block_prefix.empty() ||
      rows < kLineItemIndexBlockSize) {
    while (rows > 0 && current_order_index_ <= total_orders_) {
      if (has_order_) {
        int64_t remaining = static_cast<int64_t>(current_order_.line_count) -
                            current_line_index_;
        if (rows < remaining) {
          current_line_index_ += static_cast<int32_t>(rows);
          return;
        }
        rows -= remaining;
        has_order_ = false;
        current_order_index_++;
        current_line_index_ = 0;
        continue;
      }

      int64_t line_count = order_generator_.PeekLineCount();
      if (rows < line_count) {
        order_generator_.GenerateRow(current_order_index_, &current_order_);
        has_order_ = true;
        current_line_index_ = static_cast<int32_t>(rows);
        return;
      }

      order_generator_.SkipRows(1);
      rows -= line_count;
      current_order_index_++;
    }
    return;
  }

  int64_t current_order_zero_based = current_order_index_ - 1;
  int64_t current_offset =
      CurrentOffset(index, current_order_zero_based,
                    has_order_ ? current_line_index_ : 0);
  int64_t target_offset = current_offset + rows;
  if (target_offset >= index.block_prefix.back()) {
    current_order_index_ = total_orders_ + 1;
    current_line_index_ = 0;
    has_order_ = false;
    return;
  }

  LineItemPosition pos = FindPosition(index, target_offset);
  int64_t target_order_index = pos.order_index + 1;
  if (has_order_ && target_order_index == current_order_index_) {
    current_line_index_ = static_cast<int32_t>(pos.offset_in_order);
    return;
  }

  if (has_order_) {
    has_order_ = false;
    current_line_index_ = 0;
    current_order_index_++;
  }

  int64_t orders_to_skip = target_order_index - current_order_index_;
  if (orders_to_skip > 0) {
    order_generator_.SkipRows(orders_to_skip);
    current_order_index_ = target_order_index;
  }

  if (pos.offset_in_order > 0) {
    order_generator_.GenerateRow(current_order_index_, &current_order_);
    has_order_ = true;
    current_line_index_ = static_cast<int32_t>(pos.offset_in_order);
  } else {
    has_order_ = false;
    current_line_index_ = 0;
  }
}

bool LineItemRowGenerator::NextRow(LineItemRow* out) {
  if (!out) {
    return false;
  }
  while (current_order_index_ <= total_orders_) {
    if (!has_order_) {
      order_generator_.GenerateRow(current_order_index_, &current_order_);
      has_order_ = true;
      current_line_index_ = 0;
    }
    if (current_line_index_ < current_order_.line_count) {
      *out =
          current_order_.lines[static_cast<std::size_t>(current_line_index_)];
      ++current_line_index_;
      return true;
    }
    has_order_ = false;
    current_order_index_++;
    current_line_index_ = 0;
  }
  return false;
}

}  // namespace benchgen::tpch::internal
