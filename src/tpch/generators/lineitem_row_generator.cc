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

namespace benchgen::tpch::internal {

LineItemRowGenerator::LineItemRowGenerator(double scale_factor,
                                           DbgenSeedMode seed_mode)
    : order_generator_(scale_factor, seed_mode) {}

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
  while (rows > 0 && current_order_index_ <= total_orders_) {
    if (!has_order_) {
      order_generator_.GenerateRow(current_order_index_, &current_order_);
      has_order_ = true;
      current_line_index_ = 0;
    }
    int64_t remaining =
        static_cast<int64_t>(current_order_.line_count) - current_line_index_;
    if (rows < remaining) {
      current_line_index_ += static_cast<int32_t>(rows);
      return;
    }
    rows -= remaining;
    has_order_ = false;
    current_order_index_++;
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
