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

#pragma once

#include <arrow/status.h>

#include <cstdint>

#include "benchgen/generator_options.h"
#include "generators/orders_row_generator.h"

namespace benchgen::tpch::internal {

class LineItemRowGenerator {
 public:
  LineItemRowGenerator(double scale_factor, DbgenSeedMode seed_mode);

  arrow::Status Init();
  void SkipRows(int64_t rows);
  bool NextRow(LineItemRow* out);
  int64_t total_orders() const { return total_orders_; }

 private:
  OrdersRowGenerator order_generator_;
  OrderRow current_order_{};
  int64_t total_orders_ = 0;
  int64_t current_order_index_ = 1;
  int32_t current_line_index_ = 0;
  bool has_order_ = false;
};

}  // namespace benchgen::tpch::internal
