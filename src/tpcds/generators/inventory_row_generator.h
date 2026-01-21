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

#include <cstdint>
#include <vector>

#include "distribution/scaling.h"
#include "utils/row_streams.h"

namespace benchgen::tpcds::internal {

struct InventoryRowData {
  int32_t date_sk = 0;
  int64_t item_sk = 0;
  int64_t warehouse_sk = 0;
  int32_t quantity_on_hand = 0;
  int64_t null_bitmap = 0;
};

class InventoryRowGenerator {
 public:
  InventoryRowGenerator(double scale);

  void SkipRows(int64_t start_row);
  InventoryRowData GenerateRow(int64_t row_number);
  void ConsumeRemainingSeedsForRow();

 private:
  static std::vector<int> ColumnIds();

  Scaling scaling_;
  RowStreams streams_;
  int64_t item_count_ = 0;
  int64_t warehouse_count_ = 0;
  int32_t base_julian_ = 0;
};

}  // namespace benchgen::tpcds::internal
