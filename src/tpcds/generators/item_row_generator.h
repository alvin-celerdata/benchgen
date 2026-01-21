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
#include <string>
#include <vector>

#include "distribution/dst_distribution_store.h"
#include "distribution/scaling.h"
#include "utils/build_support.h"
#include "utils/decimal.h"
#include "utils/row_streams.h"
#include "utils/scd.h"

namespace benchgen::tpcds::internal {

struct ItemRowData {
  int64_t item_sk = 0;
  std::string item_id;
  int32_t rec_start_date_id = 0;
  int32_t rec_end_date_id = 0;
  std::string item_desc;
  Decimal current_price;
  Decimal wholesale_cost;
  int64_t brand_id = 0;
  std::string brand;
  int64_t class_id = 0;
  std::string class_name;
  int64_t category_id = 0;
  std::string category;
  int64_t manufact_id = 0;
  std::string manufact;
  std::string size;
  std::string formulation;
  std::string color;
  std::string units;
  std::string container;
  int64_t manager_id = 0;
  std::string product_name;
  int64_t promo_sk = 0;
  int64_t null_bitmap = 0;
};

class ItemRowGenerator {
 public:
  ItemRowGenerator(double scale);

  void SkipRows(int64_t start_row);
  ItemRowData GenerateRow(int64_t row_number);
  void ConsumeRemainingSeedsForRow();

 private:
  static std::vector<int> ColumnIds();

  Scaling scaling_;
  DstDistributionStore distribution_store_;
  RowStreams streams_;
  ItemRowData old_values_;
  bool old_values_initialized_ = false;
  HierarchyState hierarchy_state_;
  ScdState scd_state_;
  Decimal min_markdown_;
  Decimal max_markdown_;
};

}  // namespace benchgen::tpcds::internal
