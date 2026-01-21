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

#include <cstddef>
#include <cstdint>
#include <vector>

#include "distribution/dst_distribution_store.h"
#include "distribution/scaling.h"
#include "generators/store_sales_row_generator.h"
#include "utils/pricing.h"
#include "utils/row_streams.h"

namespace benchgen::tpcds::internal {

struct StoreReturnsRowData {
  int32_t returned_date_sk = 0;
  int32_t returned_time_sk = 0;
  int64_t item_sk = 0;
  int64_t customer_sk = 0;
  int64_t cdemo_sk = 0;
  int64_t hdemo_sk = 0;
  int64_t addr_sk = 0;
  int64_t store_sk = 0;
  int64_t reason_sk = 0;
  int64_t ticket_number = 0;
  Pricing pricing;
  int64_t null_bitmap = 0;
};

class StoreReturnsRowGenerator {
 public:
  StoreReturnsRowGenerator(double scale);

  void SkipRows(int64_t start_row);
  StoreReturnsRowData GenerateRow(int64_t row_number);
  void ConsumeRemainingSeedsForRow();

 private:
  static std::vector<int> ColumnIds();
  StoreReturnsRowData BuildReturnRow(const StoreSalesRowData& sale);
  void LoadNextReturns();

  Scaling scaling_;
  DstDistributionStore distribution_store_;
  RowStreams streams_;
  StoreSalesRowGenerator sales_generator_;
  int64_t current_order_ = 0;
  std::vector<StoreReturnsRowData> pending_returns_;
  size_t pending_index_ = 0;
  PricingState pricing_state_;
};

}  // namespace benchgen::tpcds::internal
