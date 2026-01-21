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

#include "distribution/dst_distribution_store.h"
#include "distribution/scaling.h"
#include "utils/pricing.h"
#include "utils/row_streams.h"

namespace benchgen::tpcds::internal {

struct StoreSalesRowData {
  int32_t sold_date_sk = 0;
  int32_t sold_time_sk = 0;
  int64_t sold_item_sk = 0;
  int64_t sold_customer_sk = 0;
  int64_t sold_cdemo_sk = 0;
  int64_t sold_hdemo_sk = 0;
  int64_t sold_addr_sk = 0;
  int64_t sold_store_sk = 0;
  int64_t sold_promo_sk = 0;
  int64_t ticket_number = 0;
  Pricing pricing;
  int64_t null_bitmap = 0;
  bool is_returned = false;
  bool last_row_in_ticket = true;
  int remaining_items = 0;
};

class StoreSalesRowGenerator {
 public:
  StoreSalesRowGenerator(double scale);

  void SkipRows(int64_t start_row);
  StoreSalesRowData GenerateRow(int64_t row_number);
  void ConsumeRemainingSeedsForRow();
  bool LastRowInTicket() const { return last_row_in_ticket_; }

 private:
  struct TicketInfo {
    int32_t sold_date_sk = 0;
    int32_t sold_time_sk = 0;
    int64_t customer_sk = 0;
    int64_t cdemo_sk = 0;
    int64_t hdemo_sk = 0;
    int64_t addr_sk = 0;
    int64_t store_sk = 0;
    int64_t ticket_number = 0;
  };

  static std::vector<int> ColumnIds();
  void EnsurePermutation();
  void EnsureDateState();
  TicketInfo BuildTicketInfo(int64_t ticket_number);

  Scaling scaling_;
  DstDistributionStore distribution_store_;
  RowStreams streams_;
  std::vector<int> item_permutation_;
  int item_count_ = 0;
  int remaining_items_ = 0;
  int ticket_item_base_ = 0;
  int64_t next_date_index_ = 0;
  int64_t julian_date_ = 0;
  bool last_row_in_ticket_ = true;
  TicketInfo ticket_info_;
  PricingState pricing_state_;
};

}  // namespace benchgen::tpcds::internal
