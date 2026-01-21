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

#include "generators/store_returns_row_generator.h"

#include <algorithm>

#include "utils/columns.h"
#include "utils/constants.h"
#include "utils/join.h"
#include "utils/null_utils.h"
#include "utils/random_utils.h"
#include "utils/tables.h"

namespace benchgen::tpcds::internal {

StoreReturnsRowGenerator::StoreReturnsRowGenerator(
    double scale)
    : scaling_(scale),
      distribution_store_(),
      streams_(ColumnIds()),
      sales_generator_(scale) {}

void StoreReturnsRowGenerator::SkipRows(int64_t start_row) {
  pricing_state_ = PricingState();
  for (int64_t i = 0; i < start_row; ++i) {
    GenerateRow(i + 1);
  }
}

StoreReturnsRowData StoreReturnsRowGenerator::GenerateRow(int64_t row_number) {
  (void)row_number;
  if (pending_index_ >= pending_returns_.size()) {
    pending_returns_.clear();
    pending_index_ = 0;
    LoadNextReturns();
  }
  return pending_returns_[pending_index_++];
}

void StoreReturnsRowGenerator::ConsumeRemainingSeedsForRow() {
  // Return streams are aligned per sales order in LoadNextReturns.
}

std::vector<int> StoreReturnsRowGenerator::ColumnIds() {
  std::vector<int> ids;
  ids.reserve(static_cast<size_t>(STORE_RETURNS_END - STORE_RETURNS_START + 1));
  for (int column = STORE_RETURNS_START; column <= STORE_RETURNS_END;
       ++column) {
    ids.push_back(column);
  }
  return ids;
}

StoreReturnsRowData StoreReturnsRowGenerator::BuildReturnRow(
    const StoreSalesRowData& sale) {
  StoreReturnsRowData row;
  row.ticket_number = sale.ticket_number;
  row.item_sk = sale.sold_item_sk;
  row.pricing = sale.pricing;

  row.customer_sk =
      MakeJoin(SR_CUSTOMER_SK, CUSTOMER, 1, &streams_.Stream(SR_CUSTOMER_SK),
               scaling_, &distribution_store_);
  if (GenerateUniformRandomInt(1, 100, &streams_.Stream(SR_TICKET_NUMBER)) <
      SR_SAME_CUSTOMER) {
    row.customer_sk = sale.sold_customer_sk;
  }

  row.returned_date_sk = static_cast<int32_t>(MakeJoin(
      SR_RETURNED_DATE_SK, DATE, sale.sold_date_sk,
      &streams_.Stream(SR_RETURNED_DATE_SK), scaling_, &distribution_store_));
  row.returned_time_sk = static_cast<int32_t>(GenerateUniformRandomInt(
      (8 * 3600) - 1, (17 * 3600) - 1, &streams_.Stream(SR_RETURNED_TIME_SK)));

  row.cdemo_sk =
      MakeJoin(SR_CDEMO_SK, CUSTOMER_DEMOGRAPHICS, 1,
               &streams_.Stream(SR_CDEMO_SK), scaling_, &distribution_store_);
  row.hdemo_sk =
      MakeJoin(SR_HDEMO_SK, HOUSEHOLD_DEMOGRAPHICS, 1,
               &streams_.Stream(SR_HDEMO_SK), scaling_, &distribution_store_);
  row.addr_sk =
      MakeJoin(SR_ADDR_SK, CUSTOMER_ADDRESS, 1, &streams_.Stream(SR_ADDR_SK),
               scaling_, &distribution_store_);
  row.store_sk = MakeJoin(SR_STORE_SK, STORE, 1, &streams_.Stream(SR_STORE_SK),
                          scaling_, &distribution_store_);
  row.reason_sk =
      MakeJoin(SR_REASON_SK, REASON, 1, &streams_.Stream(SR_REASON_SK),
               scaling_, &distribution_store_);

  row.pricing.quantity = GenerateUniformRandomInt(1, sale.pricing.quantity,
                                                  &streams_.Stream(SR_PRICING));
  SetPricing(SR_PRICING, &row.pricing, &streams_.Stream(SR_PRICING),
             &pricing_state_);

  row.null_bitmap =
      GenerateNullBitmap(STORE_RETURNS, &streams_.Stream(SR_NULLS));

  return row;
}

void StoreReturnsRowGenerator::LoadNextReturns() {
  while (pending_returns_.empty()) {
    int64_t order_number = current_order_ + 1;
    bool last_row = false;
    do {
      StoreSalesRowData sale = sales_generator_.GenerateRow(order_number);
      if (sale.is_returned) {
        pending_returns_.push_back(BuildReturnRow(sale));
      }
      sales_generator_.ConsumeRemainingSeedsForRow();
      last_row = sales_generator_.LastRowInTicket();
      if (last_row) {
        streams_.ConsumeRemainingSeedsForRow();
        current_order_ = order_number;
      }
    } while (!last_row);
  }
}

}  // namespace benchgen::tpcds::internal
