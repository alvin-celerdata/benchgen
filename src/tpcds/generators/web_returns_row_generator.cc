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

#include "generators/web_returns_row_generator.h"

#include <algorithm>

#include "utils/columns.h"
#include "utils/constants.h"
#include "utils/join.h"
#include "utils/null_utils.h"
#include "utils/random_utils.h"
#include "utils/tables.h"

namespace benchgen::tpcds::internal {

WebReturnsRowGenerator::WebReturnsRowGenerator(
    double scale)
    : scaling_(scale),
      distribution_store_(),
      streams_(ColumnIds()),
      sales_generator_(scale) {}

void WebReturnsRowGenerator::SkipRows(int64_t start_row) {
  pricing_state_ = PricingState();
  for (int64_t i = 0; i < start_row; ++i) {
    GenerateRow(i + 1);
  }
}

WebReturnsRowData WebReturnsRowGenerator::GenerateRow(int64_t row_number) {
  (void)row_number;
  if (pending_index_ >= pending_returns_.size()) {
    pending_returns_.clear();
    pending_index_ = 0;
    LoadNextReturns();
  }
  return pending_returns_[pending_index_++];
}

void WebReturnsRowGenerator::ConsumeRemainingSeedsForRow() {
  // Return streams are aligned per sales order in LoadNextReturns.
}

std::vector<int> WebReturnsRowGenerator::ColumnIds() {
  std::vector<int> ids;
  ids.reserve(static_cast<size_t>(WEB_RETURNS_END - WEB_RETURNS_START + 1));
  for (int column = WEB_RETURNS_START; column <= WEB_RETURNS_END; ++column) {
    ids.push_back(column);
  }
  return ids;
}

WebReturnsRowData WebReturnsRowGenerator::BuildReturnRow(
    const WebSalesRowData& sale) {
  WebReturnsRowData row;
  row.item_sk = sale.item_sk;
  row.order_number = sale.order_number;
  row.web_page_sk = sale.web_page_sk;
  row.pricing = sale.pricing;

  row.returned_date_sk = static_cast<int32_t>(MakeJoin(
      WR_RETURNED_DATE_SK, DATE, sale.ship_date_sk,
      &streams_.Stream(WR_RETURNED_DATE_SK), scaling_, &distribution_store_));
  row.returned_time_sk = static_cast<int32_t>(MakeJoin(
      WR_RETURNED_TIME_SK, TIME, 1, &streams_.Stream(WR_RETURNED_TIME_SK),
      scaling_, &distribution_store_));

  row.refunded_customer_sk = MakeJoin(WR_REFUNDED_CUSTOMER_SK, CUSTOMER, 1,
                                      &streams_.Stream(WR_REFUNDED_CUSTOMER_SK),
                                      scaling_, &distribution_store_);
  row.refunded_cdemo_sk = MakeJoin(WR_REFUNDED_CDEMO_SK, CUSTOMER_DEMOGRAPHICS,
                                   1, &streams_.Stream(WR_REFUNDED_CDEMO_SK),
                                   scaling_, &distribution_store_);
  row.refunded_hdemo_sk = MakeJoin(WR_REFUNDED_HDEMO_SK, HOUSEHOLD_DEMOGRAPHICS,
                                   1, &streams_.Stream(WR_REFUNDED_HDEMO_SK),
                                   scaling_, &distribution_store_);
  row.refunded_addr_sk = MakeJoin(WR_REFUNDED_ADDR_SK, CUSTOMER_ADDRESS, 1,
                                  &streams_.Stream(WR_REFUNDED_ADDR_SK),
                                  scaling_, &distribution_store_);

  if (GenerateUniformRandomInt(
          0, 99, &streams_.Stream(WR_RETURNING_CUSTOMER_SK)) < WS_GIFT_PCT) {
    row.refunded_customer_sk = sale.ship_customer_sk;
    row.refunded_cdemo_sk = sale.ship_cdemo_sk;
    row.refunded_hdemo_sk = sale.ship_hdemo_sk;
    row.refunded_addr_sk = sale.ship_addr_sk;
  }

  row.returning_customer_sk = row.refunded_customer_sk;
  row.returning_cdemo_sk = row.refunded_cdemo_sk;
  row.returning_hdemo_sk = row.refunded_hdemo_sk;
  row.returning_addr_sk = row.refunded_addr_sk;

  row.reason_sk =
      MakeJoin(WR_REASON_SK, REASON, 1, &streams_.Stream(WR_REASON_SK),
               scaling_, &distribution_store_);

  row.pricing.quantity = GenerateUniformRandomInt(1, sale.pricing.quantity,
                                                  &streams_.Stream(WR_PRICING));
  SetPricing(WR_PRICING, &row.pricing, &streams_.Stream(WR_PRICING),
             &pricing_state_);

  row.null_bitmap = GenerateNullBitmap(WEB_RETURNS, &streams_.Stream(WR_NULLS));

  return row;
}

void WebReturnsRowGenerator::LoadNextReturns() {
  while (pending_returns_.empty()) {
    int64_t order_number = current_order_ + 1;
    bool last_row = false;
    do {
      WebSalesRowData sale = sales_generator_.GenerateRow(order_number);
      if (sale.is_returned) {
        pending_returns_.push_back(BuildReturnRow(sale));
      }
      sales_generator_.ConsumeRemainingSeedsForRow();
      last_row = sales_generator_.LastRowInOrder();
      if (last_row) {
        streams_.ConsumeRemainingSeedsForRow();
        current_order_ = order_number;
      }
    } while (!last_row);
  }
}

}  // namespace benchgen::tpcds::internal
