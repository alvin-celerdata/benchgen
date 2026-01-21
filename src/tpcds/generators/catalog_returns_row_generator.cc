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

#include "generators/catalog_returns_row_generator.h"

#include <algorithm>

#include "utils/columns.h"
#include "utils/constants.h"
#include "utils/join.h"
#include "utils/null_utils.h"
#include "utils/random_utils.h"
#include "utils/tables.h"

namespace benchgen::tpcds::internal {

CatalogReturnsRowGenerator::CatalogReturnsRowGenerator(
    double scale)
    : scaling_(scale),
      distribution_store_(),
      streams_(ColumnIds()),
      sales_generator_(scale) {}

void CatalogReturnsRowGenerator::SkipRows(int64_t start_row) {
  pricing_state_ = PricingState();
  for (int64_t i = 0; i < start_row; ++i) {
    GenerateRow(i + 1);
  }
}

CatalogReturnsRowData CatalogReturnsRowGenerator::GenerateRow(
    int64_t row_number) {
  (void)row_number;
  if (pending_index_ >= pending_returns_.size()) {
    pending_returns_.clear();
    pending_index_ = 0;
    LoadNextReturns();
  }
  return pending_returns_[pending_index_++];
}

void CatalogReturnsRowGenerator::ConsumeRemainingSeedsForRow() {
  // Return streams are aligned per sales order in LoadNextReturns.
}

std::vector<int> CatalogReturnsRowGenerator::ColumnIds() {
  std::vector<int> ids;
  ids.reserve(
      static_cast<size_t>(CATALOG_RETURNS_END - CATALOG_RETURNS_START + 1));
  for (int column = CATALOG_RETURNS_START; column <= CATALOG_RETURNS_END;
       ++column) {
    ids.push_back(column);
  }
  return ids;
}

CatalogReturnsRowData CatalogReturnsRowGenerator::BuildReturnRow(
    const CatalogSalesRowData& sale) {
  CatalogReturnsRowData row;
  row.item_sk = sale.sold_item_sk;
  row.catalog_page_sk = sale.catalog_page_sk;
  row.order_number = sale.order_number;
  row.call_center_sk = sale.call_center_sk;
  row.pricing = sale.pricing;

  row.refunded_customer_sk = sale.bill_customer_sk;
  row.refunded_cdemo_sk = sale.bill_cdemo_sk;
  row.refunded_hdemo_sk = sale.bill_hdemo_sk;
  row.refunded_addr_sk = sale.bill_addr_sk;

  row.returning_customer_sk =
      MakeJoin(CR_RETURNING_CUSTOMER_SK, CUSTOMER, 2,
               &streams_.Stream(CR_RETURNING_CUSTOMER_SK), scaling_,
               &distribution_store_);
  row.returning_cdemo_sk = MakeJoin(
      CR_RETURNING_CDEMO_SK, CUSTOMER_DEMOGRAPHICS, 2,
      &streams_.Stream(CR_RETURNING_CDEMO_SK), scaling_, &distribution_store_);
  row.returning_hdemo_sk = MakeJoin(
      CR_RETURNING_HDEMO_SK, HOUSEHOLD_DEMOGRAPHICS, 2,
      &streams_.Stream(CR_RETURNING_HDEMO_SK), scaling_, &distribution_store_);
  row.returning_addr_sk = MakeJoin(CR_RETURNING_ADDR_SK, CUSTOMER_ADDRESS, 2,
                                   &streams_.Stream(CR_RETURNING_ADDR_SK),
                                   scaling_, &distribution_store_);

  if (GenerateUniformRandomInt(
          0, 99, &streams_.Stream(CR_RETURNING_CUSTOMER_SK)) < CS_GIFT_PCT) {
    row.returning_customer_sk = sale.ship_customer_sk;
    row.returning_cdemo_sk = sale.ship_cdemo_sk;
    row.returning_addr_sk = sale.ship_addr_sk;
  }

  row.returned_date_sk = static_cast<int32_t>(MakeJoin(
      CR_RETURNED_DATE_SK, DATE, sale.ship_date_sk,
      &streams_.Stream(CR_RETURNED_DATE_SK), scaling_, &distribution_store_));
  row.returned_time_sk = static_cast<int32_t>(MakeJoin(
      CR_RETURNED_TIME_SK, TIME, 1, &streams_.Stream(CR_RETURNED_TIME_SK),
      scaling_, &distribution_store_));

  row.ship_mode_sk =
      MakeJoin(CR_SHIP_MODE_SK, SHIP_MODE, 1, &streams_.Stream(CR_SHIP_MODE_SK),
               scaling_, &distribution_store_);
  row.warehouse_sk =
      MakeJoin(CR_WAREHOUSE_SK, WAREHOUSE, 1, &streams_.Stream(CR_WAREHOUSE_SK),
               scaling_, &distribution_store_);
  row.reason_sk =
      MakeJoin(CR_REASON_SK, REASON, 1, &streams_.Stream(CR_REASON_SK),
               scaling_, &distribution_store_);

  if (sale.pricing.quantity != -1) {
    row.pricing.quantity = GenerateUniformRandomInt(
        1, sale.pricing.quantity, &streams_.Stream(CR_PRICING));
  } else {
    row.pricing.quantity = -1;
  }
  SetPricing(CR_PRICING, &row.pricing, &streams_.Stream(CR_PRICING),
             &pricing_state_);

  row.null_bitmap =
      GenerateNullBitmap(CATALOG_RETURNS, &streams_.Stream(CR_NULLS));

  return row;
}

void CatalogReturnsRowGenerator::LoadNextReturns() {
  while (pending_returns_.empty()) {
    int64_t order_number = current_order_ + 1;
    bool last_row = false;
    do {
      CatalogSalesRowData sale = sales_generator_.GenerateRow(order_number);
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
