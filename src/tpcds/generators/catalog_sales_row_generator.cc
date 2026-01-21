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

#include "generators/catalog_sales_row_generator.h"

#include <algorithm>
#include <cmath>

#include "distribution/date_scaling.h"
#include "utils/column_streams.h"
#include "utils/columns.h"
#include "utils/constants.h"
#include "utils/decimal.h"
#include "utils/join.h"
#include "utils/null_utils.h"
#include "utils/permute.h"
#include "utils/random_utils.h"
#include "utils/scd.h"
#include "utils/tables.h"

namespace benchgen::tpcds::internal {
namespace {

struct TicketOffset {
  int64_t order_number = 1;
  int64_t ticket_start_row = 1;
  int64_t rows_into_ticket = 0;
  int64_t prev_order_number = 0;
  int64_t prev_ticket_start_row = 1;
};

TicketOffset FindTicketOffset(int64_t start_row, int column_id, int min_items,
                              int max_items) {
  TicketOffset offset;
  if (start_row <= 0) {
    return offset;
  }
  RandomNumberStream stream(column_id, SeedsPerRow(column_id));
  int64_t ticket_start_row = 1;
  int64_t order_number = 1;
  int64_t prev_ticket_start_row = 1;
  int64_t prev_order_number = 0;
  while (true) {
    int items = GenerateUniformRandomInt(min_items, max_items, &stream);
    while (stream.seeds_used() < stream.seeds_per_row()) {
      GenerateUniformRandomInt(1, 100, &stream);
    }
    stream.ResetSeedsUsed();
    int64_t ticket_end_row = ticket_start_row + items - 1;
    if (start_row <= ticket_end_row) {
      offset.order_number = order_number;
      offset.ticket_start_row = ticket_start_row;
      offset.rows_into_ticket = start_row - ticket_start_row + 1;
      offset.prev_order_number = prev_order_number;
      offset.prev_ticket_start_row = prev_ticket_start_row;
      return offset;
    }
    prev_ticket_start_row = ticket_start_row;
    prev_order_number = order_number;
    ticket_start_row = ticket_end_row + 1;
    ++order_number;
  }
}

}  // namespace

CatalogSalesRowGenerator::CatalogSalesRowGenerator(
    double scale)
    : scaling_(scale),
      distribution_store_(),
      streams_(ColumnIds()) {
  item_count_ = static_cast<int>(scaling_.IdCount(ITEM));
  remaining_line_items_ = 0;
  ticket_item_base_ = 0;
  last_row_in_order_ = true;
}

void CatalogSalesRowGenerator::SkipRows(int64_t start_row) {
  remaining_line_items_ = 0;
  last_row_in_order_ = true;
  ticket_item_base_ = 0;
  order_info_ = OrderInfo();
  pricing_state_ = PricingState();
  julian_date_ = 0;
  next_date_index_ = 0;
  last_call_center_sk_ = 0;
  if (item_permutation_.empty()) {
    RandomNumberStream perm_stream(CS_PERMUTE, SeedsPerRow(CS_PERMUTE));
    item_permutation_ = MakePermutation(item_count_, &perm_stream);
  }
  if (start_row <= 0) {
    streams_.SkipRows(0);
    return;
  }
  TicketOffset offset = FindTicketOffset(start_row, CS_ORDER_NUMBER, 4, 14);
  int64_t regen_start_row = offset.ticket_start_row;
  int64_t regen_order_number = offset.order_number;
  if (offset.prev_order_number > 0) {
    regen_start_row = offset.prev_ticket_start_row;
    regen_order_number = offset.prev_order_number;
  }
  streams_.SkipRows(regen_order_number - 1);
  int64_t order_number = regen_order_number;
  int64_t rows_to_generate = start_row - regen_start_row + 1;
  for (int64_t i = 0; i < rows_to_generate; ++i) {
    GenerateRow(order_number);
    ConsumeRemainingSeedsForRow();
    if (LastRowInOrder()) {
      ++order_number;
    }
  }
}

CatalogSalesRowData CatalogSalesRowGenerator::GenerateRow(
    int64_t order_number) {
  CatalogSalesRowData row;

  if (remaining_line_items_ <= 0) {
    order_info_ = BuildOrderInfo(order_number);
    remaining_line_items_ =
        GenerateUniformRandomInt(4, 14, &streams_.Stream(CS_ORDER_NUMBER));
    EnsurePermutation();
    ticket_item_base_ = GenerateUniformRandomInt(
        1, item_count_, &streams_.Stream(CS_SOLD_ITEM_SK));
    last_row_in_order_ = false;
  }

  row.sold_date_sk = order_info_.sold_date_sk;
  row.sold_time_sk = order_info_.sold_time_sk;
  if (row.sold_date_sk == -1) {
    row.ship_date_sk = -1;
  } else {
    int ship_delay =
        GenerateUniformRandomInt(CS_MIN_SHIP_DELAY, CS_MAX_SHIP_DELAY,
                                 &streams_.Stream(CS_SHIP_DATE_SK));
    row.ship_date_sk = row.sold_date_sk + ship_delay;
  }

  row.bill_customer_sk = order_info_.bill_customer_sk;
  row.bill_cdemo_sk = order_info_.bill_cdemo_sk;
  row.bill_hdemo_sk = order_info_.bill_hdemo_sk;
  row.bill_addr_sk = order_info_.bill_addr_sk;

  row.ship_customer_sk = order_info_.ship_customer_sk;
  row.ship_cdemo_sk = order_info_.ship_cdemo_sk;
  row.ship_hdemo_sk = order_info_.ship_hdemo_sk;
  row.ship_addr_sk = order_info_.ship_addr_sk;

  row.call_center_sk = order_info_.call_center_sk;
  if (row.sold_date_sk == -1) {
    row.catalog_page_sk = -1;
  } else {
    row.catalog_page_sk = MakeJoin(
        CS_CATALOG_PAGE_SK, CATALOG_PAGE, row.sold_date_sk,
        &streams_.Stream(CS_CATALOG_PAGE_SK), scaling_, &distribution_store_);
  }

  row.ship_mode_sk =
      MakeJoin(CS_SHIP_MODE_SK, SHIP_MODE, 1, &streams_.Stream(CS_SHIP_MODE_SK),
               scaling_, &distribution_store_);
  row.warehouse_sk =
      MakeJoin(CS_WAREHOUSE_SK, WAREHOUSE, 1, &streams_.Stream(CS_WAREHOUSE_SK),
               scaling_, &distribution_store_);

  ++ticket_item_base_;
  if (ticket_item_base_ > item_count_) {
    ticket_item_base_ = 1;
  }
  int item_key = GetPermutationEntry(item_permutation_, ticket_item_base_);
  row.sold_item_sk = MatchSCDSK(item_key, row.sold_date_sk, ITEM, scaling_);

  row.promo_sk =
      MakeJoin(CS_PROMO_SK, PROMOTION, 1, &streams_.Stream(CS_PROMO_SK),
               scaling_, &distribution_store_);

  row.order_number = order_info_.order_number;

  SetPricing(CS_PRICING, &row.pricing, &streams_.Stream(CS_PRICING),
             &pricing_state_);

  row.is_returned =
      GenerateUniformRandomInt(0, 99, &streams_.Stream(CR_IS_RETURNED)) <
      CR_RETURN_PCT;

  row.null_bitmap =
      GenerateNullBitmap(CATALOG_SALES, &streams_.Stream(CS_NULLS));

  --remaining_line_items_;
  if (remaining_line_items_ <= 0) {
    last_row_in_order_ = true;
  }

  return row;
}

void CatalogSalesRowGenerator::ConsumeRemainingSeedsForRow() {
  if (!last_row_in_order_) {
    return;
  }
  streams_.ConsumeRemainingSeedsForRow();
}

std::vector<int> CatalogSalesRowGenerator::ColumnIds() {
  std::vector<int> ids;
  ids.reserve(static_cast<size_t>(CATALOG_SALES_END - CATALOG_SALES_START + 1));
  for (int column = CATALOG_SALES_START; column <= CATALOG_SALES_END;
       ++column) {
    ids.push_back(column);
  }
  return ids;
}

void CatalogSalesRowGenerator::EnsurePermutation() {
  if (item_permutation_.empty()) {
    item_permutation_ =
        MakePermutation(item_count_, &streams_.Stream(CS_PERMUTE));
  }
}

void CatalogSalesRowGenerator::EnsureDateState() {
  if (julian_date_ == 0) {
    const auto& calendar = distribution_store_.Get("calendar");
    julian_date_ =
        SkipDays(CATALOG_SALES, &next_date_index_, scaling_, calendar);
  }
}

CatalogSalesRowGenerator::OrderInfo CatalogSalesRowGenerator::BuildOrderInfo(
    int64_t order_number) {
  OrderInfo info;
  info.order_number = order_number;

  EnsureDateState();
  const auto& calendar = distribution_store_.Get("calendar");
  while (order_number > next_date_index_) {
    ++julian_date_;
    next_date_index_ +=
        DateScaling(CATALOG_SALES, julian_date_, scaling_, calendar);
  }
  info.sold_date_sk = static_cast<int32_t>(julian_date_);
  info.sold_time_sk = static_cast<int32_t>(MakeJoin(
      CS_SOLD_TIME_SK, TIME, last_call_center_sk_,
      &streams_.Stream(CS_SOLD_TIME_SK), scaling_, &distribution_store_));
  info.call_center_sk =
      (info.sold_date_sk == -1)
          ? -1
          : MakeJoin(CS_CALL_CENTER_SK, CALL_CENTER, info.sold_date_sk,
                     &streams_.Stream(CS_CALL_CENTER_SK), scaling_,
                     &distribution_store_);
  last_call_center_sk_ = info.call_center_sk;

  info.bill_customer_sk = MakeJoin(CS_BILL_CUSTOMER_SK, CUSTOMER, 1,
                                   &streams_.Stream(CS_BILL_CUSTOMER_SK),
                                   scaling_, &distribution_store_);
  info.bill_cdemo_sk = MakeJoin(CS_BILL_CDEMO_SK, CUSTOMER_DEMOGRAPHICS, 1,
                                &streams_.Stream(CS_BILL_CDEMO_SK), scaling_,
                                &distribution_store_);
  info.bill_hdemo_sk = MakeJoin(CS_BILL_HDEMO_SK, HOUSEHOLD_DEMOGRAPHICS, 1,
                                &streams_.Stream(CS_BILL_HDEMO_SK), scaling_,
                                &distribution_store_);
  info.bill_addr_sk = MakeJoin(CS_BILL_ADDR_SK, CUSTOMER_ADDRESS, 1,
                               &streams_.Stream(CS_BILL_ADDR_SK), scaling_,
                               &distribution_store_);

  int gift_pct =
      GenerateUniformRandomInt(0, 99, &streams_.Stream(CS_SHIP_CUSTOMER_SK));
  if (gift_pct <= CS_GIFT_PCT) {
    info.ship_customer_sk = MakeJoin(CS_SHIP_CUSTOMER_SK, CUSTOMER, 2,
                                     &streams_.Stream(CS_SHIP_CUSTOMER_SK),
                                     scaling_, &distribution_store_);
    info.ship_cdemo_sk = MakeJoin(CS_SHIP_CDEMO_SK, CUSTOMER_DEMOGRAPHICS, 2,
                                  &streams_.Stream(CS_SHIP_CDEMO_SK), scaling_,
                                  &distribution_store_);
    info.ship_hdemo_sk = MakeJoin(CS_SHIP_HDEMO_SK, HOUSEHOLD_DEMOGRAPHICS, 2,
                                  &streams_.Stream(CS_SHIP_HDEMO_SK), scaling_,
                                  &distribution_store_);
    info.ship_addr_sk = MakeJoin(CS_SHIP_ADDR_SK, CUSTOMER_ADDRESS, 2,
                                 &streams_.Stream(CS_SHIP_ADDR_SK), scaling_,
                                 &distribution_store_);
  } else {
    info.ship_customer_sk = info.bill_customer_sk;
    info.ship_cdemo_sk = info.bill_cdemo_sk;
    info.ship_hdemo_sk = info.bill_hdemo_sk;
    info.ship_addr_sk = info.bill_addr_sk;
  }

  return info;
}

}  // namespace benchgen::tpcds::internal
