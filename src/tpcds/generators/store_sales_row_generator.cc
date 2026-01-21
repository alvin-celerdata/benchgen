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

#include "generators/store_sales_row_generator.h"

#include <algorithm>

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

StoreSalesRowGenerator::StoreSalesRowGenerator(
    double scale)
    : scaling_(scale),
      distribution_store_(),
      streams_(ColumnIds()) {
  item_count_ = static_cast<int>(scaling_.IdCount(ITEM));
  remaining_items_ = 0;
  last_row_in_ticket_ = true;
}

void StoreSalesRowGenerator::SkipRows(int64_t start_row) {
  remaining_items_ = 0;
  last_row_in_ticket_ = true;
  ticket_item_base_ = 0;
  ticket_info_ = TicketInfo();
  pricing_state_ = PricingState();
  julian_date_ = 0;
  next_date_index_ = 0;
  if (item_permutation_.empty()) {
    RandomNumberStream perm_stream(SS_PERMUTATION, SeedsPerRow(SS_PERMUTATION));
    item_permutation_ = MakePermutation(item_count_, &perm_stream);
  }
  if (start_row <= 0) {
    streams_.SkipRows(0);
    return;
  }
  TicketOffset offset = FindTicketOffset(start_row, SS_TICKET_NUMBER, 8, 16);
  streams_.SkipRows(offset.order_number - 1);
  int64_t order_number = offset.order_number;
  for (int64_t i = 0; i < offset.rows_into_ticket; ++i) {
    GenerateRow(order_number);
    ConsumeRemainingSeedsForRow();
    if (LastRowInTicket()) {
      ++order_number;
    }
  }
}

StoreSalesRowData StoreSalesRowGenerator::GenerateRow(int64_t row_number) {
  StoreSalesRowData row;

  // Check if we need a new ticket
  if (remaining_items_ <= 0) {
    ticket_info_ = BuildTicketInfo(row_number);
    remaining_items_ =
        GenerateUniformRandomInt(8, 16, &streams_.Stream(SS_TICKET_NUMBER));
    EnsurePermutation();
    ticket_item_base_ = GenerateUniformRandomInt(
        1, item_count_, &streams_.Stream(SS_SOLD_ITEM_SK));
    last_row_in_ticket_ = false;
  }

  row.sold_date_sk = ticket_info_.sold_date_sk;
  row.sold_time_sk = ticket_info_.sold_time_sk;
  row.sold_customer_sk = ticket_info_.customer_sk;
  row.sold_cdemo_sk = ticket_info_.cdemo_sk;
  row.sold_hdemo_sk = ticket_info_.hdemo_sk;
  row.sold_addr_sk = ticket_info_.addr_sk;
  row.sold_store_sk = ticket_info_.store_sk;
  row.ticket_number = ticket_info_.ticket_number;

  ++ticket_item_base_;
  if (ticket_item_base_ > item_count_) {
    ticket_item_base_ = 1;
  }
  int item_key = GetPermutationEntry(item_permutation_, ticket_item_base_);
  row.sold_item_sk = MatchSCDSK(item_key, row.sold_date_sk, ITEM, scaling_);

  row.sold_promo_sk = MakeJoin(SS_SOLD_PROMO_SK, PROMOTION, 1,
                               &streams_.Stream(SS_SOLD_PROMO_SK), scaling_,
                               &distribution_store_);

  SetPricing(SS_PRICING, &row.pricing, &streams_.Stream(SS_PRICING),
             &pricing_state_);

  row.is_returned =
      GenerateUniformRandomInt(0, 99, &streams_.Stream(SR_IS_RETURNED)) <
      SR_RETURN_PCT;

  // Null bitmap
  row.null_bitmap = GenerateNullBitmap(STORE_SALES, &streams_.Stream(SS_NULLS));

  // Decrement remaining items
  --remaining_items_;
  if (remaining_items_ <= 0) {
    last_row_in_ticket_ = true;
  }

  return row;
}

void StoreSalesRowGenerator::ConsumeRemainingSeedsForRow() {
  if (!last_row_in_ticket_) {
    return;
  }
  streams_.ConsumeRemainingSeedsForRow();
}

std::vector<int> StoreSalesRowGenerator::ColumnIds() {
  std::vector<int> ids;
  ids.reserve(static_cast<size_t>(STORE_SALES_END - STORE_SALES_START + 1));
  for (int column = STORE_SALES_START; column <= STORE_SALES_END; ++column) {
    ids.push_back(column);
  }
  return ids;
}

void StoreSalesRowGenerator::EnsurePermutation() {
  if (item_permutation_.empty()) {
    item_permutation_ =
        MakePermutation(item_count_, &streams_.Stream(SS_PERMUTATION));
  }
}

void StoreSalesRowGenerator::EnsureDateState() {
  if (julian_date_ == 0) {
    const auto& calendar = distribution_store_.Get("calendar");
    julian_date_ = SkipDays(STORE_SALES, &next_date_index_, scaling_, calendar);
  }
}

StoreSalesRowGenerator::TicketInfo StoreSalesRowGenerator::BuildTicketInfo(
    int64_t ticket_number) {
  TicketInfo info;
  info.ticket_number = ticket_number;

  EnsureDateState();
  const auto& calendar = distribution_store_.Get("calendar");
  while (ticket_number > next_date_index_) {
    ++julian_date_;
    next_date_index_ +=
        DateScaling(STORE_SALES, julian_date_, scaling_, calendar);
  }

  info.store_sk =
      MakeJoin(SS_SOLD_STORE_SK, STORE, 1, &streams_.Stream(SS_SOLD_STORE_SK),
               scaling_, &distribution_store_);
  info.sold_time_sk = static_cast<int32_t>(
      MakeJoin(SS_SOLD_TIME_SK, TIME, 1, &streams_.Stream(SS_SOLD_TIME_SK),
               scaling_, &distribution_store_));
  info.sold_date_sk = static_cast<int32_t>(
      MakeJoin(SS_SOLD_DATE_SK, DATE, 1, &streams_.Stream(SS_SOLD_DATE_SK),
               scaling_, &distribution_store_));
  info.customer_sk = MakeJoin(SS_SOLD_CUSTOMER_SK, CUSTOMER, 1,
                              &streams_.Stream(SS_SOLD_CUSTOMER_SK), scaling_,
                              &distribution_store_);
  info.cdemo_sk = MakeJoin(SS_SOLD_CDEMO_SK, CUSTOMER_DEMOGRAPHICS, 1,
                           &streams_.Stream(SS_SOLD_CDEMO_SK), scaling_,
                           &distribution_store_);
  info.hdemo_sk = MakeJoin(SS_SOLD_HDEMO_SK, HOUSEHOLD_DEMOGRAPHICS, 1,
                           &streams_.Stream(SS_SOLD_HDEMO_SK), scaling_,
                           &distribution_store_);
  info.addr_sk = MakeJoin(SS_SOLD_ADDR_SK, CUSTOMER_ADDRESS, 1,
                          &streams_.Stream(SS_SOLD_ADDR_SK), scaling_,
                          &distribution_store_);

  return info;
}

}  // namespace benchgen::tpcds::internal
