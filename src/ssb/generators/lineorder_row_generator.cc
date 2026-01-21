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

#include "generators/lineorder_row_generator.h"

#include <algorithm>
#include <cmath>
#include <cstdio>
#include <cstring>

#include "utils/context.h"
#include "utils/utils.h"
#include "utils/scaling.h"

namespace benchgen::ssb::internal {
namespace {

int64_t PartKeyMaxForScale(long scale) {
  double factor = std::floor(std::log(static_cast<double>(scale))) + 1.0;
  return static_cast<int64_t>(kPartBase) * static_cast<int64_t>(factor);
}

int64_t OrderDateMax() {
  return kStartDate + kTotalDate - (kLSdteMax + kLRdteMax) - 1;
}

void MkSparse(int64_t index, DSS_HUGE* out, int64_t seq) {
  if (!out) {
    return;
  }
  int64_t value = index;
  int64_t low_bits = value & ((1 << kSparseKeep) - 1);
  value >>= kSparseKeep;
  value <<= kSparseBits;
  value += seq;
  value <<= kSparseKeep;
  value += low_bits;
  *out = value;
}

}  // namespace

LineorderRowGenerator::LineorderRowGenerator(double scale_factor,
                                             DbgenSeedMode seed_mode)
    : scale_factor_(scale_factor), seed_mode_(seed_mode) {}

LineorderRowGenerator::~LineorderRowGenerator() = default;

arrow::Status LineorderRowGenerator::Init() {
  if (initialized_) {
    return arrow::Status::OK();
  }
  auto status = context_.Init(scale_factor_);
  if (!status.ok()) {
    return status;
  }
  random_state_.Reset();
  if (seed_mode_ == DbgenSeedMode::kAllTables) {
    status = AdvanceSeedsForTable(&random_state_, TableId::kLineorder,
                                  scale_factor_);
    if (!status.ok()) {
      return status;
    }
  }

  total_orders_ = OrderCount(scale_factor_);
  current_order_index_ = 1;
  current_line_index_ = 0;
  has_order_ = false;
  initialized_ = true;
  return arrow::Status::OK();
}

void LineorderRowGenerator::LoadOrder() {
  random_state_.RowStart();

  std::memset(&order_, 0, sizeof(order_));

  long scale = static_cast<long>(scale_factor_);
  int64_t tmp_date =
      random_state_.RandomInt(kStartDate, OrderDateMax(), kOOdateSd);
  std::strcpy(order_.odate, context_.asc_date()[tmp_date - kStartDate].c_str());

  MkSparse(current_order_index_, &order_.okey, 0);

  int64_t cust_max = CustKeyMax();
  int64_t custkey = random_state_.RandomInt(kOCkeyMin, cust_max, kOCkeySd);
  int delta = 1;
  while (custkey % kCustomerMortality == 0) {
    custkey += delta;
    custkey = std::min(custkey, cust_max);
    delta *= -1;
  }
  order_.custkey = custkey;

  std::string opriority;
  PickString(*context_.distributions().o_priority, kOPrioSd, &random_state_,
             &opriority);
  std::snprintf(order_.opriority, sizeof(order_.opriority), "%s",
                opriority.c_str());
  int64_t max_clerk = std::max(scale * kOClrkScl, kOClrkScl);
  random_state_.RandomInt(1, max_clerk, kOClrkSd);
  order_.spriority = 0;
  order_.totalprice = 0;

  order_.lines = random_state_.RandomInt(kOLcntMin, kOLcntMax, kOLcntSd);

  int64_t part_max = PartKeyMax();
  int64_t supp_max = SuppKeyMax();
  for (int64_t lcnt = 0; lcnt < order_.lines; ++lcnt) {
    lineorder_t& line = order_.lineorders[lcnt];
    line.okey = order_.okey;
    line.linenumber = static_cast<int>(lcnt + 1);
    line.custkey = order_.custkey;
    line.partkey = random_state_.RandomInt(kLPkeyMin, part_max, kLPkeySd);
    line.suppkey = random_state_.RandomInt(kLSkeyMin, supp_max, kLSkeySd);

    line.quantity = random_state_.RandomInt(kLQtyMin, kLQtyMax, kLQtySd);
    line.discount = random_state_.RandomInt(kLDcntMin, kLDcntMax, kLDcntSd);
    line.tax = random_state_.RandomInt(kLTaxMin, kLTaxMax, kLTaxSd);

    std::strcpy(line.orderdate, order_.odate);
    std::strcpy(line.opriority, order_.opriority);
    line.ship_priority = order_.spriority;

    int64_t commit_date =
        random_state_.RandomInt(kLCdteMin, kLCdteMax, kLCdteSd);
    commit_date += tmp_date;
    std::strcpy(line.commit_date,
                context_.asc_date()[commit_date - kStartDate].c_str());

    std::string shipmode;
    PickString(*context_.distributions().l_smode, kLSmodeSd, &random_state_,
               &shipmode);
    std::snprintf(line.shipmode, sizeof(line.shipmode), "%s",
                  shipmode.c_str());

    int64_t rprice = RetailPrice(line.partkey);
    line.extended_price = rprice * line.quantity;
    line.revenue = line.extended_price * (100 - line.discount) / kPennies;
    line.supp_cost = 6 * rprice / 10;

    order_.totalprice +=
        (line.extended_price * (100 - line.discount) / kPennies) *
        (100 + line.tax) / kPennies;
  }

  for (int64_t lcnt = 0; lcnt < order_.lines; ++lcnt) {
    order_.lineorders[lcnt].order_totalprice = order_.totalprice;
  }

  random_state_.RowStop(DbgenTable::kLine);

  has_order_ = true;
  current_line_index_ = 0;
}

int64_t LineorderRowGenerator::PeekLineCount() const {
  return random_state_.PeekRandomInt(kOLcntMin, kOLcntMax, kOLcntSd);
}

void LineorderRowGenerator::AdvanceOrderSeeds() {
  random_state_.AdvanceStream(kOOdateSd, 1);
  random_state_.AdvanceStream(kOCkeySd, 1);
  random_state_.AdvanceStream(kOPrioSd, 1);
  random_state_.AdvanceStream(kOClrkSd, 1);
  random_state_.AdvanceStream(kOLcntSd, 1);
}

void LineorderRowGenerator::AdvanceLineSeeds() {
  for (int stream = kLQtySd; stream <= kLRflgSd; ++stream) {
    random_state_.AdvanceStream(stream, kOLcntMax);
  }
}

int64_t LineorderRowGenerator::PartKeyMax() const {
  long scale = static_cast<long>(scale_factor_);
  return PartKeyMaxForScale(scale);
}

int64_t LineorderRowGenerator::SuppKeyMax() const {
  return RowCount(TableId::kSupplier, scale_factor_);
}

int64_t LineorderRowGenerator::CustKeyMax() const {
  return RowCount(TableId::kCustomer, scale_factor_);
}

void LineorderRowGenerator::SkipRows(int64_t rows) {
  if (rows <= 0 || current_order_index_ > total_orders_) {
    return;
  }

  while (rows > 0 && current_order_index_ <= total_orders_) {
    if (has_order_) {
      int64_t remaining =
          static_cast<int64_t>(order_.lines) - current_line_index_;
      if (rows < remaining) {
        current_line_index_ += static_cast<int>(rows);
        return;
      }
      rows -= remaining;
      has_order_ = false;
      current_order_index_++;
      current_line_index_ = 0;
      continue;
    }

    int64_t line_count = PeekLineCount();
    if (rows < line_count) {
      LoadOrder();
      current_line_index_ = static_cast<int>(rows);
      return;
    }

    AdvanceOrderSeeds();
    AdvanceLineSeeds();
    rows -= line_count;
    current_order_index_++;
  }
}

int64_t LineorderRowGenerator::SkipOrders(int64_t orders) {
  if (orders <= 0 || current_order_index_ > total_orders_) {
    return 0;
  }

  int64_t skipped_rows = 0;
  while (orders > 0 && current_order_index_ <= total_orders_) {
    if (has_order_) {
      int64_t remaining = static_cast<int64_t>(order_.lines) -
                          static_cast<int64_t>(current_line_index_);
      if (remaining < 0) {
        remaining = 0;
      }
      skipped_rows += remaining;
      has_order_ = false;
      current_order_index_++;
      current_line_index_ = 0;
      --orders;
      continue;
    }

    int64_t line_count = PeekLineCount();
    skipped_rows += line_count;
    AdvanceOrderSeeds();
    AdvanceLineSeeds();
    current_order_index_++;
    --orders;
  }

  return skipped_rows;
}

bool LineorderRowGenerator::NextRow(const lineorder_t** out) {
  if (out == nullptr) {
    return false;
  }
  while (current_order_index_ <= total_orders_) {
    if (!has_order_) {
      LoadOrder();
    }
    if (current_line_index_ < order_.lines) {
      *out = &order_.lineorders[current_line_index_++];
      return true;
    }
    has_order_ = false;
    current_order_index_++;
    current_line_index_ = 0;
  }
  return false;
}

}  // namespace benchgen::ssb::internal
