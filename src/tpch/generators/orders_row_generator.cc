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

#include "generators/orders_row_generator.h"

#include <algorithm>

#include "distribution/scaling.h"
#include "utils/constants.h"
#include "utils/text.h"
#include "utils/utils.h"

namespace benchgen::tpch::internal {

OrdersRowGenerator::OrdersRowGenerator(double scale_factor,
                                       DbgenSeedMode seed_mode)
    : scale_factor_(scale_factor), seed_mode_(seed_mode) {}

arrow::Status OrdersRowGenerator::Init() {
  if (initialized_) {
    return arrow::Status::OK();
  }
  auto status = context_.Init(scale_factor_);
  if (!status.ok()) {
    return status;
  }
  random_state_.Reset();
  if (seed_mode_ == DbgenSeedMode::kAllTables) {
    status = AdvanceSeedsForTable(&random_state_, TableId::kOrders,
                                  scale_factor_, context_.distributions());
    if (!status.ok()) {
      return status;
    }
  }
  total_rows_ = RowCount(TableId::kOrders, scale_factor_);
  part_count_ = RowCount(TableId::kPart, scale_factor_);
  supplier_count_ = RowCount(TableId::kSupplier, scale_factor_);
  customer_count_ = RowCount(TableId::kCustomer, scale_factor_);
  int64_t scale = scale_factor_ < 1.0 ? 1 : static_cast<int64_t>(scale_factor_);
  max_clerk_ = std::max(scale * kOClerkScale, kOClerkScale);
  initialized_ = true;
  return arrow::Status::OK();
}

void OrdersRowGenerator::SkipRows(int64_t rows) {
  if (rows <= 0) {
    return;
  }
  SkipOrder(&random_state_, rows);
  SkipLine(&random_state_, rows, false);
}

int32_t OrdersRowGenerator::PeekLineCount() const {
  return static_cast<int32_t>(
      random_state_.PeekRandomInt(kOLcntMin, kOLcntMax, kOLcntSd));
}

void OrdersRowGenerator::GenerateRow(int64_t row_number, OrderRow* out) {
  if (!out) {
    return;
  }
  out->orderdate.clear();
  out->orderpriority.clear();
  out->clerk.clear();
  out->comment.clear();

  random_state_.RowStart();

  out->orderkey = MakeSparseKey(row_number, 0);
  out->totalprice = 0;
  out->shippriority = 0;
  out->orderstatus = 'O';

  int64_t custkey = random_state_.RandomInt(1, customer_count_, kOCkeySd);
  int delta = 1;
  while (custkey % kCustomerMortality == 0) {
    custkey += delta;
    custkey = std::min(custkey, customer_count_);
    delta *= -1;
  }
  out->custkey = custkey;

  int64_t tmp_date =
      random_state_.RandomInt(kStartDate, OrderDateMax(), kOOdateSd);
  if (!context_.asc_date().empty()) {
    out->orderdate = context_.asc_date()[tmp_date - kStartDate];
  }

  PickString(*context_.distributions().o_priority, kOPrioSd, &random_state_,
             &out->orderpriority);

  int64_t clerk_num = random_state_.RandomInt(1, max_clerk_, kOClrkSd);
  out->clerk = FormatTagNumber(kOClerkTag, 9, clerk_num);

  GenerateText(kOCommentLen, kOCmntSd, &random_state_, context_.distributions(),
               &out->comment);

  int32_t line_count = static_cast<int32_t>(
      random_state_.RandomInt(kOLcntMin, kOLcntMax, kOLcntSd));
  out->line_count = line_count;

  int32_t shipped_lines = 0;
  for (int32_t lcnt = 0; lcnt < line_count; ++lcnt) {
    LineItemRow& line = out->lines[lcnt];
    line.orderkey = out->orderkey;
    line.linenumber = lcnt + 1;
    line.partkey = random_state_.RandomInt(1, part_count_, kLPkeySd);
    int64_t supp_index = random_state_.RandomInt(0, kSuppPerPart - 1, kLSkeySd);
    line.suppkey = PartSuppBridge(line.partkey, supp_index, supplier_count_);

    line.quantity = random_state_.RandomInt(kLQtyMin, kLQtyMax, kLQtySd);
    line.discount = random_state_.RandomInt(kLDiscMin, kLDiscMax, kLDcntSd);
    line.tax = random_state_.RandomInt(kLTaxMin, kLTaxMax, kLTaxSd);

    PickString(*context_.distributions().l_instruct, kLShipSd, &random_state_,
               &line.shipinstruct);
    PickString(*context_.distributions().l_smode, kLSmodeSd, &random_state_,
               &line.shipmode);
    GenerateText(kLCommentLen, kLCmntSd, &random_state_,
                 context_.distributions(), &line.comment);

    int64_t rprice = RetailPrice(line.partkey);
    line.extendedprice = rprice * line.quantity;

    int64_t s_date = random_state_.RandomInt(kLSdteMin, kLSdteMax, kLSdteSd);
    s_date += tmp_date;
    int64_t c_date = random_state_.RandomInt(kLCdteMin, kLCdteMax, kLCdteSd);
    c_date += tmp_date;
    int64_t r_date = random_state_.RandomInt(kLRdteMin, kLRdteMax, kLRdteSd);
    r_date += s_date;

    if (!context_.asc_date().empty()) {
      line.shipdate = context_.asc_date()[s_date - kStartDate];
      line.commitdate = context_.asc_date()[c_date - kStartDate];
      line.receiptdate = context_.asc_date()[r_date - kStartDate];
    } else {
      line.shipdate.clear();
      line.commitdate.clear();
      line.receiptdate.clear();
    }

    if (JulianDate(r_date) <= kCurrentDate) {
      std::string rflag;
      PickString(*context_.distributions().l_rflag, kLRflgSd, &random_state_,
                 &rflag);
      line.returnflag = rflag.empty() ? 'N' : rflag[0];
    } else {
      line.returnflag = 'N';
    }

    if (JulianDate(s_date) <= kCurrentDate) {
      line.linestatus = 'F';
      ++shipped_lines;
    } else {
      line.linestatus = 'O';
    }

    out->totalprice +=
        (line.extendedprice * (kPennies - line.discount) / kPennies) *
        (kPennies + line.tax) / kPennies;
  }

  if (shipped_lines > 0) {
    out->orderstatus = 'P';
  }
  if (shipped_lines == line_count) {
    out->orderstatus = 'F';
  }

  random_state_.RowStop(DbgenTable::kOrders);
}

}  // namespace benchgen::tpch::internal
