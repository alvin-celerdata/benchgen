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

#include <arrow/status.h>

#include <array>
#include <cstdint>
#include <string>

#include "benchgen/generator_options.h"
#include "utils/context.h"
#include "utils/random.h"

namespace benchgen::tpch::internal {

struct LineItemRow {
  int64_t orderkey = 0;
  int64_t partkey = 0;
  int64_t suppkey = 0;
  int32_t linenumber = 0;
  int64_t quantity = 0;
  int64_t extendedprice = 0;
  int64_t discount = 0;
  int64_t tax = 0;
  char returnflag = 'N';
  char linestatus = 'O';
  std::string shipdate;
  std::string commitdate;
  std::string receiptdate;
  std::string shipinstruct;
  std::string shipmode;
  std::string comment;
};

struct OrderRow {
  int64_t orderkey = 0;
  int64_t custkey = 0;
  char orderstatus = 'O';
  int64_t totalprice = 0;
  std::string orderdate;
  std::string orderpriority;
  std::string clerk;
  int32_t shippriority = 0;
  std::string comment;
  int32_t line_count = 0;
  std::array<LineItemRow, kOLcntMax> lines{};
};

class OrdersRowGenerator {
 public:
  OrdersRowGenerator(double scale_factor, DbgenSeedMode seed_mode);

  arrow::Status Init();
  void SkipRows(int64_t rows);
  int32_t PeekLineCount() const;
  void GenerateRow(int64_t row_number, OrderRow* out);
  int64_t total_rows() const { return total_rows_; }

 private:
  double scale_factor_;
  DbgenSeedMode seed_mode_;
  bool initialized_ = false;
  int64_t total_rows_ = 0;
  int64_t part_count_ = 0;
  int64_t supplier_count_ = 0;
  int64_t customer_count_ = 0;
  int64_t max_clerk_ = 0;
  DbgenContext context_;
  RandomState random_state_;
};

}  // namespace benchgen::tpch::internal
