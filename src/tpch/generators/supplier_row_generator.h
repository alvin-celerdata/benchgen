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

#include <cstdint>
#include <string>

#include "benchgen/generator_options.h"
#include "utils/context.h"
#include "utils/random.h"

namespace benchgen::tpch::internal {

struct SupplierRow {
  int64_t suppkey = 0;
  std::string name;
  std::string address;
  int64_t nationkey = 0;
  std::string phone;
  int64_t acctbal = 0;
  std::string comment;
};

class SupplierRowGenerator {
 public:
  SupplierRowGenerator(double scale_factor, DbgenSeedMode seed_mode);

  arrow::Status Init();
  void SkipRows(int64_t rows);
  void GenerateRow(int64_t row_number, SupplierRow* out);
  int64_t total_rows() const { return total_rows_; }

 private:
  double scale_factor_;
  DbgenSeedMode seed_mode_;
  bool initialized_ = false;
  int64_t total_rows_ = 0;
  DbgenContext context_;
  RandomState random_state_;
};

}  // namespace benchgen::tpch::internal
