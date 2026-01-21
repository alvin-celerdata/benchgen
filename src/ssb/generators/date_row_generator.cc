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

#include "generators/date_row_generator.h"

#include "utils/context.h"
#include "utils/utils.h"

namespace benchgen::ssb::internal {

DateRowGenerator::DateRowGenerator(double scale_factor, DbgenSeedMode seed_mode)
    : scale_factor_(scale_factor), seed_mode_(seed_mode) {}

arrow::Status DateRowGenerator::Init() {
  if (initialized_) {
    return arrow::Status::OK();
  }
  auto status = context_.Init(scale_factor_);
  if (!status.ok()) {
    return status;
  }
  random_state_.Reset();
  if (seed_mode_ == DbgenSeedMode::kAllTables) {
    status =
        AdvanceSeedsForTable(&random_state_, TableId::kDate, scale_factor_);
    if (!status.ok()) {
      return status;
    }
  }
  initialized_ = true;
  return arrow::Status::OK();
}

void DateRowGenerator::SkipRows(int64_t rows) {
  if (rows <= 0) {
    return;
  }
  SkipOrder(&random_state_, rows);
}

void DateRowGenerator::GenerateRow(int64_t row_number, date_t* out) {
  if (!out) {
    return;
  }
  random_state_.RowStart();
  GenerateDateRow(row_number, out);
  random_state_.RowStop(kDateTable);
}

}  // namespace benchgen::ssb::internal
