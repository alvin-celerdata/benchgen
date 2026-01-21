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

#include "generators/nation_row_generator.h"

#include "utils/constants.h"
#include "utils/text.h"

namespace benchgen::tpch::internal {

NationRowGenerator::NationRowGenerator(double scale_factor,
                                       DbgenSeedMode seed_mode)
    : scale_factor_(scale_factor), seed_mode_(seed_mode) {}

arrow::Status NationRowGenerator::Init() {
  if (initialized_) {
    return arrow::Status::OK();
  }
  auto status = context_.Init(scale_factor_);
  if (!status.ok()) {
    return status;
  }

  random_state_.Reset();
  if (seed_mode_ == DbgenSeedMode::kAllTables) {
    status = AdvanceSeedsForTable(&random_state_, TableId::kNation,
                                  scale_factor_, context_.distributions());
    if (!status.ok()) {
      return status;
    }
  }

  if (context_.distributions().nations) {
    total_rows_ =
        static_cast<int64_t>(context_.distributions().nations->list.size());
  }

  initialized_ = true;
  return arrow::Status::OK();
}

void NationRowGenerator::SkipRows(int64_t rows) {
  if (rows <= 0) {
    return;
  }
  random_state_.AdvanceStream(kNCmntSd,
                              random_state_.SeedBoundary(kNCmntSd) * rows);
}

void NationRowGenerator::GenerateRow(int64_t row_number, NationRow* out) {
  if (!out) {
    return;
  }
  out->name.clear();
  out->comment.clear();

  random_state_.RowStart();

  out->nationkey = row_number - 1;
  out->regionkey = 0;
  if (context_.distributions().nations && row_number > 0 &&
      row_number <=
          static_cast<int64_t>(context_.distributions().nations->list.size())) {
    const auto& entry = context_.distributions().nations->list[row_number - 1];
    out->name = entry.text;
    out->regionkey = entry.weight;
  }
  GenerateText(kNCommentLen, kNCmntSd, &random_state_, context_.distributions(),
               &out->comment);

  random_state_.RowStop(DbgenTable::kNation);
}

}  // namespace benchgen::tpch::internal
