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

#include "generators/part_row_generator.h"

#include "distribution/scaling.h"
#include "utils/constants.h"
#include "utils/text.h"
#include "utils/utils.h"

namespace benchgen::tpch::internal {

PartRowGenerator::PartRowGenerator(double scale_factor, DbgenSeedMode seed_mode)
    : scale_factor_(scale_factor), seed_mode_(seed_mode) {}

arrow::Status PartRowGenerator::Init() {
  if (initialized_) {
    return arrow::Status::OK();
  }
  auto status = context_.Init(scale_factor_);
  if (!status.ok()) {
    return status;
  }
  random_state_.Reset();
  if (seed_mode_ == DbgenSeedMode::kAllTables) {
    status = AdvanceSeedsForTable(&random_state_, TableId::kPart, scale_factor_,
                                  context_.distributions());
    if (!status.ok()) {
      return status;
    }
  }
  total_rows_ = RowCount(TableId::kPart, scale_factor_);
  initialized_ = true;
  return arrow::Status::OK();
}

void PartRowGenerator::SkipRows(int64_t rows) {
  if (rows <= 0) {
    return;
  }
  SkipPart(&random_state_, rows);
  SkipPartSupp(&random_state_, rows);
}

void PartRowGenerator::GenerateRow(int64_t row_number, PartRow* out) {
  if (!out) {
    return;
  }
  out->name.clear();
  out->mfgr.clear();
  out->brand.clear();
  out->type.clear();
  out->container.clear();
  out->comment.clear();

  random_state_.RowStart();

  out->partkey = row_number;
  AggString(*context_.distributions().colors, static_cast<int>(kPNameScl),
            kPNameSd, &random_state_, &out->name);

  int64_t mfgr = random_state_.RandomInt(kPMfgMin, kPMfgMax, kPMfgSd);
  out->mfgr = FormatTagNumber(kPMfgTag, 1, mfgr);

  int64_t brnd = random_state_.RandomInt(kPBrndMin, kPBrndMax, kPBrndSd);
  out->brand = FormatTagNumber(kPBrndTag, 2, mfgr * 10 + brnd);

  PickString(*context_.distributions().p_types, kPTypeSd, &random_state_,
             &out->type);

  out->size = static_cast<int32_t>(
      random_state_.RandomInt(kPSizeMin, kPSizeMax, kPSizeSd));
  PickString(*context_.distributions().p_cntr, kPCntrSd, &random_state_,
             &out->container);

  out->retailprice = RetailPrice(out->partkey);
  GenerateText(kPCommentLen, kPCmntSd, &random_state_, context_.distributions(),
               &out->comment);

  random_state_.RowStop(DbgenTable::kPart);
}

}  // namespace benchgen::tpch::internal
