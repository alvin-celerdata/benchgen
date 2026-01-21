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

#include "generators/supplier_row_generator.h"

#include "distribution/scaling.h"
#include "utils/constants.h"
#include "utils/text.h"
#include "utils/utils.h"

namespace benchgen::tpch::internal {

SupplierRowGenerator::SupplierRowGenerator(double scale_factor,
                                           DbgenSeedMode seed_mode)
    : scale_factor_(scale_factor), seed_mode_(seed_mode) {}

arrow::Status SupplierRowGenerator::Init() {
  if (initialized_) {
    return arrow::Status::OK();
  }
  auto status = context_.Init(scale_factor_);
  if (!status.ok()) {
    return status;
  }
  random_state_.Reset();
  if (seed_mode_ == DbgenSeedMode::kAllTables) {
    status = AdvanceSeedsForTable(&random_state_, TableId::kSupplier,
                                  scale_factor_, context_.distributions());
    if (!status.ok()) {
      return status;
    }
  }
  total_rows_ = RowCount(TableId::kSupplier, scale_factor_);
  initialized_ = true;
  return arrow::Status::OK();
}

void SupplierRowGenerator::SkipRows(int64_t rows) {
  if (rows <= 0) {
    return;
  }
  SkipSupplier(&random_state_, rows);
}

void SupplierRowGenerator::GenerateRow(int64_t row_number, SupplierRow* out) {
  if (!out) {
    return;
  }
  out->name.clear();
  out->address.clear();
  out->phone.clear();
  out->comment.clear();

  random_state_.RowStart();

  out->suppkey = row_number;
  out->name = FormatTagNumber(kSNameTag, 9, row_number);

  VariableString(kSAddressLen, kSAddrSd, &random_state_, &out->address);

  int64_t nation_index = 0;
  if (context_.distributions().nations &&
      !context_.distributions().nations->list.empty()) {
    nation_index = random_state_.RandomInt(
        0,
        static_cast<int64_t>(context_.distributions().nations->list.size() - 1),
        kSNtrgSd);
  }
  out->nationkey = nation_index;
  GeneratePhone(nation_index, kSPhneSd, &random_state_, &out->phone);

  out->acctbal = random_state_.RandomInt(kSAbalMin, kSAbalMax, kSAbalSd);
  GenerateText(kSCommentLen, kSCmntSd, &random_state_, context_.distributions(),
               &out->comment);

  int64_t bad_press = random_state_.RandomInt(1, 10000, kBbbCmntSd);
  int64_t type = random_state_.RandomInt(0, 100, kBbbTypeSd);
  int64_t comment_len = static_cast<int64_t>(out->comment.size());
  int64_t noise =
      random_state_.RandomInt(0, comment_len - kBbbCommentLen, kBbbJnkSd);
  int64_t offset = random_state_.RandomInt(
      0, comment_len - (kBbbCommentLen + noise), kBbbOffsetSd);

  if (bad_press <= kSCommentBbb) {
    const char* type_text = (type < kBbbDeadbeats) ? kBbbComplain : kBbbCommend;
    out->comment.replace(static_cast<std::size_t>(offset), kBbbBaseLen,
                         kBbbBase);
    out->comment.replace(static_cast<std::size_t>(offset + kBbbBaseLen + noise),
                         kBbbTypeLen, type_text);
  }

  random_state_.RowStop(DbgenTable::kSupplier);
}

}  // namespace benchgen::tpch::internal
