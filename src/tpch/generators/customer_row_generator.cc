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

#include "generators/customer_row_generator.h"

#include "distribution/scaling.h"
#include "utils/constants.h"
#include "utils/text.h"
#include "utils/utils.h"

namespace benchgen::tpch::internal {

CustomerRowGenerator::CustomerRowGenerator(double scale_factor,
                                           DbgenSeedMode seed_mode)
    : scale_factor_(scale_factor), seed_mode_(seed_mode) {}

arrow::Status CustomerRowGenerator::Init() {
  if (initialized_) {
    return arrow::Status::OK();
  }
  auto status = context_.Init(scale_factor_);
  if (!status.ok()) {
    return status;
  }
  random_state_.Reset();
  if (seed_mode_ == DbgenSeedMode::kAllTables) {
    status = AdvanceSeedsForTable(&random_state_, TableId::kCustomer,
                                  scale_factor_, context_.distributions());
    if (!status.ok()) {
      return status;
    }
  }
  total_rows_ = RowCount(TableId::kCustomer, scale_factor_);
  initialized_ = true;
  return arrow::Status::OK();
}

void CustomerRowGenerator::SkipRows(int64_t rows) {
  if (rows <= 0) {
    return;
  }
  SkipCustomer(&random_state_, rows);
}

void CustomerRowGenerator::GenerateRow(int64_t row_number, CustomerRow* out) {
  if (!out) {
    return;
  }
  out->name.clear();
  out->address.clear();
  out->phone.clear();
  out->mktsegment.clear();
  out->comment.clear();

  random_state_.RowStart();

  out->custkey = row_number;
  out->name = FormatTagNumber(kCNameTag, 9, row_number);

  VariableString(kCAddressLen, kCAddrSd, &random_state_, &out->address);

  int64_t nation_index = 0;
  if (context_.distributions().nations &&
      !context_.distributions().nations->list.empty()) {
    nation_index = random_state_.RandomInt(
        0,
        static_cast<int64_t>(context_.distributions().nations->list.size() - 1),
        kCNtrgSd);
  }
  out->nationkey = nation_index;
  GeneratePhone(nation_index, kCPhneSd, &random_state_, &out->phone);

  out->acctbal = random_state_.RandomInt(kCAbalMin, kCAbalMax, kCAbalSd);
  PickString(*context_.distributions().c_mseg, kCMsegSd, &random_state_,
             &out->mktsegment);
  GenerateText(kCCommentLen, kCCmntSd, &random_state_, context_.distributions(),
               &out->comment);

  random_state_.RowStop(DbgenTable::kCustomer);
}

}  // namespace benchgen::tpch::internal
