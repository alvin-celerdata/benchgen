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

#include <cstdio>
#include <cstring>

#include "utils/context.h"
#include "utils/utils.h"

namespace benchgen::ssb::internal {

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
    status =
        AdvanceSeedsForTable(&random_state_, TableId::kCustomer, scale_factor_);
    if (!status.ok()) {
      return status;
    }
  }
  initialized_ = true;
  return arrow::Status::OK();
}

void CustomerRowGenerator::SkipRows(int64_t rows) {
  if (rows <= 0) {
    return;
  }
  SkipCustomer(&random_state_, rows);
}

void CustomerRowGenerator::GenerateRow(int64_t row_number, customer_t* out) {
  if (!out) {
    return;
  }
  std::memset(out, 0, sizeof(*out));

  random_state_.RowStart();

  out->custkey = row_number;
  std::snprintf(out->name, sizeof(out->name), kCNameFmt, kCNameTag,
                static_cast<int>(row_number));
  std::string address;
  out->alen = VariableString(kCAddrLen, kCAddrSd, &random_state_, &address);
  std::snprintf(out->address, sizeof(out->address), "%s", address.c_str());

  int64_t nation_index = random_state_.RandomInt(
      0,
      static_cast<int64_t>(context_.distributions().nations->list.size() - 1),
      kCNtrgSd);
  std::strcpy(
      out->nation_name,
      context_.distributions().nations->list[nation_index].text.c_str());
  std::strcpy(
      out->region_name,
      context_.distributions()
          .regions
          ->list[context_.distributions().nations->list[nation_index].weight]
          .text.c_str());
  std::string city;
  GenerateCity(&city, std::string(out->nation_name), &random_state_);
  std::snprintf(out->city, sizeof(out->city), "%s", city.c_str());
  std::string phone;
  GeneratePhone(nation_index, &phone, kCPhneSd, &random_state_);
  std::snprintf(out->phone, sizeof(out->phone), "%s", phone.c_str());
  std::string mktsegment;
  PickString(*context_.distributions().c_mseg, kCMsegSd, &random_state_,
             &mktsegment);
  std::snprintf(out->mktsegment, sizeof(out->mktsegment), "%s",
                mktsegment.c_str());

  random_state_.RowStop(DbgenTable::kCust);
}

}  // namespace benchgen::ssb::internal
