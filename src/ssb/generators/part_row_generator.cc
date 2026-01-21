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

#include <cstdio>
#include <cstring>

#include "utils/context.h"
#include "utils/utils.h"

namespace benchgen::ssb::internal {

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
    status =
        AdvanceSeedsForTable(&random_state_, TableId::kPart, scale_factor_);
    if (!status.ok()) {
      return status;
    }
  }
  initialized_ = true;
  return arrow::Status::OK();
}

void PartRowGenerator::SkipRows(int64_t rows) {
  if (rows <= 0) {
    return;
  }
  SkipPart(&random_state_, rows);
}

void PartRowGenerator::GenerateRow(int64_t row_number, part_t* out) {
  if (!out) {
    return;
  }
  std::memset(out, 0, sizeof(*out));

  random_state_.RowStart();

  out->partkey = row_number;
  std::string name;
  AggString(*context_.distributions().colors, static_cast<int>(kPNameScl),
            kPNameSd, &random_state_, &name);
  std::string color;
  out->clen = GenerateColor(&name, &color);
  std::snprintf(out->name, sizeof(out->name), "%s", name.c_str());
  std::snprintf(out->color, sizeof(out->color), "%s", color.c_str());

  int64_t mfgr = random_state_.RandomInt(kPMfgMin, kPMfgMax, kPMfgSd);
  std::snprintf(out->mfgr, sizeof(out->mfgr), "%s%lld", "MFGR#",
                static_cast<long long>(mfgr));

  int64_t cat = random_state_.RandomInt(kPCatMin, kPCatMax, kPCatSd);
  std::snprintf(out->category, sizeof(out->category), "%s%lld", out->mfgr,
                static_cast<long long>(cat));

  int64_t brnd = random_state_.RandomInt(kPBrndMin, kPBrndMax, kPBrndSd);
  std::snprintf(out->brand, sizeof(out->brand), "%s%lld", out->category,
                static_cast<long long>(brnd));

  std::string type;
  int type_index =
      PickString(*context_.distributions().p_types, kPTypeSd, &random_state_,
                 &type);
  std::snprintf(out->type, sizeof(out->type), "%s", type.c_str());
  if (type_index >= 0) {
    out->tlen = static_cast<int>(type.size());
  }

  out->size = random_state_.RandomInt(kPSizeMin, kPSizeMax, kPSizeSd);
  std::string container;
  PickString(*context_.distributions().p_cntr, kPCntrSd, &random_state_,
             &container);
  std::snprintf(out->container, sizeof(out->container), "%s",
                container.c_str());

  random_state_.RowStop(DbgenTable::kPart);
}

}  // namespace benchgen::ssb::internal
