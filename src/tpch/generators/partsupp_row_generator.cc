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

#include "generators/partsupp_row_generator.h"

#include "distribution/scaling.h"
#include "utils/constants.h"
#include "utils/text.h"
#include "utils/utils.h"

namespace benchgen::tpch::internal {

PartSuppRowGenerator::PartSuppRowGenerator(double scale_factor,
                                           DbgenSeedMode seed_mode)
    : scale_factor_(scale_factor), seed_mode_(seed_mode) {}

arrow::Status PartSuppRowGenerator::Init() {
  if (initialized_) {
    return arrow::Status::OK();
  }
  auto status = context_.Init(scale_factor_);
  if (!status.ok()) {
    return status;
  }
  random_state_.Reset();
  if (seed_mode_ == DbgenSeedMode::kAllTables) {
    status = AdvanceSeedsForTable(&random_state_, TableId::kPartSupp,
                                  scale_factor_, context_.distributions());
    if (!status.ok()) {
      return status;
    }
  }
  total_parts_ = RowCount(TableId::kPart, scale_factor_);
  total_rows_ = total_parts_ * kSuppPerPart;
  supplier_count_ = RowCount(TableId::kSupplier, scale_factor_);
  current_part_index_ = 1;
  current_supp_index_ = 0;
  has_part_ = false;
  initialized_ = true;
  return arrow::Status::OK();
}

void PartSuppRowGenerator::LoadPart() {
  random_state_.RowStart();
  has_part_ = true;
  current_supp_index_ = 0;
}

void PartSuppRowGenerator::SkipRows(int64_t rows) {
  if (rows <= 0) {
    return;
  }
  while (rows > 0 && current_part_index_ <= total_parts_) {
    if (!has_part_) {
      LoadPart();
    }
    int64_t remaining =
        static_cast<int64_t>(kSuppPerPart - current_supp_index_);
    if (rows < remaining) {
      for (int64_t i = 0; i < rows; ++i) {
        random_state_.RandomInt(kPSQtyMin, kPSQtyMax, kPsQtySd);
        random_state_.RandomInt(kPSScostMin, kPSScostMax, kPsScstSd);
        std::string scratch;
        GenerateText(kPSCommentLen, kPsCmntSd, &random_state_,
                     context_.distributions(), &scratch);
        ++current_supp_index_;
      }
      return;
    }
    for (int i = current_supp_index_; i < kSuppPerPart; ++i) {
      random_state_.RandomInt(kPSQtyMin, kPSQtyMax, kPsQtySd);
      random_state_.RandomInt(kPSScostMin, kPSScostMax, kPsScstSd);
      std::string scratch;
      GenerateText(kPSCommentLen, kPsCmntSd, &random_state_,
                   context_.distributions(), &scratch);
    }
    rows -= remaining;
    random_state_.RowStop(DbgenTable::kPartSupp);
    has_part_ = false;
    current_part_index_++;
    current_supp_index_ = 0;
  }
}

bool PartSuppRowGenerator::NextRow(PartSuppRow* out) {
  if (!out) {
    return false;
  }
  while (current_part_index_ <= total_parts_) {
    if (!has_part_) {
      LoadPart();
    }
    if (current_supp_index_ < kSuppPerPart) {
      int64_t supp_index = current_supp_index_;
      out->partkey = current_part_index_;
      out->suppkey =
          PartSuppBridge(current_part_index_, supp_index, supplier_count_);
      out->availqty = static_cast<int32_t>(
          random_state_.RandomInt(kPSQtyMin, kPSQtyMax, kPsQtySd));
      out->supplycost =
          random_state_.RandomInt(kPSScostMin, kPSScostMax, kPsScstSd);
      out->comment.clear();
      GenerateText(kPSCommentLen, kPsCmntSd, &random_state_,
                   context_.distributions(), &out->comment);

      ++current_supp_index_;
      if (current_supp_index_ >= kSuppPerPart) {
        random_state_.RowStop(DbgenTable::kPartSupp);
        has_part_ = false;
        current_part_index_++;
        current_supp_index_ = 0;
      }
      return true;
    }
    random_state_.RowStop(DbgenTable::kPartSupp);
    has_part_ = false;
    current_part_index_++;
    current_supp_index_ = 0;
  }
  return false;
}

}  // namespace benchgen::tpch::internal
