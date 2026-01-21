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

#include "generators/ship_mode_row_generator.h"

#include "distribution/dst_distribution_utils.h"
#include "utils/columns.h"
#include "utils/constants.h"
#include "utils/null_utils.h"
#include "utils/random_utils.h"
#include "utils/tables.h"

namespace benchgen::tpcds::internal {
namespace {

constexpr char kAlphaNum[] =
    "abcdefghijklmnopqrstuvxyzABCDEFGHIJKLMNOPQRSTUVXYZ0123456789";

}  // namespace

ShipModeRowGenerator::ShipModeRowGenerator(double scale)
    : scaling_(scale),
      distribution_store_(),
      streams_(ColumnIds()) {
  type_dist_ = &distribution_store_.Get("ship_mode_type");
  code_dist_ = &distribution_store_.Get("ship_mode_code");
  carrier_dist_ = &distribution_store_.Get("ship_mode_carrier");
}

void ShipModeRowGenerator::SkipRows(int64_t start_row) {
  streams_.SkipRows(start_row);
}

ShipModeRowData ShipModeRowGenerator::GenerateRow(int64_t row_number) {
  ShipModeRowData row;
  row.ship_mode_sk = row_number;
  row.ship_mode_id = MakeBusinessKey(static_cast<uint64_t>(row_number));

  row.null_bitmap = GenerateNullBitmap(SHIP_MODE, &streams_.Stream(SM_NULLS));

  int64_t modulus = row_number;
  row.type = BitmapToString(*type_dist_, 1, &modulus);
  row.code = BitmapToString(*code_dist_, 1, &modulus);
  row.carrier = carrier_dist_->GetString(static_cast<int>(row_number), 1);
  row.contract = GenerateRandomCharset(kAlphaNum, 1, RS_SM_CONTRACT,
                                       &streams_.Stream(SM_CONTRACT));

  return row;
}

void ShipModeRowGenerator::ConsumeRemainingSeedsForRow() {
  streams_.ConsumeRemainingSeedsForRow();
}

std::vector<int> ShipModeRowGenerator::ColumnIds() {
  std::vector<int> ids;
  ids.reserve(static_cast<size_t>(SHIP_MODE_END - SHIP_MODE_START + 1));
  for (int column = SHIP_MODE_START; column <= SHIP_MODE_END; ++column) {
    ids.push_back(column);
  }
  return ids;
}

}  // namespace benchgen::tpcds::internal
