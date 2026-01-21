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

#include "generators/warehouse_row_generator.h"

#include "utils/columns.h"
#include "utils/constants.h"
#include "utils/null_utils.h"
#include "utils/random_utils.h"
#include "utils/tables.h"
#include "utils/text.h"

namespace benchgen::tpcds::internal {

WarehouseRowGenerator::WarehouseRowGenerator(
    double scale)
    : scaling_(scale),
      distribution_store_(),
      streams_(ColumnIds()) {}

void WarehouseRowGenerator::SkipRows(int64_t start_row) {
  streams_.SkipRows(start_row);
}

WarehouseRowData WarehouseRowGenerator::GenerateRow(int64_t row_number) {
  WarehouseRowData row;
  row.null_bitmap = GenerateNullBitmap(WAREHOUSE, &streams_.Stream(W_NULLS));
  row.warehouse_sk = row_number;
  row.warehouse_id = MakeBusinessKey(static_cast<uint64_t>(row_number));
  row.warehouse_name =
      GenerateText(W_NAME_MIN, RS_W_WAREHOUSE_NAME, &distribution_store_,
                   &streams_.Stream(W_WAREHOUSE_NAME));
  row.warehouse_sq_ft = GenerateUniformRandomInt(
      W_SQFT_MIN, W_SQFT_MAX, &streams_.Stream(W_WAREHOUSE_SQ_FT));
  row.address =
      GenerateAddress(WAREHOUSE, &distribution_store_,
                      &streams_.Stream(W_WAREHOUSE_ADDRESS), scaling_);
  return row;
}

void WarehouseRowGenerator::ConsumeRemainingSeedsForRow() {
  streams_.ConsumeRemainingSeedsForRow();
}

std::vector<int> WarehouseRowGenerator::ColumnIds() {
  std::vector<int> ids;
  ids.reserve(static_cast<size_t>(WAREHOUSE_END - WAREHOUSE_START + 1));
  for (int column = WAREHOUSE_START; column <= WAREHOUSE_END; ++column) {
    ids.push_back(column);
  }
  return ids;
}

}  // namespace benchgen::tpcds::internal
