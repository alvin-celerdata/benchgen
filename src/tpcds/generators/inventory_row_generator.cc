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

#include "generators/inventory_row_generator.h"

#include "utils/columns.h"
#include "utils/constants.h"
#include "utils/date.h"
#include "utils/null_utils.h"
#include "utils/random_utils.h"
#include "utils/scd.h"
#include "utils/tables.h"

namespace benchgen::tpcds::internal {

InventoryRowGenerator::InventoryRowGenerator(
    double scale)
    : scaling_(scale), streams_(ColumnIds()) {
  item_count_ = scaling_.IdCount(ITEM);
  warehouse_count_ = scaling_.RowCountByTableNumber(WAREHOUSE);
  base_julian_ = Date::ToJulianDays(Date::FromString(DATE_MINIMUM));
}

void InventoryRowGenerator::SkipRows(int64_t start_row) {
  streams_.SkipRows(start_row);
}

InventoryRowData InventoryRowGenerator::GenerateRow(int64_t row_number) {
  InventoryRowData row;
  row.null_bitmap = GenerateNullBitmap(INVENTORY, &streams_.Stream(INV_NULLS));

  int64_t offset = row_number - 1;
  row.item_sk = (offset % item_count_) + 1;
  offset /= item_count_;
  row.warehouse_sk = (offset % warehouse_count_) + 1;
  offset /= warehouse_count_;
  row.date_sk = base_julian_ + static_cast<int32_t>(offset * 7);

  row.item_sk = MatchSCDSK(row.item_sk, row.date_sk, ITEM, scaling_);

  row.quantity_on_hand =
      GenerateUniformRandomInt(INV_QUANTITY_MIN, INV_QUANTITY_MAX,
                               &streams_.Stream(INV_QUANTITY_ON_HAND));

  return row;
}

void InventoryRowGenerator::ConsumeRemainingSeedsForRow() {
  streams_.ConsumeRemainingSeedsForRow();
}

std::vector<int> InventoryRowGenerator::ColumnIds() {
  std::vector<int> ids;
  ids.reserve(static_cast<size_t>(INVENTORY_END - INVENTORY_START + 1));
  for (int column = INVENTORY_START; column <= INVENTORY_END; ++column) {
    ids.push_back(column);
  }
  return ids;
}

}  // namespace benchgen::tpcds::internal
