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

#include "utils/table_metadata.h"

#include <cstddef>
#include <stdexcept>

#include "utils/columns.h"

namespace benchgen::tpcds::internal {
namespace {
static const TableMetadata kTableMetadata[] = {
    {CALL_CENTER, CALL_CENTER_START, CALL_CENTER_END, kFlagType2 | kFlagSmall,
     100, 0x0B, -1},
    {CATALOG_PAGE, CATALOG_PAGE_START, CATALOG_PAGE_END, 0, 200, 0x03, -1},
    {CATALOG_RETURNS, CATALOG_RETURNS_START, CATALOG_RETURNS_END, kFlagChild,
     400, 0x10007, -1},
    {CATALOG_SALES, CATALOG_SALES_START, CATALOG_SALES_END,
     kFlagParent | kFlagDateBased | kFlagVPrint, 100, 0x28000, CATALOG_RETURNS},
    {CUSTOMER, CUSTOMER_START, CUSTOMER_END, 0, 700, 0x13, -1},
    {CUSTOMER_ADDRESS, CUSTOMER_ADDRESS_START, CUSTOMER_ADDRESS_END, 0, 600,
     0x03, -1},
    {CUSTOMER_DEMOGRAPHICS, CUSTOMER_DEMOGRAPHICS_START,
     CUSTOMER_DEMOGRAPHICS_END, 0, 0, 0x1, 823200},
    {DATE, DATE_START, DATE_END, 0, 0, 0x03, -1},
    {HOUSEHOLD_DEMOGRAPHICS, HOUSEHOLD_DEMOGRAPHICS_START,
     HOUSEHOLD_DEMOGRAPHICS_END, 0, 0, 0x01, 7200},
    {INCOME_BAND, INCOME_BAND_START, INCOME_BAND_END, 0, 0, 0x1, -1},
    {INVENTORY, INVENTORY_START, INVENTORY_END, kFlagDateBased, 1000, 0x07, -1},
    {ITEM, ITEM_START, ITEM_END, kFlagType2, 50, 0x0B, -1},
    {PROMOTION, PROMOTION_START, PROMOTION_END, 0, 200, 0x03, -1},
    {REASON, REASON_START, REASON_END, 0, 0, 0x03, -1},
    {SHIP_MODE, SHIP_MODE_START, SHIP_MODE_END, 0, 0, 0x03, -1},
    {STORE, STORE_START, STORE_END, kFlagType2 | kFlagSmall, 100, 0xB, -1},
    {STORE_RETURNS, STORE_RETURNS_START, STORE_RETURNS_END, kFlagChild, 700,
     0x204, -1},
    {STORE_SALES, STORE_SALES_START, STORE_SALES_END,
     kFlagParent | kFlagDateBased | kFlagVPrint, 900, 0x204, STORE_RETURNS},
    {TIME, TIME_START, TIME_END, 0, 0, 0x03, -1},
    {WAREHOUSE, WAREHOUSE_START, WAREHOUSE_END, kFlagSmall, 200, 0x03, -1},
    {WEB_PAGE, WEB_PAGE_START, WEB_PAGE_END, kFlagType2, 250, 0x0B, -1},
    {WEB_RETURNS, WEB_RETURNS_START, WEB_RETURNS_END, kFlagChild, 900, 0x2004,
     -1},
    {WEB_SALES, WEB_SALES_START, WEB_SALES_END,
     kFlagVPrint | kFlagParent | kFlagDateBased, 5, 0x20008, WEB_RETURNS},
    {WEB_SITE, WEB_SITE_START, WEB_SITE_END, kFlagType2 | kFlagSmall, 100, 0x0B,
     -1},
    {DBGEN_VERSION, DBGEN_VERSION_START, DBGEN_VERSION_END, 0, 0, 0, -1},
};

constexpr int kTableMetadataCount =
    sizeof(kTableMetadata) / sizeof(kTableMetadata[0]);
}  // namespace

const TableMetadata& GetTableMetadata(int table_number) {
  if (table_number < 0 || table_number >= kTableMetadataCount) {
    throw std::out_of_range("table_number out of range");
  }
  return kTableMetadata[table_number];
}

int TableFromColumn(int column_id) {
  for (int i = 0; i < kTableMetadataCount; ++i) {
    const auto& meta = kTableMetadata[i];
    if (column_id >= meta.first_column && column_id <= meta.last_column) {
      return meta.table_number;
    }
  }
  return -1;
}

bool IsType2Table(int table_number) {
  return (GetTableMetadata(table_number).flags & kFlagType2) != 0;
}

bool IsSmallTable(int table_number) {
  return (GetTableMetadata(table_number).flags & kFlagSmall) != 0;
}

bool IsSparseTable(int table_number) {
  return (GetTableMetadata(table_number).flags & kFlagSparse) != 0;
}

bool IsParentTable(int table_number) {
  return (GetTableMetadata(table_number).flags & kFlagParent) != 0;
}

bool IsDateBasedTable(int table_number) {
  return (GetTableMetadata(table_number).flags & kFlagDateBased) != 0;
}

}  // namespace benchgen::tpcds::internal
