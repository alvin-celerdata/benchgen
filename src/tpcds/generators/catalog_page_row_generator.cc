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

#include "generators/catalog_page_row_generator.h"

#include "utils/columns.h"
#include "utils/constants.h"
#include "utils/date.h"
#include "utils/null_utils.h"
#include "utils/random_utils.h"
#include "utils/tables.h"
#include "utils/text.h"

namespace benchgen::tpcds::internal {

CatalogPageRowGenerator::CatalogPageRowGenerator(
    double scale)
    : scaling_(scale),
      distribution_store_(),
      streams_(ColumnIds()) {
  int64_t total = scaling_.RowCountByTableNumber(CATALOG_PAGE);
  pages_per_catalog_ = static_cast<int>(total / CP_CATALOGS_PER_YEAR);
  pages_per_catalog_ /= (YEAR_MAXIMUM - YEAR_MINIMUM + 2);
  start_julian_ = Date::ToJulianDays(Date::FromString(DATA_START_DATE));
}

void CatalogPageRowGenerator::SkipRows(int64_t start_row) {
  streams_.SkipRows(start_row);
}

CatalogPageRowData CatalogPageRowGenerator::GenerateRow(int64_t row_number) {
  CatalogPageRowData row;
  row.null_bitmap =
      GenerateNullBitmap(CATALOG_PAGE, &streams_.Stream(CP_NULLS));
  row.catalog_page_sk = row_number;
  row.catalog_page_id = MakeBusinessKey(static_cast<uint64_t>(row_number));

  row.catalog_number =
      static_cast<int32_t>((row_number - 1) / pages_per_catalog_) + 1;
  row.catalog_page_number =
      static_cast<int32_t>((row_number - 1) % pages_per_catalog_) + 1;

  int catalog_interval = (row.catalog_number - 1) % CP_CATALOGS_PER_YEAR;
  int duration = 0;
  int offset = 0;
  int type_index = 0;
  switch (catalog_interval) {
    case 0:
    case 1:
      duration = 182;
      offset = catalog_interval * duration;
      type_index = 1;
      break;
    case 2:
    case 3:
    case 4:
    case 5:
      duration = 91;
      offset = (catalog_interval - 2) * duration;
      type_index = 2;
      break;
    default:
      duration = 30;
      offset = (catalog_interval - 6) * duration;
      type_index = 3;
      break;
  }

  row.start_date_id = static_cast<int32_t>(start_julian_ + offset);
  row.start_date_id += static_cast<int32_t>(
      ((row.catalog_number - 1) / CP_CATALOGS_PER_YEAR) * 365);
  row.end_date_id = row.start_date_id + duration - 1;

  row.department = "DEPARTMENT";

  const auto& dist = distribution_store_.Get("catalog_page_type");
  row.type = dist.GetString(type_index, 1);

  row.description =
      GenerateText(RS_CP_DESCRIPTION / 2, RS_CP_DESCRIPTION - 1,
                   &distribution_store_, &streams_.Stream(CP_DESCRIPTION));
  return row;
}

void CatalogPageRowGenerator::ConsumeRemainingSeedsForRow() {
  streams_.ConsumeRemainingSeedsForRow();
}

std::vector<int> CatalogPageRowGenerator::ColumnIds() {
  std::vector<int> ids;
  ids.reserve(static_cast<size_t>(CATALOG_PAGE_END - CATALOG_PAGE_START + 1));
  for (int column = CATALOG_PAGE_START; column <= CATALOG_PAGE_END; ++column) {
    ids.push_back(column);
  }
  return ids;
}

}  // namespace benchgen::tpcds::internal
