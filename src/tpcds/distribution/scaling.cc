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

#include "distribution/scaling.h"

#include <algorithm>
#include <array>
#include <cmath>
#include <stdexcept>

#include "utils/constants.h"
#include "utils/date.h"
#include "utils/table_metadata.h"
#include "utils/tables.h"

namespace benchgen::tpcds::internal {
namespace {

constexpr std::array<double, 10> kDefinedScales = {
    0, 1, 10, 100, 300, 1000, 3000, 10000, 30000, 100000};

constexpr std::array<double, 9> kScaleVolumes = {
    1, 10, 100, 300, 1000, 3000, 10000, 30000, 100000};

int64_t ComputeInventoryRowcount(const Scaling& scaling) {
  int64_t item_count = scaling.IdCount(ITEM);
  int64_t warehouse_count = scaling.RowCountByTableNumber(WAREHOUSE);
  Date min_date = Date::FromString("1998-01-01");
  Date max_date = Date::FromString("2002-12-31");
  int days = Date::ToJulianDays(max_date) - Date::ToJulianDays(min_date) + 1;
  int weeks = (days + 6) / 7;
  return item_count * warehouse_count * weeks;
}

int64_t ComputeReturnsRowcount(const Scaling& scaling, int table_number) {
  // Returns tables are computed as a percentage of their corresponding sales
  // tables
  int sales_table = -1;
  int return_pct = 0;

  switch (table_number) {
    case CATALOG_RETURNS:
      sales_table = CATALOG_SALES;
      return_pct = CR_RETURN_PCT;
      break;
    case STORE_RETURNS:
      sales_table = STORE_SALES;
      return_pct = SR_RETURN_PCT;
      break;
    case WEB_RETURNS:
      sales_table = WEB_SALES;
      return_pct = WR_RETURN_PCT;
      break;
    default:
      return 0;
  }

  int64_t sales_count = scaling.RowCountByTableNumber(sales_table);
  return (sales_count * return_pct) / 100;
}

}  // namespace

Scaling::Scaling(double scale) : scale_(scale), distribution_store_() {
  rowcounts_ = &distribution_store_.Get("rowcounts");
}

int64_t Scaling::RowCount(benchgen::tpcds::TableId table) const {
  return RowCountByTableNumber(static_cast<int>(table));
}

int64_t Scaling::RowCountByTableNumber(int table_number) const {
  if (table_number == INVENTORY) {
    return ComputeInventoryRowcount(*this);
  }
  // Handle returns tables - they're computed as a percentage of sales
  if (table_number == CATALOG_RETURNS || table_number == STORE_RETURNS ||
      table_number == WEB_RETURNS) {
    return ComputeReturnsRowcount(*this, table_number);
  }
  if (table_number < 0 || table_number > MAX_TABLE) {
    throw std::out_of_range("table_number out of range");
  }
  return RowCountForTableNumber(table_number);
}

int64_t Scaling::IdCount(int table_number) const {
  int64_t rowcount = RowCountByTableNumber(table_number);
  if (table_number >= PSEUDO_TABLE_START) {
    return rowcount;
  }
  if (!IsType2Table(table_number)) {
    return rowcount;
  }

  int64_t unique = (rowcount / 6) * 3;
  switch (rowcount % 6) {
    case 1:
      unique += 1;
      break;
    case 2:
    case 3:
      unique += 2;
      break;
    case 4:
    case 5:
      unique += 3;
      break;
    default:
      break;
  }
  return unique;
}

int Scaling::ScaleIndex() const {
  if (scale_ <= 0.0) {
    return 1;
  }
  int index = static_cast<int>(std::floor(std::log10(scale_))) + 1;
  return std::max(1, index);
}

int64_t Scaling::RowCountForTableNumber(int table_number) const {
  int64_t base = BaseRowCount(table_number);
  if (base < 0) {
    base = 0;
  }

  int multiplier = 1;
  if (table_number < PSEUDO_TABLE_START && IsType2Table(table_number)) {
    multiplier = 2;
  }

  int exponent = rowcounts_->GetInt(table_number + 1, 2);
  if (exponent > 0) {
    for (int i = 0; i < exponent; ++i) {
      multiplier *= 10;
    }
  }

  return base * multiplier;
}

int64_t Scaling::BaseRowCount(int table_number) const {
  for (size_t i = 1; i < kDefinedScales.size(); ++i) {
    if (std::abs(scale_ - kDefinedScales[i]) < 1e-9) {
      return RowCountAtScale(table_number, static_cast<int>(i));
    }
  }

  int model = rowcounts_->GetInt(table_number + 1, 3);
  switch (model) {
    case 1:
      return RowCountAtScale(table_number, 1);
    case 2:
      return LinearScale(table_number);
    case 3:
      return LogScale(table_number);
    default:
      return RowCountAtScale(table_number, 1);
  }
}

int64_t Scaling::RowCountAtScale(int table_number, int scale_slot) const {
  if (scale_slot <= 0) {
    return 0;
  }
  return rowcounts_->Weight(table_number + 1, scale_slot);
}

int Scaling::ScaleSlot(double scale) {
  for (size_t i = 0; i < kDefinedScales.size(); ++i) {
    if (scale <= kDefinedScales[i]) {
      return static_cast<int>(i);
    }
  }
  throw std::runtime_error("scale was greater than max scale");
}

int64_t Scaling::LinearScale(int table_number) const {
  if (scale_ < 1.0) {
    int64_t base = RowCountAtScale(table_number, 1);
    int64_t scaled = static_cast<int64_t>(std::llround(scale_ * base));
    return scaled == 0 ? 1 : scaled;
  }

  double target = scale_;
  int64_t count = 0;
  for (int i = static_cast<int>(kScaleVolumes.size() - 1); i >= 0; --i) {
    while (target >= kScaleVolumes[i]) {
      count += RowCountAtScale(table_number, i + 1);
      target -= kScaleVolumes[i];
    }
  }
  return count;
}

int64_t Scaling::LogScale(int table_number) const {
  int scale_slot = ScaleSlot(scale_);
  int64_t delta = RowCountAtScale(table_number, scale_slot) -
                  RowCountAtScale(table_number, scale_slot - 1);
  double offset = (scale_ - kDefinedScales[scale_slot - 1]) /
                  (kDefinedScales[scale_slot] - kDefinedScales[scale_slot - 1]);
  int64_t base = scale_ < 1.0 ? 0 : RowCountAtScale(table_number, 1);
  int64_t count = static_cast<int64_t>(offset * delta) + base;
  if (count == 0) {
    count = 1;
  }
  return count;
}

}  // namespace benchgen::tpcds::internal
