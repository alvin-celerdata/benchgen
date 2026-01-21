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

#include "distribution/date_scaling.h"

#include <stdexcept>

#include "utils/constants.h"
#include "utils/date.h"
#include "utils/tables.h"

namespace {
constexpr int kCalendarUniform = 1;
constexpr int kCalendarSales = 3;
}  // namespace

namespace benchgen::tpcds::internal {

int64_t DateScaling(int table_number, int64_t julian_date,
                    const Scaling& scaling, const DstDistribution& calendar) {
  Date date = Date::FromJulianDays(static_cast<int>(julian_date));
  int weight_set = kCalendarUniform;
  int64_t base_rows = 0;

  switch (table_number) {
    case STORE_SALES:
    case CATALOG_SALES:
    case WEB_SALES:
      base_rows = scaling.RowCountByTableNumber(table_number);
      weight_set = kCalendarSales;
      break;
    case INVENTORY:
      base_rows =
          scaling.RowCountByTableNumber(WAREHOUSE) * scaling.IdCount(ITEM);
      weight_set = kCalendarUniform;
      break;
    default:
      throw std::runtime_error("dateScaling table not supported");
  }

  if (table_number != INVENTORY) {
    if (Date::IsLeapYear(date.year)) {
      weight_set += 1;
    }
    int calendar_total = calendar.MaxWeight(weight_set);
    calendar_total *= 5;
    int day_weight = calendar.Weight(Date::DayNumber(date), weight_set);
    base_rows *= day_weight;
    base_rows += calendar_total / 2;
    base_rows /= calendar_total;
  }

  return base_rows;
}

int64_t SkipDays(int table_number, int64_t* remainder, const Scaling& scaling,
                 const DstDistribution& calendar) {
  Date base = Date::FromString(DATA_START_DATE);
  int64_t julian = Date::ToJulianDays(base);
  int64_t index = 1;
  if (remainder != nullptr) {
    *remainder = DateScaling(table_number, julian, scaling, calendar) + index;
  }
  return julian;
}

}  // namespace benchgen::tpcds::internal
