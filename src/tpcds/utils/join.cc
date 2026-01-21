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

#include "utils/join.h"

#include <algorithm>
#include <stdexcept>

#include "utils/columns.h"
#include "utils/constants.h"
#include "utils/date.h"
#include "utils/random_utils.h"
#include "utils/scd.h"
#include "utils/table_metadata.h"
#include "utils/tables.h"

namespace benchgen::tpcds::internal {
namespace {

constexpr int kCalendarUniform = 1;
constexpr int kCalendarSales = 3;

int CalendarWeightSet(int base, int year) {
  return base + (Date::IsLeapYear(year) ? 1 : 0);
}

int64_t JulianFromString(const char* value) {
  return Date::ToJulianDays(Date::FromString(value));
}

}  // namespace

int64_t DateJoin(int from_table, int from_column, int64_t join_count, int year,
                 RandomNumberStream* stream, const Scaling& scaling,
                 const DstDistribution& calendar) {
  if (stream == nullptr) {
    throw std::invalid_argument("stream must not be null");
  }

  static const int64_t kToday = JulianFromString(TODAYS_DATE);

  int min = -1;
  int max = -1;
  int day = 0;

  switch (from_table) {
    case STORE_SALES:
    case CATALOG_SALES:
    case WEB_SALES:
      day = calendar.PickIndex(CalendarWeightSet(kCalendarSales, year), stream);
      break;
    case STORE_RETURNS:
      min = SS_MIN_SHIP_DELAY;
      max = SS_MAX_SHIP_DELAY;
      break;
    case CATALOG_RETURNS:
      min = CS_MIN_SHIP_DELAY;
      max = CS_MAX_SHIP_DELAY;
      break;
    case WEB_RETURNS:
      min = WS_MIN_SHIP_DELAY;
      max = WS_MAX_SHIP_DELAY;
      break;
    case WEB_SITE:
    case WEB_PAGE:
      return WebJoin(from_column, join_count, stream, scaling);
    default:
      day =
          calendar.PickIndex(CalendarWeightSet(kCalendarUniform, year), stream);
      break;
  }

  if (min != -1 && max != -1) {
    int lag = GenerateUniformRandomInt(min * 2, max * 2, stream);
    return join_count + lag;
  }

  Date base{year, 1, 1};
  int64_t result = Date::ToJulianDays(base) + day;
  return result > kToday ? -1 : result;
}

int64_t TimeJoin(int from_table, RandomNumberStream* stream,
                 const DstDistribution& hours) {
  if (stream == nullptr) {
    throw std::invalid_argument("stream must not be null");
  }

  int weight_set = 1;
  switch (from_table) {
    case STORE_SALES:
    case STORE_RETURNS:
      weight_set = 2;
      break;
    case CATALOG_SALES:
    case WEB_SALES:
    case CATALOG_RETURNS:
    case WEB_RETURNS:
      weight_set = 3;
      break;
    default:
      weight_set = 1;
      break;
  }

  int hour_index = hours.PickIndex(weight_set, stream);
  int hour = hours.GetInt(hour_index, 1);
  int seconds = GenerateUniformRandomInt(0, 3599, stream);
  return static_cast<int64_t>(hour * 3600 + seconds);
}

int64_t CatalogPageJoin(int from_table, int from_column, int64_t julian_date,
                        RandomNumberStream* stream, const Scaling& scaling,
                        DstDistributionStore* store) {
  (void)from_table;
  (void)from_column;
  if (stream == nullptr || store == nullptr) {
    throw std::invalid_argument("stream and store must not be null");
  }

  int64_t page_count = scaling.RowCountByTableNumber(CATALOG_PAGE);
  int pages_per_catalog = static_cast<int>(page_count / CP_CATALOGS_PER_YEAR);
  pages_per_catalog /= (YEAR_MAXIMUM - YEAR_MINIMUM + 2);

  const auto& dist = store->Get("catalog_page_type");
  int type_index = dist.PickIndex(2, stream);
  int page = GenerateUniformRandomInt(1, pages_per_catalog, stream);

  int64_t base = JulianFromString(DATA_START_DATE);
  int offset = static_cast<int>(julian_date - base - 1);
  int count = (offset / 365) * CP_CATALOGS_PER_YEAR;
  offset %= 365;

  switch (type_index) {
    case 1:
      if (offset > 183) {
        count += 1;
      }
      break;
    case 2:
      count += (offset / 91);
      break;
    case 3:
      count += (offset / 31);
      break;
    default:
      break;
  }

  return static_cast<int64_t>(CP_SK(count, pages_per_catalog, page));
}

int64_t WebJoin(int column_id, int64_t join_key, RandomNumberStream* stream,
                const Scaling& scaling) {
  if (stream == nullptr) {
    throw std::invalid_argument("stream must not be null");
  }

  int64_t concurrent_sites =
      scaling.RowCountByTableNumber(CONCURRENT_WEB_SITES);
  int64_t start = JulianFromString(WEB_START_DATE);
  int64_t end = JulianFromString(WEB_END_DATE);
  int64_t site_duration = (end - start) * concurrent_sites;
  int64_t base = JulianFromString(DATE_MINIMUM);

  int table_param = 0;
  if (column_id == WEB_OPEN_DATE || column_id == WEB_CLOSE_DATE ||
      column_id == WEB_REC_START_DATE_ID || column_id == WEB_REC_END_DATE_ID) {
    table_param = GetTableMetadata(WEB_SITE).param;
  } else if (column_id == WP_REC_START_DATE_ID ||
             column_id == WP_REC_END_DATE_ID ||
             column_id == WP_CREATION_DATE_SK) {
    table_param = GetTableMetadata(WEB_PAGE).param;
  }
  if (table_param == 0) {
    table_param = -1;
  }

  switch (column_id) {
    case WEB_OPEN_DATE: {
      int64_t res = base - ((join_key * WEB_DATE_STAGGER) % site_duration / 2);
      if (WEB_IS_REPLACED(join_key)) {
        if (WEB_IS_REPLACEMENT(join_key)) {
          int64_t offset = (end - start) / (2 * site_duration);
          res += offset * site_duration;
        }
      }
      return res;
    }
    case WEB_CLOSE_DATE: {
      int64_t res = base - ((join_key * WEB_DATE_STAGGER) % site_duration / 2);
      res += static_cast<int64_t>(table_param) * site_duration;
      if (WEB_IS_REPLACED(join_key)) {
        if (!WEB_IS_REPLACEMENT(join_key)) {
          res -= static_cast<int64_t>(table_param) * site_duration / 2;
        }
      }
      return res;
    }
    case WEB_REC_START_DATE_ID: {
      int64_t res =
          base - (((join_key - 1) * WEB_DATE_STAGGER) % site_duration / 2);
      res += (join_key % table_param) * site_duration;
      return res;
    }
    case WEB_REC_END_DATE_ID: {
      int64_t res = base - ((join_key * WEB_DATE_STAGGER) % site_duration / 2);
      res += ((join_key + 1) % table_param) * site_duration * 5 - 1;
      return res;
    }
    case WP_REC_START_DATE_ID: {
      int64_t res =
          base - (((join_key - 1) * WEB_DATE_STAGGER) % site_duration / 2);
      res += (join_key % table_param) * site_duration * 5;
      return res;
    }
    case WP_REC_END_DATE_ID: {
      int64_t res = base - ((join_key * WEB_DATE_STAGGER) % site_duration / 2);
      res += ((join_key + 1) % table_param) * site_duration - 1;
      return res;
    }
    case WP_CREATION_DATE_SK: {
      int64_t site = join_key / WEB_PAGES_PER_SITE + 1;
      int64_t res = base - ((site * WEB_DATE_STAGGER) % site_duration / 2);
      if ((site % table_param) == 0) {
        int low = static_cast<int>(std::min(res, base));
        int high = static_cast<int>(std::max(res, base));
        res = GenerateUniformRandomInt(low, high, stream);
      }
      return res;
    }
    case WR_WEB_PAGE_SK:
    case WS_WEB_PAGE_SK:
      return GenerateUniformRandomInt(1, WEB_PAGES_PER_SITE, stream);
    default:
      break;
  }

  return -1;
}

int64_t MakeJoin(int from_column, int to_table, int64_t join_count,
                 RandomNumberStream* stream, const Scaling& scaling,
                 DstDistributionStore* store) {
  if (stream == nullptr || store == nullptr) {
    throw std::invalid_argument("stream and store must not be null");
  }

  int from_table = TableFromColumn(from_column);
  if (from_table < 0) {
    from_table = 0;
  }

  switch (to_table) {
    case CATALOG_PAGE:
      return CatalogPageJoin(from_table, from_column, join_count, stream,
                             scaling, store);
    case DATE: {
      int year = GenerateUniformRandomInt(YEAR_MINIMUM, YEAR_MAXIMUM, stream);
      const auto& calendar = store->Get("calendar");
      return DateJoin(from_table, from_column, join_count, year, stream,
                      scaling, calendar);
    }
    case TIME: {
      const auto& hours = store->Get("hours");
      return TimeJoin(from_table, stream, hours);
    }
    default:
      break;
  }

  if (IsType2Table(to_table)) {
    return ScdJoin(to_table, from_column, join_count, stream, scaling);
  }

  int64_t row_count = scaling.RowCountByTableNumber(to_table);
  return GenerateRandomKey(RandomDistribution::kUniform, 1, row_count, 0,
                           stream);
}

int64_t GetCatalogNumberFromPage(int64_t page_number, const Scaling& scaling) {
  int64_t page_count = scaling.RowCountByTableNumber(CATALOG_PAGE);
  int pages_per_catalog = static_cast<int>(page_count / CP_CATALOGS_PER_YEAR);
  pages_per_catalog /= (YEAR_MAXIMUM - YEAR_MINIMUM + 2);
  return page_number / pages_per_catalog;
}

}  // namespace benchgen::tpcds::internal
