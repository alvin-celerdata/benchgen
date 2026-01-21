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

#include "utils/scd.h"

#include <array>
#include <stdexcept>

#include "utils/columns.h"
#include "utils/constants.h"
#include "utils/date.h"
#include "utils/random_utils.h"
#include "utils/table_metadata.h"
#include "utils/tables.h"

namespace benchgen::tpcds::internal {
namespace {

ScdDates BuildScdDates() {
  Date min_date = Date::FromString(DATA_START_DATE);
  Date max_date = Date::FromString(DATA_END_DATE);
  int min_julian = Date::ToJulianDays(min_date);
  int max_julian = Date::ToJulianDays(max_date);
  int half = min_julian + (max_julian - min_julian) / 2;
  int third = (max_julian - min_julian) / 3;
  int first_third = min_julian + third;
  int second_third = first_third + third;
  return ScdDates{min_julian, max_julian, half, first_third, second_third};
}

}  // namespace

const ScdDates& GetScdDates() {
  static ScdDates dates = BuildScdDates();
  return dates;
}

bool SetSCDKeys(int column_id, int64_t index, std::string* business_key,
                int32_t* rec_start_date_id, int32_t* rec_end_date_id,
                ScdState* state) {
  if (business_key == nullptr || rec_start_date_id == nullptr ||
      rec_end_date_id == nullptr) {
    throw std::invalid_argument("SCD key outputs must not be null");
  }

  const ScdDates& dates = GetScdDates();
  int table_id = TableFromColumn(column_id);
  if (table_id < 0) {
    table_id = 0;
  }

  int modulo = static_cast<int>(index % 6);
  bool new_key = false;
  int32_t start = 0;
  int32_t end = -1;
  uint64_t key_source = 0;

  switch (modulo) {
    case 1:
      key_source = static_cast<uint64_t>(index);
      new_key = true;
      start = static_cast<int32_t>(dates.min_date - table_id * 6);
      end = -1;
      break;
    case 2:
      key_source = static_cast<uint64_t>(index);
      new_key = true;
      start = static_cast<int32_t>(dates.min_date - table_id * 6);
      end = static_cast<int32_t>(dates.half_date - table_id * 6);
      break;
    case 3:
      key_source = static_cast<uint64_t>(index - 1);
      start = static_cast<int32_t>(dates.half_date - table_id * 6 + 1);
      end = -1;
      break;
    case 4:
      key_source = static_cast<uint64_t>(index);
      new_key = true;
      start = static_cast<int32_t>(dates.min_date - table_id * 6);
      end = static_cast<int32_t>(dates.third_date - table_id * 6);
      break;
    case 5:
      key_source = static_cast<uint64_t>(index - 1);
      start = static_cast<int32_t>(dates.third_date - table_id * 6 + 1);
      end = static_cast<int32_t>(dates.two_third_date - table_id * 6);
      break;
    case 0:
      key_source = static_cast<uint64_t>(index - 2);
      start = static_cast<int32_t>(dates.two_third_date - table_id * 6 + 1);
      end = -1;
      break;
    default:
      key_source = static_cast<uint64_t>(index);
      new_key = true;
      start = static_cast<int32_t>(dates.min_date - table_id * 6);
      end = -1;
      break;
  }

  if (end > dates.max_date) {
    end = -1;
  }

  std::string key = MakeBusinessKey(key_source);
  if (state != nullptr) {
    state->business_keys.at(static_cast<size_t>(table_id)) = key;
  }
  *business_key = key;
  *rec_start_date_id = start;
  *rec_end_date_id = end;
  return new_key;
}

int64_t ScdGroupStartRow(int64_t row_number) {
  if (row_number <= 0) {
    return 0;
  }
  int mod = static_cast<int>(row_number % 6);
  switch (mod) {
    case 1:
    case 2:
    case 4:
      return row_number;
    case 3:
    case 5:
      return row_number - 1;
    case 0:
      return row_number - 2;
    default:
      return row_number;
  }
}

int64_t MatchSCDSK(int64_t unique_id, int64_t julian_date, int table_number,
                   const Scaling& scaling) {
  const ScdDates& dates = GetScdDates();
  int64_t result = -1;

  switch (unique_id % 3) {
    case 1:
      result = (unique_id / 3) * 6 + 1;
      break;
    case 2:
      result = (unique_id / 3) * 6 + 2;
      if (julian_date > dates.half_date) {
        result += 1;
      }
      break;
    case 0:
      result = (unique_id / 3) * 6 - 2;
      if (julian_date > dates.third_date) {
        result += 1;
      }
      if (julian_date > dates.two_third_date) {
        result += 1;
      }
      break;
    default:
      break;
  }

  int64_t rowcount = scaling.RowCountByTableNumber(table_number);
  if (result > rowcount) {
    result = rowcount;
  }
  return result;
}

int64_t ScdJoin(int table_number, int column_id, int64_t julian_date,
                RandomNumberStream* stream, const Scaling& scaling) {
  if (stream == nullptr) {
    throw std::invalid_argument("stream must not be null");
  }

  const ScdDates& dates = GetScdDates();
  int64_t id_count = scaling.IdCount(table_number);
  int64_t picked =
      GenerateRandomKey(RandomDistribution::kUniform, 1, id_count, 0, stream);
  int64_t sk = MatchSCDSK(picked, julian_date, table_number, scaling);

  if (julian_date > dates.max_date) {
    sk = -1;
  }

  int64_t rowcount = scaling.RowCountByTableNumber(table_number);
  if (sk > rowcount) {
    sk = -1;
  }
  return sk;
}

int64_t GetSKFromID(int64_t id, int column_id, RandomNumberStream* stream) {
  if (stream == nullptr) {
    throw std::invalid_argument("stream must not be null");
  }

  int64_t result = -1;
  switch (id % 3) {
    case 1:
      result = (id / 3) * 6 + 1;
      break;
    case 2:
      result = (id / 3) * 6 +
               GenerateRandomInt(RandomDistribution::kUniform, 2, 3, 0, stream);
      break;
    case 0:
      result = (id / 3 - 1) * 6 +
               GenerateRandomInt(RandomDistribution::kUniform, 4, 6, 0, stream);
      break;
    default:
      break;
  }
  return result;
}

int64_t GetFirstSK(int64_t id) {
  int64_t result = -1;
  switch (id % 3) {
    case 1:
      result = (id / 3) * 6 + 1;
      break;
    case 2:
      result = (id / 3) * 6 + 2;
      break;
    case 0:
      result = (id / 3 - 1) * 6 + 4;
      break;
    default:
      break;
  }
  return result;
}

}  // namespace benchgen::tpcds::internal
