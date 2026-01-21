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

#include "generators/store_row_generator.h"

#include <algorithm>

#include "utils/build_support.h"
#include "utils/columns.h"
#include "utils/constants.h"
#include "utils/date.h"
#include "utils/null_utils.h"
#include "utils/random_utils.h"
#include "utils/scd.h"
#include "utils/tables.h"
#include "utils/text.h"

namespace benchgen::tpcds::internal {

StoreRowGenerator::StoreRowGenerator(double scale)
    : scaling_(scale),
      distribution_store_(),
      streams_(ColumnIds()) {
  min_tax_ = DecimalFromString(STORE_MIN_TAX_PERCENTAGE);
  max_tax_ = DecimalFromString(STORE_MAX_TAX_PERCENTAGE);
  base_date_ = Date::ToJulianDays(Date::FromString(DATE_MINIMUM));
}

void StoreRowGenerator::SkipRows(int64_t start_row) {
  old_values_ = StoreRowData();
  old_values_initialized_ = false;
  scd_state_ = ScdState();
  if (start_row <= 0) {
    return;
  }
  int64_t regen_start = ScdGroupStartRow(start_row);
  streams_.SkipRows(regen_start - 1);
  for (int64_t row = regen_start; row <= start_row; ++row) {
    GenerateRow(row);
    ConsumeRemainingSeedsForRow();
  }
}

StoreRowData StoreRowGenerator::GenerateRow(int64_t row_number) {
  StoreRowData row;
  row.null_bitmap = GenerateNullBitmap(STORE, &streams_.Stream(W_STORE_NULLS));
  row.store_sk = row_number;

  bool new_key =
      SetSCDKeys(W_STORE_ID, row_number, &row.store_id, &row.rec_start_date_id,
                 &row.rec_end_date_id, &scd_state_);
  constexpr int kStoreScdOffset = (S_STORE - STORE) * 6;
  row.rec_start_date_id -= kStoreScdOffset;
  if (row.rec_end_date_id != -1) {
    row.rec_end_date_id -= kStoreScdOffset;
  }
  bool first_record = new_key;

  int change_flags =
      static_cast<int>(streams_.Stream(W_STORE_SCD).NextRandom());

  int percentage = GenerateUniformRandomInt(
      1, 100, &streams_.Stream(W_STORE_CLOSED_DATE_ID));
  int days_open =
      GenerateUniformRandomInt(STORE_MIN_DAYS_OPEN, STORE_MAX_DAYS_OPEN,
                               &streams_.Stream(W_STORE_CLOSED_DATE_ID));
  if (percentage < STORE_CLOSED_PCT) {
    row.closed_date_id = base_date_ + days_open;
  } else {
    row.closed_date_id = -1;
  }
  ChangeSCDValue(&row.closed_date_id, &old_values_.closed_date_id,
                 &change_flags, first_record);
  if (row.closed_date_id == 0) {
    row.closed_date_id = -1;
  }

  MakeWord(&row.store_name, "syllables", row_number, 5, &distribution_store_);
  ChangeSCDValue(&row.store_name, &old_values_.store_name, &change_flags,
                 first_record);

  const auto& store_type = distribution_store_.Get("store_type");
  int store_type_index =
      store_type.PickIndex(1, &streams_.Stream(W_STORE_TYPE));
  int employees_min = store_type.GetInt(store_type_index, 2);
  int employees_max = store_type.GetInt(store_type_index, 3);
  row.employees = GenerateUniformRandomInt(employees_min, employees_max,
                                           &streams_.Stream(W_STORE_EMPLOYEES));
  ChangeSCDValue(&row.employees, &old_values_.employees, &change_flags,
                 first_record);

  int floor_min = store_type.GetInt(store_type_index, 4);
  int floor_max = store_type.GetInt(store_type_index, 5);
  row.floor_space = GenerateUniformRandomInt(
      floor_min, floor_max, &streams_.Stream(W_STORE_FLOOR_SPACE));
  ChangeSCDValue(&row.floor_space, &old_values_.floor_space, &change_flags,
                 first_record);

  const auto& hours_dist = distribution_store_.Get("call_center_hours");
  int hours_index = hours_dist.PickIndex(1, &streams_.Stream(W_STORE_HOURS));
  row.hours = hours_dist.GetString(hours_index, 1);
  ChangeSCDValuePtr(&row.hours, &old_values_.hours, &change_flags,
                    first_record);

  const auto& first_names = distribution_store_.Get("first_names");
  const auto& last_names = distribution_store_.Get("last_names");
  int first_index = first_names.PickIndex(1, &streams_.Stream(W_STORE_MANAGER));
  int last_index = last_names.PickIndex(1, &streams_.Stream(W_STORE_MANAGER));
  row.store_manager = first_names.GetString(first_index, 1) + " " +
                      last_names.GetString(last_index, 1);
  ChangeSCDValue(&row.store_manager, &old_values_.store_manager, &change_flags,
                 first_record);

  row.market_id =
      GenerateUniformRandomInt(1, 10, &streams_.Stream(W_STORE_MARKET_ID));
  ChangeSCDValue(&row.market_id, &old_values_.market_id, &change_flags,
                 first_record);

  row.tax_percentage =
      GenerateRandomDecimal(RandomDistribution::kUniform, min_tax_, max_tax_,
                            nullptr, &streams_.Stream(W_STORE_TAX_PERCENTAGE));
  ChangeSCDValue(&row.tax_percentage, &old_values_.tax_percentage,
                 &change_flags, first_record);

  const auto& geo_dist = distribution_store_.Get("geography_class");
  int geo_index =
      geo_dist.PickIndex(1, &streams_.Stream(W_STORE_GEOGRAPHY_CLASS));
  row.geography_class = geo_dist.GetString(geo_index, 1);
  ChangeSCDValuePtr(&row.geography_class, &old_values_.geography_class,
                    &change_flags, first_record);

  row.market_desc =
      GenerateText(STORE_DESC_MIN, RS_S_MARKET_DESC, &distribution_store_,
                   &streams_.Stream(W_STORE_MARKET_DESC));
  ChangeSCDValue(&row.market_desc, &old_values_.market_desc, &change_flags,
                 first_record);

  int manager_first =
      first_names.PickIndex(1, &streams_.Stream(W_STORE_MARKET_MANAGER));
  int manager_last =
      last_names.PickIndex(1, &streams_.Stream(W_STORE_MARKET_MANAGER));
  row.market_manager = first_names.GetString(manager_first, 1) + " " +
                       last_names.GetString(manager_last, 1);
  ChangeSCDValue(&row.market_manager, &old_values_.market_manager,
                 &change_flags, first_record);

  const auto& divisions = distribution_store_.Get("divisions");
  int division_index =
      divisions.PickIndex(1, &streams_.Stream(W_STORE_DIVISION_NAME));
  row.division_id = division_index;
  row.division_name = divisions.GetString(division_index, 1);
  ChangeSCDValue(&row.division_id, &old_values_.division_id, &change_flags,
                 first_record);
  ChangeSCDValuePtr(&row.division_name, &old_values_.division_name,
                    &change_flags, first_record);

  const auto& stores = distribution_store_.Get("stores");
  int company_index =
      stores.PickIndex(1, &streams_.Stream(W_STORE_COMPANY_NAME));
  row.company_id = company_index;
  row.company_name = stores.GetString(company_index, 1);
  ChangeSCDValue(&row.company_id, &old_values_.company_id, &change_flags,
                 first_record);
  ChangeSCDValuePtr(&row.company_name, &old_values_.company_name, &change_flags,
                    first_record);

  row.address = GenerateAddress(STORE, &distribution_store_,
                                &streams_.Stream(W_STORE_ADDRESS), scaling_);
  ChangeSCDValuePtr(&row.address.city, &old_values_.address.city, &change_flags,
                    first_record);
  ChangeSCDValuePtr(&row.address.county, &old_values_.address.county,
                    &change_flags, first_record);
  ChangeSCDValue(&row.address.gmt_offset, &old_values_.address.gmt_offset,
                 &change_flags, first_record);
  ChangeSCDValuePtr(&row.address.state, &old_values_.address.state,
                    &change_flags, first_record);
  ChangeSCDValuePtr(&row.address.street_type, &old_values_.address.street_type,
                    &change_flags, first_record);
  ChangeSCDValuePtr(&row.address.street_name1,
                    &old_values_.address.street_name1, &change_flags,
                    first_record);
  ChangeSCDValuePtr(&row.address.street_name2,
                    &old_values_.address.street_name2, &change_flags,
                    first_record);
  ChangeSCDValue(&row.address.street_num, &old_values_.address.street_num,
                 &change_flags, first_record);
  ChangeSCDValue(&row.address.zip, &old_values_.address.zip, &change_flags,
                 first_record);

  if (first_record || !old_values_initialized_) {
    old_values_initialized_ = true;
    old_values_.store_id = row.store_id;
    old_values_.rec_start_date_id = row.rec_start_date_id;
    old_values_.rec_end_date_id = row.rec_end_date_id;
  }

  return row;
}

void StoreRowGenerator::ConsumeRemainingSeedsForRow() {
  streams_.ConsumeRemainingSeedsForRow();
}

std::vector<int> StoreRowGenerator::ColumnIds() {
  std::vector<int> ids;
  ids.reserve(static_cast<size_t>(STORE_END - STORE_START + 1));
  for (int column = STORE_START; column <= STORE_END; ++column) {
    ids.push_back(column);
  }
  return ids;
}

}  // namespace benchgen::tpcds::internal
