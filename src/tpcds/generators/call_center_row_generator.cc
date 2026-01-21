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

#include "generators/call_center_row_generator.h"

#include <algorithm>
#include <cmath>

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

CallCenterRowGenerator::CallCenterRowGenerator(
    double scale)
    : scale_(scale),
      scaling_(scale),
      distribution_store_(),
      streams_(ColumnIds()) {
  min_tax_ = DecimalFromString(MIN_CC_TAX_PERCENTAGE);
  max_tax_ = DecimalFromString(MAX_CC_TAX_PERCENTAGE);
  open_date_base_ =
      Date::ToJulianDays(Date::FromString(DATA_START_DATE)) - WEB_SITE;
  old_values_.closed_date_id = -1;
}

void CallCenterRowGenerator::SkipRows(int64_t start_row) {
  old_values_ = CallCenterRowData();
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

CallCenterRowData CallCenterRowGenerator::GenerateRow(int64_t row_number) {
  CallCenterRowData row;
  row.null_bitmap = GenerateNullBitmap(CALL_CENTER, &streams_.Stream(CC_NULLS));
  row.call_center_sk = row_number;

  bool new_key =
      SetSCDKeys(CC_CALL_CENTER_ID, row_number, &row.call_center_id,
                 &row.rec_start_date_id, &row.rec_end_date_id, &scd_state_);
  bool first_record = new_key;

  if (new_key) {
    int open_offset =
        GenerateUniformRandomInt(-365, 0, &streams_.Stream(CC_OPEN_DATE_ID));
    row.open_date_id = static_cast<int32_t>(open_date_base_ - open_offset);

    const auto& call_centers = distribution_store_.Get("call_centers");
    int dist_size = call_centers.size();
    int suffix = static_cast<int>(row_number / dist_size);
    int index = static_cast<int>(row_number % dist_size) + 1;
    row.name = call_centers.GetString(index, 1);
    if (suffix > 0) {
      row.name += "_" + std::to_string(suffix);
    }

    row.address = GenerateAddress(CALL_CENTER, &distribution_store_,
                                  &streams_.Stream(CC_ADDRESS), scaling_);
    old_values_.name = row.name;
    old_values_.address = row.address;
    old_values_.open_date_id = row.open_date_id;
  } else {
    row.name = old_values_.name;
    row.address = old_values_.address;
    row.open_date_id = old_values_.open_date_id;
  }

  int change_flags = static_cast<int>(streams_.Stream(CC_SCD).NextRandom());

  {
    const auto& dist = distribution_store_.Get("call_center_class");
    int index = dist.PickIndex(1, &streams_.Stream(CC_CLASS));
    row.class_name = dist.GetString(index, 1);
  }
  ChangeSCDValuePtr(&row.class_name, &old_values_.class_name, &change_flags,
                    first_record);

  int n_scale = std::max(1, static_cast<int>(std::llround(scale_)));
  row.employees = GenerateUniformRandomInt(
      1, CC_EMPLOYEE_MAX * n_scale * n_scale, &streams_.Stream(CC_EMPLOYEES));
  ChangeSCDValue(&row.employees, &old_values_.employees, &change_flags,
                 first_record);

  row.sq_ft = GenerateUniformRandomInt(100, 700, &streams_.Stream(CC_SQ_FT));
  row.sq_ft *= row.employees;
  ChangeSCDValue(&row.sq_ft, &old_values_.sq_ft, &change_flags, first_record);

  {
    const auto& dist = distribution_store_.Get("call_center_hours");
    int index = dist.PickIndex(1, &streams_.Stream(CC_HOURS));
    row.hours = dist.GetString(index, 1);
  }
  ChangeSCDValuePtr(&row.hours, &old_values_.hours, &change_flags,
                    first_record);

  {
    const auto& first_names = distribution_store_.Get("first_names");
    const auto& last_names = distribution_store_.Get("last_names");
    int first_index = first_names.PickIndex(1, &streams_.Stream(CC_MANAGER));
    int last_index = last_names.PickIndex(1, &streams_.Stream(CC_MANAGER));
    std::string first = first_names.GetString(first_index, 1);
    std::string last = last_names.GetString(last_index, 1);
    row.manager = first + " " + last;
  }
  ChangeSCDValue(&row.manager, &old_values_.manager, &change_flags,
                 first_record);

  row.market_id =
      GenerateUniformRandomInt(1, 6, &streams_.Stream(CC_MARKET_ID));
  ChangeSCDValue(&row.market_id, &old_values_.market_id, &change_flags,
                 first_record);

  row.market_class = GenerateText(20, RS_CC_MARKET_CLASS, &distribution_store_,
                                  &streams_.Stream(CC_MARKET_CLASS));
  ChangeSCDValue(&row.market_class, &old_values_.market_class, &change_flags,
                 first_record);

  row.market_desc = GenerateText(20, RS_CC_MARKET_DESC, &distribution_store_,
                                 &streams_.Stream(CC_MARKET_DESC));
  ChangeSCDValue(&row.market_desc, &old_values_.market_desc, &change_flags,
                 first_record);

  {
    const auto& first_names = distribution_store_.Get("first_names");
    const auto& last_names = distribution_store_.Get("last_names");
    int first_index =
        first_names.PickIndex(1, &streams_.Stream(CC_MARKET_MANAGER));
    int last_index =
        last_names.PickIndex(1, &streams_.Stream(CC_MARKET_MANAGER));
    std::string first = first_names.GetString(first_index, 1);
    std::string last = last_names.GetString(last_index, 1);
    row.market_manager = first + " " + last;
  }
  ChangeSCDValue(&row.market_manager, &old_values_.market_manager,
                 &change_flags, first_record);

  row.company = GenerateUniformRandomInt(1, 6, &streams_.Stream(CC_COMPANY));
  ChangeSCDValue(&row.company, &old_values_.company, &change_flags,
                 first_record);

  row.division_id =
      GenerateUniformRandomInt(1, 6, &streams_.Stream(CC_COMPANY));
  ChangeSCDValue(&row.division_id, &old_values_.division_id, &change_flags,
                 first_record);

  MakeWord(&row.division_name, "syllables", row.division_id,
           RS_CC_DIVISION_NAME, &distribution_store_);
  ChangeSCDValue(&row.division_name, &old_values_.division_name, &change_flags,
                 first_record);

  MakeCompanyName(&row.company_name, CC_COMPANY_NAME, row.company,
                  &distribution_store_);
  ChangeSCDValue(&row.company_name, &old_values_.company_name, &change_flags,
                 first_record);

  row.tax_percentage =
      GenerateRandomDecimal(RandomDistribution::kUniform, min_tax_, max_tax_,
                            nullptr, &streams_.Stream(CC_TAX_PERCENTAGE));
  ChangeSCDValue(&row.tax_percentage, &old_values_.tax_percentage,
                 &change_flags, first_record);

  if (first_record || !old_values_initialized_) {
    old_values_initialized_ = true;
    old_values_.call_center_id = row.call_center_id;
    old_values_.rec_start_date_id = row.rec_start_date_id;
    old_values_.rec_end_date_id = row.rec_end_date_id;
  }

  return row;
}

void CallCenterRowGenerator::ConsumeRemainingSeedsForRow() {
  streams_.ConsumeRemainingSeedsForRow();
}

std::vector<int> CallCenterRowGenerator::ColumnIds() {
  std::vector<int> ids;
  ids.reserve(static_cast<size_t>(CALL_CENTER_END - CALL_CENTER_START + 1));
  for (int column = CALL_CENTER_START; column <= CALL_CENTER_END; ++column) {
    ids.push_back(column);
  }
  return ids;
}

}  // namespace benchgen::tpcds::internal
