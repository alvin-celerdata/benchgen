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

#include "generators/web_page_row_generator.h"

#include "utils/columns.h"
#include "utils/constants.h"
#include "utils/date.h"
#include "utils/join.h"
#include "utils/null_utils.h"
#include "utils/random_utils.h"
#include "utils/scd.h"
#include "utils/tables.h"

namespace benchgen::tpcds::internal {

WebPageRowGenerator::WebPageRowGenerator(double scale)
    : scaling_(scale),
      distribution_store_(),
      streams_(ColumnIds()) {
  today_julian_ =
      Date::ToJulianDays(Date{CURRENT_YEAR, CURRENT_MONTH, CURRENT_DAY});
}

void WebPageRowGenerator::SkipRows(int64_t start_row) {
  old_values_ = WebPageRowData();
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

WebPageRowData WebPageRowGenerator::GenerateRow(int64_t row_number) {
  WebPageRowData row;
  row.null_bitmap = GenerateNullBitmap(WEB_PAGE, &streams_.Stream(WP_NULLS));
  row.page_sk = row_number;

  bool new_key =
      SetSCDKeys(WP_PAGE_ID, row_number, &row.page_id, &row.rec_start_date_id,
                 &row.rec_end_date_id, &scd_state_);
  bool first_record = new_key;

  int change_flags = static_cast<int>(streams_.Stream(WP_SCD).NextRandom());

  row.creation_date_sk = static_cast<int32_t>(MakeJoin(
      WP_CREATION_DATE_SK, DATE, row_number,
      &streams_.Stream(WP_CREATION_DATE_SK), scaling_, &distribution_store_));
  ChangeSCDValue(&row.creation_date_sk, &old_values_.creation_date_sk,
                 &change_flags, first_record);

  int access_offset = GenerateUniformRandomInt(
      0, WP_IDLE_TIME_MAX, &streams_.Stream(WP_ACCESS_DATE_SK));
  row.access_date_sk = today_julian_ - access_offset;
  ChangeSCDValue(&row.access_date_sk, &old_values_.access_date_sk,
                 &change_flags, first_record);
  if (row.access_date_sk == 0) {
    row.access_date_sk = -1;
  }

  int autogen =
      GenerateUniformRandomInt(0, 99, &streams_.Stream(WP_AUTOGEN_FLAG));
  row.autogen_flag = autogen < WP_AUTOGEN_PCT;
  ChangeSCDValue(&row.autogen_flag, &old_values_.autogen_flag, &change_flags,
                 first_record);

  row.customer_sk =
      MakeJoin(WP_CUSTOMER_SK, CUSTOMER, 1, &streams_.Stream(WP_CUSTOMER_SK),
               scaling_, &distribution_store_);
  ChangeSCDValue(&row.customer_sk, &old_values_.customer_sk, &change_flags,
                 first_record);
  if (!row.autogen_flag) {
    row.customer_sk = -1;
  }

  row.url = GenerateRandomUrl(&streams_.Stream(WP_URL));
  ChangeSCDValue(&row.url, &old_values_.url, &change_flags, first_record);

  const auto& type_dist = distribution_store_.Get("web_page_use");
  int type_index = type_dist.PickIndex(1, &streams_.Stream(WP_TYPE));
  row.type = type_dist.GetString(type_index, 1);
  ChangeSCDValuePtr(&row.type, &old_values_.type, &change_flags, first_record);

  row.link_count = GenerateUniformRandomInt(WP_LINK_MIN, WP_LINK_MAX,
                                            &streams_.Stream(WP_LINK_COUNT));
  ChangeSCDValue(&row.link_count, &old_values_.link_count, &change_flags,
                 first_record);

  row.image_count = GenerateUniformRandomInt(WP_IMAGE_MIN, WP_IMAGE_MAX,
                                             &streams_.Stream(WP_IMAGE_COUNT));
  ChangeSCDValue(&row.image_count, &old_values_.image_count, &change_flags,
                 first_record);

  row.max_ad_count = GenerateUniformRandomInt(
      WP_AD_MIN, WP_AD_MAX, &streams_.Stream(WP_MAX_AD_COUNT));
  ChangeSCDValue(&row.max_ad_count, &old_values_.max_ad_count, &change_flags,
                 first_record);

  int char_min = row.link_count * 125 + row.image_count * 50;
  int char_max = row.link_count * 300 + row.image_count * 150;
  row.char_count = GenerateUniformRandomInt(char_min, char_max,
                                            &streams_.Stream(WP_CHAR_COUNT));
  ChangeSCDValue(&row.char_count, &old_values_.char_count, &change_flags,
                 first_record);

  if (first_record || !old_values_initialized_) {
    old_values_initialized_ = true;
    old_values_.page_id = row.page_id;
    old_values_.rec_start_date_id = row.rec_start_date_id;
    old_values_.rec_end_date_id = row.rec_end_date_id;
  }

  return row;
}

void WebPageRowGenerator::ConsumeRemainingSeedsForRow() {
  streams_.ConsumeRemainingSeedsForRow();
}

std::vector<int> WebPageRowGenerator::ColumnIds() {
  std::vector<int> ids;
  ids.reserve(static_cast<size_t>(WEB_PAGE_END - WEB_PAGE_START + 1));
  for (int column = WEB_PAGE_START; column <= WEB_PAGE_END; ++column) {
    ids.push_back(column);
  }
  return ids;
}

}  // namespace benchgen::tpcds::internal
