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

#include "generators/web_site_row_generator.h"

#include "utils/build_support.h"
#include "utils/columns.h"
#include "utils/constants.h"
#include "utils/join.h"
#include "utils/null_utils.h"
#include "utils/random_utils.h"
#include "utils/scd.h"
#include "utils/tables.h"
#include "utils/text.h"

namespace benchgen::tpcds::internal {

WebSiteRowGenerator::WebSiteRowGenerator(double scale)
    : scaling_(scale),
      distribution_store_(),
      streams_(ColumnIds()) {
  min_tax_ = DecimalFromString(WEB_MIN_TAX_PERCENTAGE);
  max_tax_ = DecimalFromString(WEB_MAX_TAX_PERCENTAGE);
}

void WebSiteRowGenerator::SkipRows(int64_t start_row) {
  old_values_ = WebSiteRowData();
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

WebSiteRowData WebSiteRowGenerator::GenerateRow(int64_t row_number) {
  WebSiteRowData row;
  row.null_bitmap = GenerateNullBitmap(WEB_SITE, &streams_.Stream(WEB_NULLS));
  row.site_sk = row_number;

  bool new_key =
      SetSCDKeys(WEB_SITE_ID, row_number, &row.site_id, &row.rec_start_date_id,
                 &row.rec_end_date_id, &scd_state_);
  bool first_record = new_key;

  if (new_key) {
    row.open_date = static_cast<int32_t>(MakeJoin(
        WEB_OPEN_DATE, DATE, row_number, &streams_.Stream(WEB_OPEN_DATE),
        scaling_, &distribution_store_));
    row.close_date = static_cast<int32_t>(MakeJoin(
        WEB_CLOSE_DATE, DATE, row_number, &streams_.Stream(WEB_CLOSE_DATE),
        scaling_, &distribution_store_));
    if (row.close_date > row.rec_end_date_id) {
      row.close_date = -1;
    }
    row.name = "site_" + std::to_string(row_number / 6);
    old_values_.open_date = row.open_date;
    old_values_.close_date = row.close_date;
    old_values_.name = row.name;
  } else {
    row.open_date = old_values_.open_date;
    row.close_date = old_values_.close_date;
    row.name = old_values_.name;
  }

  row.class_name = "Unknown";

  int change_flags = static_cast<int>(streams_.Stream(WEB_SCD).NextRandom());

  const auto& first_names = distribution_store_.Get("first_names");
  const auto& last_names = distribution_store_.Get("last_names");
  int manager_first = first_names.PickIndex(1, &streams_.Stream(WEB_MANAGER));
  int manager_last = last_names.PickIndex(1, &streams_.Stream(WEB_MANAGER));
  row.manager = first_names.GetString(manager_first, 1) + " " +
                last_names.GetString(manager_last, 1);
  ChangeSCDValue(&row.manager, &old_values_.manager, &change_flags,
                 first_record);

  row.market_id =
      GenerateUniformRandomInt(1, 6, &streams_.Stream(WEB_MARKET_ID));
  ChangeSCDValue(&row.market_id, &old_values_.market_id, &change_flags,
                 first_record);

  row.market_class = GenerateText(20, RS_WEB_MARKET_CLASS, &distribution_store_,
                                  &streams_.Stream(WEB_MARKET_CLASS));
  ChangeSCDValue(&row.market_class, &old_values_.market_class, &change_flags,
                 first_record);

  row.market_desc = GenerateText(20, RS_WEB_MARKET_DESC, &distribution_store_,
                                 &streams_.Stream(WEB_MARKET_DESC));
  ChangeSCDValue(&row.market_desc, &old_values_.market_desc, &change_flags,
                 first_record);

  int market_first =
      first_names.PickIndex(1, &streams_.Stream(WEB_MARKET_MANAGER));
  int market_last =
      last_names.PickIndex(1, &streams_.Stream(WEB_MARKET_MANAGER));
  row.market_manager = first_names.GetString(market_first, 1) + " " +
                       last_names.GetString(market_last, 1);
  ChangeSCDValue(&row.market_manager, &old_values_.market_manager,
                 &change_flags, first_record);

  row.company_id =
      GenerateUniformRandomInt(1, 6, &streams_.Stream(WEB_COMPANY_ID));
  ChangeSCDValue(&row.company_id, &old_values_.company_id, &change_flags,
                 first_record);

  MakeWord(&row.company_name, "syllables", row.company_id, RS_WEB_COMPANY_NAME,
           &distribution_store_);
  ChangeSCDValue(&row.company_name, &old_values_.company_name, &change_flags,
                 first_record);

  row.address = GenerateAddress(WEB_SITE, &distribution_store_,
                                &streams_.Stream(WEB_ADDRESS), scaling_);
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

  row.tax_percentage =
      GenerateRandomDecimal(RandomDistribution::kUniform, min_tax_, max_tax_,
                            nullptr, &streams_.Stream(WEB_TAX_PERCENTAGE));
  ChangeSCDValue(&row.tax_percentage, &old_values_.tax_percentage,
                 &change_flags, first_record);

  if (first_record || !old_values_initialized_) {
    old_values_initialized_ = true;
    old_values_.site_id = row.site_id;
    old_values_.rec_start_date_id = row.rec_start_date_id;
    old_values_.rec_end_date_id = row.rec_end_date_id;
  }

  return row;
}

void WebSiteRowGenerator::ConsumeRemainingSeedsForRow() {
  streams_.ConsumeRemainingSeedsForRow();
}

std::vector<int> WebSiteRowGenerator::ColumnIds() {
  std::vector<int> ids;
  ids.reserve(static_cast<size_t>(WEB_SITE_END - WEB_SITE_START + 1));
  for (int column = WEB_SITE_START; column <= WEB_SITE_END; ++column) {
    ids.push_back(column);
  }
  return ids;
}

}  // namespace benchgen::tpcds::internal
