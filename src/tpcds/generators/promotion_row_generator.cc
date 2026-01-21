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

#include "generators/promotion_row_generator.h"

#include "utils/build_support.h"
#include "utils/columns.h"
#include "utils/constants.h"
#include "utils/date.h"
#include "utils/join.h"
#include "utils/null_utils.h"
#include "utils/random_utils.h"
#include "utils/tables.h"
#include "utils/text.h"

namespace benchgen::tpcds::internal {

PromotionRowGenerator::PromotionRowGenerator(
    double scale)
    : scaling_(scale),
      distribution_store_(),
      streams_(ColumnIds()) {
  start_date_base_ = Date::ToJulianDays(Date::FromString(DATE_MINIMUM));
  cost_ = DecimalFromString("1000.00");
}

void PromotionRowGenerator::SkipRows(int64_t start_row) {
  streams_.SkipRows(start_row);
}

PromotionRowData PromotionRowGenerator::GenerateRow(int64_t row_number) {
  PromotionRowData row;
  row.null_bitmap = GenerateNullBitmap(PROMOTION, &streams_.Stream(P_NULLS));
  row.promo_sk = row_number;
  row.promo_id = MakeBusinessKey(static_cast<uint64_t>(row_number));

  row.start_date_id = start_date_base_ + GenerateUniformRandomInt(
                                             PROMO_START_MIN, PROMO_START_MAX,
                                             &streams_.Stream(P_START_DATE_ID));
  row.end_date_id = row.start_date_id +
                    GenerateUniformRandomInt(PROMO_LEN_MIN, PROMO_LEN_MAX,
                                             &streams_.Stream(P_END_DATE_ID));
  row.item_sk = MakeJoin(P_ITEM_SK, ITEM, 1, &streams_.Stream(P_ITEM_SK),
                         scaling_, &distribution_store_);

  row.cost = cost_;
  row.response_target = 1;

  MakeWord(&row.promo_name, "syllables", row_number, PROMO_NAME_LEN,
           &distribution_store_);

  int flags =
      GenerateUniformRandomInt(0, 511, &streams_.Stream(P_CHANNEL_DMAIL));
  row.channel_dmail = (flags & 0x01) != 0;
  flags <<= 1;
  row.channel_email = (flags & 0x01) != 0;
  flags <<= 1;
  row.channel_catalog = (flags & 0x01) != 0;
  flags <<= 1;
  row.channel_tv = (flags & 0x01) != 0;
  flags <<= 1;
  row.channel_radio = (flags & 0x01) != 0;
  flags <<= 1;
  row.channel_press = (flags & 0x01) != 0;
  flags <<= 1;
  row.channel_event = (flags & 0x01) != 0;
  flags <<= 1;
  row.channel_demo = (flags & 0x01) != 0;
  flags <<= 1;
  row.discount_active = (flags & 0x01) != 0;

  row.channel_details =
      GenerateText(PROMO_DETAIL_LEN_MIN, PROMO_DETAIL_LEN_MAX,
                   &distribution_store_, &streams_.Stream(P_CHANNEL_DETAILS));

  const auto& purpose_dist = distribution_store_.Get("promo_purpose");
  int purpose_index = purpose_dist.PickIndex(1, &streams_.Stream(P_PURPOSE));
  row.purpose = purpose_dist.GetString(purpose_index, 1);

  return row;
}

void PromotionRowGenerator::ConsumeRemainingSeedsForRow() {
  streams_.ConsumeRemainingSeedsForRow();
}

std::vector<int> PromotionRowGenerator::ColumnIds() {
  std::vector<int> ids;
  ids.reserve(static_cast<size_t>(PROMOTION_END - PROMOTION_START + 1));
  for (int column = PROMOTION_START; column <= PROMOTION_END; ++column) {
    ids.push_back(column);
  }
  return ids;
}

}  // namespace benchgen::tpcds::internal
