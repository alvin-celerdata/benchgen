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

#include "generators/item_row_generator.h"

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

ItemRowGenerator::ItemRowGenerator(double scale)
    : scaling_(scale),
      distribution_store_(),
      streams_(ColumnIds()) {
  min_markdown_ = DecimalFromString(MIN_ITEM_MARKDOWN_PCT);
  max_markdown_ = DecimalFromString(MAX_ITEM_MARKDOWN_PCT);
}

void ItemRowGenerator::SkipRows(int64_t start_row) {
  old_values_ = ItemRowData();
  old_values_initialized_ = false;
  hierarchy_state_ = HierarchyState();
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

ItemRowData ItemRowGenerator::GenerateRow(int64_t row_number) {
  ItemRowData row;
  row.null_bitmap = GenerateNullBitmap(ITEM, &streams_.Stream(I_NULLS));
  row.item_sk = row_number;

  const auto& manager_dist = distribution_store_.Get("i_manager_id");
  int manager_index = manager_dist.PickIndex(1, &streams_.Stream(I_MANAGER_ID));
  int manager_min = manager_dist.GetInt(manager_index, 2);
  int manager_max = manager_dist.GetInt(manager_index, 3);
  row.manager_id = GenerateUniformRandomKey(manager_min, manager_max,
                                            &streams_.Stream(I_MANAGER_ID));

  bool new_key =
      SetSCDKeys(I_ITEM_ID, row_number, &row.item_id, &row.rec_start_date_id,
                 &row.rec_end_date_id, &scd_state_);
  bool first_record = new_key;

  int change_flags = static_cast<int>(streams_.Stream(I_SCD).NextRandom());

  row.item_desc = GenerateText(1, RS_I_ITEM_DESC, &distribution_store_,
                               &streams_.Stream(I_ITEM_DESC));
  ChangeSCDValue(&row.item_desc, &old_values_.item_desc, &change_flags,
                 first_record);

  const auto& price_dist = distribution_store_.Get("i_current_price");
  int price_index = price_dist.PickIndex(1, &streams_.Stream(I_CURRENT_PRICE));
  Decimal min_price = DecimalFromString(price_dist.GetString(price_index, 2));
  Decimal max_price = DecimalFromString(price_dist.GetString(price_index, 3));
  row.current_price =
      GenerateRandomDecimal(RandomDistribution::kUniform, min_price, max_price,
                            nullptr, &streams_.Stream(I_CURRENT_PRICE));
  ChangeSCDValuePtr(&row.current_price, &old_values_.current_price,
                    &change_flags, first_record);

  Decimal markdown = GenerateRandomDecimal(
      RandomDistribution::kUniform, min_markdown_, max_markdown_, nullptr,
      &streams_.Stream(I_WHOLESALE_COST));
  ApplyDecimalOp(&row.wholesale_cost, DecimalOp::kMultiply, row.current_price,
                 markdown);
  ChangeSCDValue(&row.wholesale_cost, &old_values_.wholesale_cost,
                 &change_flags, first_record);

  HierarchyItem(I_CATEGORY, &row.category_id, &row.category, row_number,
                &distribution_store_, &streams_.Stream(I_CATEGORY),
                &hierarchy_state_);

  HierarchyItem(I_CLASS, &row.class_id, &row.class_name, row_number,
                &distribution_store_, &streams_.Stream(I_CLASS),
                &hierarchy_state_);
  ChangeSCDValue(&row.class_id, &old_values_.class_id, &change_flags,
                 first_record);

  HierarchyItem(I_BRAND, &row.brand_id, &row.brand, row_number,
                &distribution_store_, &streams_.Stream(I_BRAND),
                &hierarchy_state_);
  ChangeSCDValue(&row.brand_id, &old_values_.brand_id, &change_flags,
                 first_record);

  if (row.category_id != 0) {
    const auto& categories = distribution_store_.Get("categories");
    int use_size = categories.GetInt(static_cast<int>(row.category_id), 3);
    const auto& sizes = distribution_store_.Get("sizes");
    int size_index = sizes.PickIndex(use_size + 2, &streams_.Stream(I_SIZE));
    row.size = sizes.GetString(size_index, 1);
    ChangeSCDValuePtr(&row.size, &old_values_.size, &change_flags,
                      first_record);
  } else {
    row.size.clear();
  }

  const auto& manufact_dist = distribution_store_.Get("i_manufact_id");
  int manufact_index =
      manufact_dist.PickIndex(1, &streams_.Stream(I_MANUFACT_ID));
  int manufact_min = manufact_dist.GetInt(manufact_index, 2);
  int manufact_max = manufact_dist.GetInt(manufact_index, 3);
  row.manufact_id = GenerateUniformRandomInt(manufact_min, manufact_max,
                                             &streams_.Stream(I_MANUFACT_ID));
  ChangeSCDValue(&row.manufact_id, &old_values_.manufact_id, &change_flags,
                 first_record);

  MakeWord(&row.manufact, "syllables", row.manufact_id, RS_I_MANUFACT,
           &distribution_store_);
  ChangeSCDValue(&row.manufact, &old_values_.manufact, &change_flags,
                 first_record);

  row.formulation =
      GenerateRandomCharset("0123456789", RS_I_FORMULATION, RS_I_FORMULATION,
                            &streams_.Stream(I_FORMULATION));
  EmbedString(&row.formulation, "colors", 1, 2, &distribution_store_,
              &streams_.Stream(I_FORMULATION));
  ChangeSCDValue(&row.formulation, &old_values_.formulation, &change_flags,
                 first_record);

  const auto& colors = distribution_store_.Get("colors");
  int color_index = colors.PickIndex(2, &streams_.Stream(I_COLOR));
  row.color = colors.GetString(color_index, 1);
  ChangeSCDValuePtr(&row.color, &old_values_.color, &change_flags,
                    first_record);

  const auto& units = distribution_store_.Get("units");
  int unit_index = units.PickIndex(1, &streams_.Stream(I_UNITS));
  row.units = units.GetString(unit_index, 1);
  ChangeSCDValuePtr(&row.units, &old_values_.units, &change_flags,
                    first_record);

  const auto& container = distribution_store_.Get("container");
  int container_index = container.PickIndex(1, &streams_.Stream(ITEM));
  row.container = container.GetString(container_index, 1);
  ChangeSCDValuePtr(&row.container, &old_values_.container, &change_flags,
                    first_record);

  MakeWord(&row.product_name, "syllables", row_number, RS_I_PRODUCT_NAME,
           &distribution_store_);

  row.promo_sk =
      MakeJoin(I_PROMO_SK, PROMOTION, 1, &streams_.Stream(I_PROMO_SK), scaling_,
               &distribution_store_);
  int promo_value =
      GenerateUniformRandomInt(1, 100, &streams_.Stream(I_PROMO_SK));
  if (promo_value > I_PROMO_PERCENTAGE) {
    row.promo_sk = -1;
  }

  if (first_record || !old_values_initialized_) {
    old_values_initialized_ = true;
    old_values_.item_id = row.item_id;
    old_values_.rec_start_date_id = row.rec_start_date_id;
    old_values_.rec_end_date_id = row.rec_end_date_id;
  }

  return row;
}

void ItemRowGenerator::ConsumeRemainingSeedsForRow() {
  streams_.ConsumeRemainingSeedsForRow();
}

std::vector<int> ItemRowGenerator::ColumnIds() {
  std::vector<int> ids;
  ids.reserve(static_cast<size_t>(ITEM_END - ITEM_START + 2));
  for (int column = ITEM_START; column <= ITEM_END; ++column) {
    ids.push_back(column);
  }
  ids.push_back(ITEM);
  return ids;
}

}  // namespace benchgen::tpcds::internal
