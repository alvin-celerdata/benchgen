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

#include "generators/promotion_generator.h"

#include <algorithm>
#include <stdexcept>

#include "distribution/scaling.h"
#include "generators/promotion_row_generator.h"
#include "util/column_selection.h"
#include "utils/columns.h"
#include "utils/null_utils.h"
#include "utils/tables.h"

namespace benchgen::tpcds {
namespace {

std::shared_ptr<arrow::Schema> BuildPromotionSchema() {
  return arrow::schema({
      arrow::field("p_promo_sk", arrow::int64(), false),
      arrow::field("p_promo_id", arrow::utf8(), false),
      arrow::field("p_start_date_sk", arrow::int32()),
      arrow::field("p_end_date_sk", arrow::int32()),
      arrow::field("p_item_sk", arrow::int64()),
      arrow::field("p_cost", arrow::smallest_decimal(9, 2)),
      arrow::field("p_response_target", arrow::int32()),
      arrow::field("p_promo_name", arrow::utf8()),
      arrow::field("p_channel_dmail", arrow::boolean()),
      arrow::field("p_channel_email", arrow::boolean()),
      arrow::field("p_channel_catalog", arrow::boolean()),
      arrow::field("p_channel_tv", arrow::boolean()),
      arrow::field("p_channel_radio", arrow::boolean()),
      arrow::field("p_channel_press", arrow::boolean()),
      arrow::field("p_channel_event", arrow::boolean()),
      arrow::field("p_channel_demo", arrow::boolean()),
      arrow::field("p_channel_details", arrow::utf8()),
      arrow::field("p_purpose", arrow::utf8()),
      arrow::field("p_discount_active", arrow::boolean()),
  });
}

}  // namespace

struct PromotionGenerator::Impl {
  explicit Impl(GeneratorOptions options)
      : options_(std::move(options)),
        schema_(BuildPromotionSchema()),
        row_generator_(options_.scale_factor) {
    if (options_.chunk_size <= 0) {
      throw std::invalid_argument("chunk_size must be positive");
    }
    auto status = column_selection_.Init(schema_, options_.column_names);
    if (!status.ok()) {
      throw std::invalid_argument(status.ToString());
    }
    schema_ = column_selection_.schema();
    total_rows_ =
        internal::Scaling(options_.scale_factor)
            .RowCountByTableNumber(PROMOTION);
    if (options_.start_row < 0) {
      throw std::invalid_argument("start_row must be non-negative");
    }
    if (options_.start_row >= total_rows_) {
      remaining_rows_ = 0;
      current_row_ = options_.start_row;
      return;
    }
    current_row_ = options_.start_row;
    if (options_.row_count < 0) {
      remaining_rows_ = total_rows_ - options_.start_row;
    } else {
      remaining_rows_ =
          std::min(options_.row_count, total_rows_ - options_.start_row);
    }
    row_generator_.SkipRows(options_.start_row);
  }

  GeneratorOptions options_;
  int64_t total_rows_ = 0;
  int64_t remaining_rows_ = 0;
  int64_t current_row_ = 0;
  std::shared_ptr<arrow::Schema> schema_;
  ::benchgen::internal::ColumnSelection column_selection_;
  internal::PromotionRowGenerator row_generator_;
};

PromotionGenerator::PromotionGenerator(GeneratorOptions options)
    : impl_(std::make_unique<Impl>(std::move(options))) {}

PromotionGenerator::~PromotionGenerator() = default;

std::shared_ptr<arrow::Schema> PromotionGenerator::schema() const {
  return impl_->schema_;
}

std::string_view PromotionGenerator::name() const {
  return TableIdToString(TableId::kPromotion);
}

std::string_view PromotionGenerator::suite_name() const { return "tpcds"; }

arrow::Status PromotionGenerator::Next(
    std::shared_ptr<arrow::RecordBatch>* out) {
  if (impl_->remaining_rows_ == 0) {
    *out = nullptr;
    return arrow::Status::OK();
  }

  const int64_t batch_rows =
      std::min(impl_->remaining_rows_, impl_->options_.chunk_size);

  arrow::MemoryPool* pool = arrow::default_memory_pool();
  arrow::Int64Builder p_promo_sk(pool);
  arrow::StringBuilder p_promo_id(pool);
  arrow::Int32Builder p_start_date_id(pool);
  arrow::Int32Builder p_end_date_id(pool);
  arrow::Int64Builder p_item_sk(pool);
  arrow::Decimal32Builder p_cost(arrow::smallest_decimal(9, 2), pool);
  arrow::Int32Builder p_response_target(pool);
  arrow::StringBuilder p_promo_name(pool);
  arrow::BooleanBuilder p_channel_dmail(pool);
  arrow::BooleanBuilder p_channel_email(pool);
  arrow::BooleanBuilder p_channel_catalog(pool);
  arrow::BooleanBuilder p_channel_tv(pool);
  arrow::BooleanBuilder p_channel_radio(pool);
  arrow::BooleanBuilder p_channel_press(pool);
  arrow::BooleanBuilder p_channel_event(pool);
  arrow::BooleanBuilder p_channel_demo(pool);
  arrow::StringBuilder p_channel_details(pool);
  arrow::StringBuilder p_purpose(pool);
  arrow::BooleanBuilder p_discount_active(pool);

#define TPCDS_RETURN_NOT_OK(status)   \
  do {                                \
    arrow::Status _status = (status); \
    if (!_status.ok()) {              \
      return _status;                 \
    }                                 \
  } while (false)

  TPCDS_RETURN_NOT_OK(p_promo_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(p_promo_id.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(p_start_date_id.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(p_end_date_id.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(p_item_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(p_cost.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(p_response_target.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(p_promo_name.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(p_channel_dmail.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(p_channel_email.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(p_channel_catalog.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(p_channel_tv.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(p_channel_radio.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(p_channel_press.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(p_channel_event.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(p_channel_demo.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(p_channel_details.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(p_purpose.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(p_discount_active.Reserve(batch_rows));

  for (int64_t i = 0; i < batch_rows; ++i) {
    int64_t row_number = impl_->current_row_ + 1;
    internal::PromotionRowData row =
        impl_->row_generator_.GenerateRow(row_number);

    auto is_null = [&](int column_id) {
      return internal::IsNull(row.null_bitmap, PROMOTION, column_id);
    };

    if (is_null(P_PROMO_SK)) {
      TPCDS_RETURN_NOT_OK(p_promo_sk.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(p_promo_sk.Append(row.promo_sk));
    }

    if (is_null(P_PROMO_ID)) {
      TPCDS_RETURN_NOT_OK(p_promo_id.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(p_promo_id.Append(row.promo_id));
    }

    if (is_null(P_START_DATE_ID)) {
      TPCDS_RETURN_NOT_OK(p_start_date_id.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(p_start_date_id.Append(row.start_date_id));
    }

    if (is_null(P_END_DATE_ID)) {
      TPCDS_RETURN_NOT_OK(p_end_date_id.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(p_end_date_id.Append(row.end_date_id));
    }

    if (is_null(P_ITEM_SK)) {
      TPCDS_RETURN_NOT_OK(p_item_sk.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(p_item_sk.Append(row.item_sk));
    }

    if (is_null(P_COST)) {
      TPCDS_RETURN_NOT_OK(p_cost.AppendNull());
    } else {
      arrow::Decimal32 cost_val(row.cost.number);
      TPCDS_RETURN_NOT_OK(p_cost.Append(cost_val));
    }

    if (is_null(P_RESPONSE_TARGET)) {
      TPCDS_RETURN_NOT_OK(p_response_target.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(p_response_target.Append(row.response_target));
    }

    if (is_null(P_PROMO_NAME)) {
      TPCDS_RETURN_NOT_OK(p_promo_name.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(p_promo_name.Append(row.promo_name));
    }

    if (is_null(P_CHANNEL_DMAIL)) {
      TPCDS_RETURN_NOT_OK(p_channel_dmail.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(p_channel_dmail.Append(row.channel_dmail));
    }

    if (is_null(P_CHANNEL_EMAIL)) {
      TPCDS_RETURN_NOT_OK(p_channel_email.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(p_channel_email.Append(row.channel_email));
    }

    if (is_null(P_CHANNEL_CATALOG)) {
      TPCDS_RETURN_NOT_OK(p_channel_catalog.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(p_channel_catalog.Append(row.channel_catalog));
    }

    if (is_null(P_CHANNEL_TV)) {
      TPCDS_RETURN_NOT_OK(p_channel_tv.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(p_channel_tv.Append(row.channel_tv));
    }

    if (is_null(P_CHANNEL_RADIO)) {
      TPCDS_RETURN_NOT_OK(p_channel_radio.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(p_channel_radio.Append(row.channel_radio));
    }

    if (is_null(P_CHANNEL_PRESS)) {
      TPCDS_RETURN_NOT_OK(p_channel_press.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(p_channel_press.Append(row.channel_press));
    }

    if (is_null(P_CHANNEL_EVENT)) {
      TPCDS_RETURN_NOT_OK(p_channel_event.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(p_channel_event.Append(row.channel_event));
    }

    if (is_null(P_CHANNEL_DEMO)) {
      TPCDS_RETURN_NOT_OK(p_channel_demo.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(p_channel_demo.Append(row.channel_demo));
    }

    if (is_null(P_CHANNEL_DETAILS)) {
      TPCDS_RETURN_NOT_OK(p_channel_details.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(p_channel_details.Append(row.channel_details));
    }

    if (is_null(P_PURPOSE)) {
      TPCDS_RETURN_NOT_OK(p_purpose.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(p_purpose.Append(row.purpose));
    }

    if (is_null(P_DISCOUNT_ACTIVE)) {
      TPCDS_RETURN_NOT_OK(p_discount_active.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(p_discount_active.Append(row.discount_active));
    }

    impl_->row_generator_.ConsumeRemainingSeedsForRow();
    ++impl_->current_row_;
    --impl_->remaining_rows_;
  }

  std::vector<std::shared_ptr<arrow::Array>> arrays;
  arrays.reserve(19);
  std::shared_ptr<arrow::Array> array;

  TPCDS_RETURN_NOT_OK(p_promo_sk.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(p_promo_id.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(p_start_date_id.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(p_end_date_id.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(p_item_sk.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(p_cost.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(p_response_target.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(p_promo_name.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(p_channel_dmail.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(p_channel_email.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(p_channel_catalog.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(p_channel_tv.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(p_channel_radio.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(p_channel_press.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(p_channel_event.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(p_channel_demo.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(p_channel_details.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(p_purpose.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(p_discount_active.Finish(&array));
  arrays.push_back(array);

  return impl_->column_selection_.MakeRecordBatch(batch_rows, std::move(arrays),
                                                  out);
}

int64_t PromotionGenerator::total_rows() const { return impl_->total_rows_; }

int64_t PromotionGenerator::remaining_rows() const {
  return impl_->remaining_rows_;
}

int64_t PromotionGenerator::TotalRows(double scale_factor) {
  return internal::Scaling(scale_factor).RowCountByTableNumber(PROMOTION);
}

}  // namespace benchgen::tpcds
