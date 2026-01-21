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

#include "generators/web_page_generator.h"

#include <algorithm>
#include <stdexcept>

#include "distribution/scaling.h"
#include "generators/web_page_row_generator.h"
#include "util/column_selection.h"
#include "utils/columns.h"
#include "utils/date.h"
#include "utils/null_utils.h"
#include "utils/tables.h"

namespace benchgen::tpcds {
namespace {

int32_t Date32FromJulian(int32_t julian) {
  return internal::Date::DaysSinceEpoch(internal::Date::FromJulianDays(julian));
}

std::shared_ptr<arrow::Schema> BuildWebPageSchema() {
  return arrow::schema({
      arrow::field("wp_web_page_sk", arrow::int64(), false),
      arrow::field("wp_web_page_id", arrow::utf8(), false),
      arrow::field("wp_rec_start_date", arrow::date32()),
      arrow::field("wp_rec_end_date", arrow::date32()),
      arrow::field("wp_creation_date_sk", arrow::int32()),
      arrow::field("wp_access_date_sk", arrow::int32()),
      arrow::field("wp_autogen_flag", arrow::boolean()),
      arrow::field("wp_customer_sk", arrow::int64()),
      arrow::field("wp_url", arrow::utf8()),
      arrow::field("wp_type", arrow::utf8()),
      arrow::field("wp_char_count", arrow::int32()),
      arrow::field("wp_link_count", arrow::int32()),
      arrow::field("wp_image_count", arrow::int32()),
      arrow::field("wp_max_ad_count", arrow::int32()),
  });
}

}  // namespace

struct WebPageGenerator::Impl {
  explicit Impl(GeneratorOptions options)
      : options_(std::move(options)),
        schema_(BuildWebPageSchema()),
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
            .RowCountByTableNumber(WEB_PAGE);
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
  internal::WebPageRowGenerator row_generator_;
};

WebPageGenerator::WebPageGenerator(GeneratorOptions options)
    : impl_(std::make_unique<Impl>(std::move(options))) {}

WebPageGenerator::~WebPageGenerator() = default;

std::shared_ptr<arrow::Schema> WebPageGenerator::schema() const {
  return impl_->schema_;
}

std::string_view WebPageGenerator::name() const {
  return TableIdToString(TableId::kWebPage);
}

std::string_view WebPageGenerator::suite_name() const { return "tpcds"; }

arrow::Status WebPageGenerator::Next(std::shared_ptr<arrow::RecordBatch>* out) {
  if (impl_->remaining_rows_ == 0) {
    *out = nullptr;
    return arrow::Status::OK();
  }

  const int64_t batch_rows =
      std::min(impl_->remaining_rows_, impl_->options_.chunk_size);

  arrow::MemoryPool* pool = arrow::default_memory_pool();
  arrow::Int64Builder wp_page_sk(pool);
  arrow::StringBuilder wp_page_id(pool);
  arrow::Date32Builder wp_rec_start_date_id(pool);
  arrow::Date32Builder wp_rec_end_date_id(pool);
  arrow::Int32Builder wp_creation_date_sk(pool);
  arrow::Int32Builder wp_access_date_sk(pool);
  arrow::BooleanBuilder wp_autogen_flag(pool);
  arrow::Int64Builder wp_customer_sk(pool);
  arrow::StringBuilder wp_url(pool);
  arrow::StringBuilder wp_type(pool);
  arrow::Int32Builder wp_char_count(pool);
  arrow::Int32Builder wp_link_count(pool);
  arrow::Int32Builder wp_image_count(pool);
  arrow::Int32Builder wp_max_ad_count(pool);

#define TPCDS_RETURN_NOT_OK(status)   \
  do {                                \
    arrow::Status _status = (status); \
    if (!_status.ok()) {              \
      return _status;                 \
    }                                 \
  } while (false)

  TPCDS_RETURN_NOT_OK(wp_page_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(wp_page_id.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(wp_rec_start_date_id.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(wp_rec_end_date_id.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(wp_creation_date_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(wp_access_date_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(wp_autogen_flag.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(wp_customer_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(wp_url.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(wp_type.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(wp_char_count.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(wp_link_count.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(wp_image_count.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(wp_max_ad_count.Reserve(batch_rows));

  for (int64_t i = 0; i < batch_rows; ++i) {
    int64_t row_number = impl_->current_row_ + 1;
    internal::WebPageRowData row =
        impl_->row_generator_.GenerateRow(row_number);

    auto is_null = [&](int column_id) {
      return internal::IsNull(row.null_bitmap, WEB_PAGE, column_id);
    };

    if (is_null(WP_PAGE_SK)) {
      TPCDS_RETURN_NOT_OK(wp_page_sk.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(wp_page_sk.Append(row.page_sk));
    }

    if (is_null(WP_PAGE_ID)) {
      TPCDS_RETURN_NOT_OK(wp_page_id.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(wp_page_id.Append(row.page_id));
    }

    if (is_null(WP_REC_START_DATE_ID) || row.rec_start_date_id <= 0) {
      TPCDS_RETURN_NOT_OK(wp_rec_start_date_id.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(
          wp_rec_start_date_id.Append(Date32FromJulian(row.rec_start_date_id)));
    }

    if (is_null(WP_REC_END_DATE_ID) || row.rec_end_date_id <= 0) {
      TPCDS_RETURN_NOT_OK(wp_rec_end_date_id.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(
          wp_rec_end_date_id.Append(Date32FromJulian(row.rec_end_date_id)));
    }

    if (is_null(WP_CREATION_DATE_SK) || row.creation_date_sk == -1) {
      TPCDS_RETURN_NOT_OK(wp_creation_date_sk.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(wp_creation_date_sk.Append(row.creation_date_sk));
    }

    if (is_null(WP_ACCESS_DATE_SK) || row.access_date_sk == -1) {
      TPCDS_RETURN_NOT_OK(wp_access_date_sk.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(wp_access_date_sk.Append(row.access_date_sk));
    }

    if (is_null(WP_AUTOGEN_FLAG)) {
      TPCDS_RETURN_NOT_OK(wp_autogen_flag.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(wp_autogen_flag.Append(row.autogen_flag));
    }

    if (is_null(WP_CUSTOMER_SK) || row.customer_sk == -1) {
      TPCDS_RETURN_NOT_OK(wp_customer_sk.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(wp_customer_sk.Append(row.customer_sk));
    }

    if (is_null(WP_URL)) {
      TPCDS_RETURN_NOT_OK(wp_url.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(wp_url.Append(row.url));
    }

    if (is_null(WP_TYPE)) {
      TPCDS_RETURN_NOT_OK(wp_type.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(wp_type.Append(row.type));
    }

    if (is_null(WP_CHAR_COUNT)) {
      TPCDS_RETURN_NOT_OK(wp_char_count.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(wp_char_count.Append(row.char_count));
    }

    if (is_null(WP_LINK_COUNT)) {
      TPCDS_RETURN_NOT_OK(wp_link_count.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(wp_link_count.Append(row.link_count));
    }

    if (is_null(WP_IMAGE_COUNT)) {
      TPCDS_RETURN_NOT_OK(wp_image_count.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(wp_image_count.Append(row.image_count));
    }

    if (is_null(WP_MAX_AD_COUNT)) {
      TPCDS_RETURN_NOT_OK(wp_max_ad_count.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(wp_max_ad_count.Append(row.max_ad_count));
    }

    impl_->row_generator_.ConsumeRemainingSeedsForRow();
    ++impl_->current_row_;
    --impl_->remaining_rows_;
  }

  std::vector<std::shared_ptr<arrow::Array>> arrays;
  arrays.reserve(14);
  std::shared_ptr<arrow::Array> array;

  TPCDS_RETURN_NOT_OK(wp_page_sk.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(wp_page_id.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(wp_rec_start_date_id.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(wp_rec_end_date_id.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(wp_creation_date_sk.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(wp_access_date_sk.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(wp_autogen_flag.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(wp_customer_sk.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(wp_url.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(wp_type.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(wp_char_count.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(wp_link_count.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(wp_image_count.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(wp_max_ad_count.Finish(&array));
  arrays.push_back(array);

  return impl_->column_selection_.MakeRecordBatch(batch_rows, std::move(arrays),
                                                  out);
}

int64_t WebPageGenerator::total_rows() const { return impl_->total_rows_; }

int64_t WebPageGenerator::remaining_rows() const {
  return impl_->remaining_rows_;
}

int64_t WebPageGenerator::TotalRows(double scale_factor) {
  return internal::Scaling(scale_factor).RowCountByTableNumber(WEB_PAGE);
}

}  // namespace benchgen::tpcds
