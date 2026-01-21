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

#include "generators/date_dim_generator.h"

#include <algorithm>

#include "benchgen/table.h"
#include "distribution/scaling.h"
#include "generators/date_dim_row_generator.h"
#include "util/column_selection.h"

namespace benchgen::tpcds {
namespace {

#define TPCDS_RETURN_NOT_OK(status)     \
  do {                                  \
    ::arrow::Status _status = (status); \
    if (!_status.ok()) {                \
      return _status;                   \
    }                                   \
  } while (false)

std::shared_ptr<arrow::Schema> BuildDateDimSchema() {
  return arrow::schema({
      arrow::field("d_date_sk", arrow::int32(), false),
      arrow::field("d_date_id", arrow::utf8(), false),
      arrow::field("d_date", arrow::date32()),
      arrow::field("d_month_seq", arrow::int32()),
      arrow::field("d_week_seq", arrow::int32()),
      arrow::field("d_quarter_seq", arrow::int32()),
      arrow::field("d_year", arrow::int32()),
      arrow::field("d_dow", arrow::int32()),
      arrow::field("d_moy", arrow::int32()),
      arrow::field("d_dom", arrow::int32()),
      arrow::field("d_qoy", arrow::int32()),
      arrow::field("d_fy_year", arrow::int32()),
      arrow::field("d_fy_quarter_seq", arrow::int32()),
      arrow::field("d_fy_week_seq", arrow::int32()),
      arrow::field("d_day_name", arrow::utf8()),
      arrow::field("d_quarter_name", arrow::utf8()),
      arrow::field("d_holiday", arrow::boolean()),
      arrow::field("d_weekend", arrow::boolean()),
      arrow::field("d_following_holiday", arrow::boolean()),
      arrow::field("d_first_dom", arrow::int32()),
      arrow::field("d_last_dom", arrow::int32()),
      arrow::field("d_same_day_ly", arrow::int32()),
      arrow::field("d_same_day_lq", arrow::int32()),
      arrow::field("d_current_day", arrow::boolean()),
      arrow::field("d_current_week", arrow::boolean()),
      arrow::field("d_current_month", arrow::boolean()),
      arrow::field("d_current_quarter", arrow::boolean()),
      arrow::field("d_current_year", arrow::boolean()),
  });
}

}  // namespace

struct DateDimGenerator::Impl {
  explicit Impl(GeneratorOptions options)
      : options_(std::move(options)),
        schema_(BuildDateDimSchema()),
        row_generator_() {}

  arrow::Status Init() {
    if (options_.chunk_size <= 0) {
      return arrow::Status::Invalid("chunk_size must be positive");
    }
    auto status = column_selection_.Init(schema_, options_.column_names);
    if (!status.ok()) {
      return status;
    }
    schema_ = column_selection_.schema();
    total_rows_ =
        internal::Scaling(options_.scale_factor)
            .RowCount(TableId::kDateDim);
    if (options_.start_row < 0) {
      return arrow::Status::Invalid("start_row must be non-negative");
    }
    if (options_.start_row >= total_rows_) {
      remaining_rows_ = 0;
      current_row_ = options_.start_row;
      return arrow::Status::OK();
    }
    current_row_ = options_.start_row;
    if (options_.row_count < 0) {
      remaining_rows_ = total_rows_ - options_.start_row;
    } else {
      remaining_rows_ =
          std::min(options_.row_count, total_rows_ - options_.start_row);
    }
    return arrow::Status::OK();
  }

  GeneratorOptions options_;
  int64_t total_rows_ = 0;
  int64_t remaining_rows_ = 0;
  int64_t current_row_ = 0;
  std::shared_ptr<arrow::Schema> schema_;
  ::benchgen::internal::ColumnSelection column_selection_;
  internal::DateDimRowGenerator row_generator_;
};

DateDimGenerator::DateDimGenerator(GeneratorOptions options)
    : impl_(std::make_unique<Impl>(std::move(options))) {}

arrow::Status DateDimGenerator::Init() { return impl_->Init(); }

DateDimGenerator::~DateDimGenerator() = default;

std::shared_ptr<arrow::Schema> DateDimGenerator::schema() const {
  return impl_->schema_;
}

std::string_view DateDimGenerator::name() const {
  return TableIdToString(TableId::kDateDim);
}

std::string_view DateDimGenerator::suite_name() const { return "tpcds"; }

arrow::Status DateDimGenerator::Next(std::shared_ptr<arrow::RecordBatch>* out) {
  if (impl_->remaining_rows_ == 0) {
    *out = nullptr;
    return arrow::Status::OK();
  }

  const int64_t batch_rows =
      std::min(impl_->remaining_rows_, impl_->options_.chunk_size);

  arrow::MemoryPool* pool = arrow::default_memory_pool();
  arrow::Int32Builder d_date_sk(pool);
  arrow::StringBuilder d_date_id(pool);
  arrow::Date32Builder d_date(pool);
  arrow::Int32Builder d_month_seq(pool);
  arrow::Int32Builder d_week_seq(pool);
  arrow::Int32Builder d_quarter_seq(pool);
  arrow::Int32Builder d_year(pool);
  arrow::Int32Builder d_dow(pool);
  arrow::Int32Builder d_moy(pool);
  arrow::Int32Builder d_dom(pool);
  arrow::Int32Builder d_qoy(pool);
  arrow::Int32Builder d_fy_year(pool);
  arrow::Int32Builder d_fy_quarter_seq(pool);
  arrow::Int32Builder d_fy_week_seq(pool);
  arrow::StringBuilder d_day_name(pool);
  arrow::StringBuilder d_quarter_name(pool);
  arrow::BooleanBuilder d_holiday(pool);
  arrow::BooleanBuilder d_weekend(pool);
  arrow::BooleanBuilder d_following_holiday(pool);
  arrow::Int32Builder d_first_dom(pool);
  arrow::Int32Builder d_last_dom(pool);
  arrow::Int32Builder d_same_day_ly(pool);
  arrow::Int32Builder d_same_day_lq(pool);
  arrow::BooleanBuilder d_current_day(pool);
  arrow::BooleanBuilder d_current_week(pool);
  arrow::BooleanBuilder d_current_month(pool);
  arrow::BooleanBuilder d_current_quarter(pool);
  arrow::BooleanBuilder d_current_year(pool);

  TPCDS_RETURN_NOT_OK(d_date_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(d_date_id.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(d_date.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(d_month_seq.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(d_week_seq.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(d_quarter_seq.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(d_year.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(d_dow.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(d_moy.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(d_dom.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(d_qoy.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(d_fy_year.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(d_fy_quarter_seq.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(d_fy_week_seq.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(d_day_name.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(d_quarter_name.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(d_holiday.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(d_weekend.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(d_following_holiday.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(d_first_dom.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(d_last_dom.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(d_same_day_ly.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(d_same_day_lq.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(d_current_day.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(d_current_week.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(d_current_month.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(d_current_quarter.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(d_current_year.Reserve(batch_rows));

  for (int64_t i = 0; i < batch_rows; ++i) {
    int64_t row_number = impl_->current_row_ + 1;
    internal::DateDimRowData row =
        impl_->row_generator_.GenerateRow(row_number);

    TPCDS_RETURN_NOT_OK(d_date_sk.Append(row.date_sk));
    TPCDS_RETURN_NOT_OK(d_date_id.Append(row.date_id));
    TPCDS_RETURN_NOT_OK(
        d_date.Append(internal::Date::DaysSinceEpoch(row.date)));
    TPCDS_RETURN_NOT_OK(d_month_seq.Append(row.month_seq));
    TPCDS_RETURN_NOT_OK(d_week_seq.Append(row.week_seq));
    TPCDS_RETURN_NOT_OK(d_quarter_seq.Append(row.quarter_seq));
    TPCDS_RETURN_NOT_OK(d_year.Append(row.year));
    TPCDS_RETURN_NOT_OK(d_dow.Append(row.dow));
    TPCDS_RETURN_NOT_OK(d_moy.Append(row.moy));
    TPCDS_RETURN_NOT_OK(d_dom.Append(row.dom));
    TPCDS_RETURN_NOT_OK(d_qoy.Append(row.qoy));
    TPCDS_RETURN_NOT_OK(d_fy_year.Append(row.fy_year));
    TPCDS_RETURN_NOT_OK(d_fy_quarter_seq.Append(row.fy_quarter_seq));
    TPCDS_RETURN_NOT_OK(d_fy_week_seq.Append(row.fy_week_seq));
    TPCDS_RETURN_NOT_OK(d_day_name.Append(row.day_name));
    TPCDS_RETURN_NOT_OK(d_quarter_name.Append(row.quarter_name));
    TPCDS_RETURN_NOT_OK(d_holiday.Append(row.holiday));
    TPCDS_RETURN_NOT_OK(d_weekend.Append(row.weekend));
    TPCDS_RETURN_NOT_OK(d_following_holiday.Append(row.following_holiday));
    TPCDS_RETURN_NOT_OK(d_first_dom.Append(row.first_dom));
    TPCDS_RETURN_NOT_OK(d_last_dom.Append(row.last_dom));
    TPCDS_RETURN_NOT_OK(d_same_day_ly.Append(row.same_day_ly));
    TPCDS_RETURN_NOT_OK(d_same_day_lq.Append(row.same_day_lq));
    TPCDS_RETURN_NOT_OK(d_current_day.Append(row.current_day));
    TPCDS_RETURN_NOT_OK(d_current_week.Append(row.current_week));
    TPCDS_RETURN_NOT_OK(d_current_month.Append(row.current_month));
    TPCDS_RETURN_NOT_OK(d_current_quarter.Append(row.current_quarter));
    TPCDS_RETURN_NOT_OK(d_current_year.Append(row.current_year));

    ++impl_->current_row_;
    --impl_->remaining_rows_;
  }

  std::shared_ptr<arrow::Array> d_date_sk_array;
  std::shared_ptr<arrow::Array> d_date_id_array;
  std::shared_ptr<arrow::Array> d_date_array;
  std::shared_ptr<arrow::Array> d_month_seq_array;
  std::shared_ptr<arrow::Array> d_week_seq_array;
  std::shared_ptr<arrow::Array> d_quarter_seq_array;
  std::shared_ptr<arrow::Array> d_year_array;
  std::shared_ptr<arrow::Array> d_dow_array;
  std::shared_ptr<arrow::Array> d_moy_array;
  std::shared_ptr<arrow::Array> d_dom_array;
  std::shared_ptr<arrow::Array> d_qoy_array;
  std::shared_ptr<arrow::Array> d_fy_year_array;
  std::shared_ptr<arrow::Array> d_fy_quarter_seq_array;
  std::shared_ptr<arrow::Array> d_fy_week_seq_array;
  std::shared_ptr<arrow::Array> d_day_name_array;
  std::shared_ptr<arrow::Array> d_quarter_name_array;
  std::shared_ptr<arrow::Array> d_holiday_array;
  std::shared_ptr<arrow::Array> d_weekend_array;
  std::shared_ptr<arrow::Array> d_following_holiday_array;
  std::shared_ptr<arrow::Array> d_first_dom_array;
  std::shared_ptr<arrow::Array> d_last_dom_array;
  std::shared_ptr<arrow::Array> d_same_day_ly_array;
  std::shared_ptr<arrow::Array> d_same_day_lq_array;
  std::shared_ptr<arrow::Array> d_current_day_array;
  std::shared_ptr<arrow::Array> d_current_week_array;
  std::shared_ptr<arrow::Array> d_current_month_array;
  std::shared_ptr<arrow::Array> d_current_quarter_array;
  std::shared_ptr<arrow::Array> d_current_year_array;

  TPCDS_RETURN_NOT_OK(d_date_sk.Finish(&d_date_sk_array));
  TPCDS_RETURN_NOT_OK(d_date_id.Finish(&d_date_id_array));
  TPCDS_RETURN_NOT_OK(d_date.Finish(&d_date_array));
  TPCDS_RETURN_NOT_OK(d_month_seq.Finish(&d_month_seq_array));
  TPCDS_RETURN_NOT_OK(d_week_seq.Finish(&d_week_seq_array));
  TPCDS_RETURN_NOT_OK(d_quarter_seq.Finish(&d_quarter_seq_array));
  TPCDS_RETURN_NOT_OK(d_year.Finish(&d_year_array));
  TPCDS_RETURN_NOT_OK(d_dow.Finish(&d_dow_array));
  TPCDS_RETURN_NOT_OK(d_moy.Finish(&d_moy_array));
  TPCDS_RETURN_NOT_OK(d_dom.Finish(&d_dom_array));
  TPCDS_RETURN_NOT_OK(d_qoy.Finish(&d_qoy_array));
  TPCDS_RETURN_NOT_OK(d_fy_year.Finish(&d_fy_year_array));
  TPCDS_RETURN_NOT_OK(d_fy_quarter_seq.Finish(&d_fy_quarter_seq_array));
  TPCDS_RETURN_NOT_OK(d_fy_week_seq.Finish(&d_fy_week_seq_array));
  TPCDS_RETURN_NOT_OK(d_day_name.Finish(&d_day_name_array));
  TPCDS_RETURN_NOT_OK(d_quarter_name.Finish(&d_quarter_name_array));
  TPCDS_RETURN_NOT_OK(d_holiday.Finish(&d_holiday_array));
  TPCDS_RETURN_NOT_OK(d_weekend.Finish(&d_weekend_array));
  TPCDS_RETURN_NOT_OK(d_following_holiday.Finish(&d_following_holiday_array));
  TPCDS_RETURN_NOT_OK(d_first_dom.Finish(&d_first_dom_array));
  TPCDS_RETURN_NOT_OK(d_last_dom.Finish(&d_last_dom_array));
  TPCDS_RETURN_NOT_OK(d_same_day_ly.Finish(&d_same_day_ly_array));
  TPCDS_RETURN_NOT_OK(d_same_day_lq.Finish(&d_same_day_lq_array));
  TPCDS_RETURN_NOT_OK(d_current_day.Finish(&d_current_day_array));
  TPCDS_RETURN_NOT_OK(d_current_week.Finish(&d_current_week_array));
  TPCDS_RETURN_NOT_OK(d_current_month.Finish(&d_current_month_array));
  TPCDS_RETURN_NOT_OK(d_current_quarter.Finish(&d_current_quarter_array));
  TPCDS_RETURN_NOT_OK(d_current_year.Finish(&d_current_year_array));

  std::vector<std::shared_ptr<arrow::Array>> arrays = {
      d_date_sk_array,
      d_date_id_array,
      d_date_array,
      d_month_seq_array,
      d_week_seq_array,
      d_quarter_seq_array,
      d_year_array,
      d_dow_array,
      d_moy_array,
      d_dom_array,
      d_qoy_array,
      d_fy_year_array,
      d_fy_quarter_seq_array,
      d_fy_week_seq_array,
      d_day_name_array,
      d_quarter_name_array,
      d_holiday_array,
      d_weekend_array,
      d_following_holiday_array,
      d_first_dom_array,
      d_last_dom_array,
      d_same_day_ly_array,
      d_same_day_lq_array,
      d_current_day_array,
      d_current_week_array,
      d_current_month_array,
      d_current_quarter_array,
      d_current_year_array};

  return impl_->column_selection_.MakeRecordBatch(batch_rows, std::move(arrays),
                                                  out);
}

int64_t DateDimGenerator::total_rows() const { return impl_->total_rows_; }

int64_t DateDimGenerator::remaining_rows() const {
  return impl_->remaining_rows_;
}

int64_t DateDimGenerator::TotalRows(double scale_factor) {
  return internal::Scaling(scale_factor).RowCount(TableId::kDateDim);
}

}  // namespace benchgen::tpcds
