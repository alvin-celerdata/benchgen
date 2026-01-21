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

#include "generators/time_dim_generator.h"

#include <algorithm>

#include "benchgen/table.h"
#include "distribution/scaling.h"
#include "generators/time_dim_row_generator.h"
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

std::shared_ptr<arrow::Schema> BuildTimeDimSchema() {
  return arrow::schema({
      arrow::field("t_time_sk", arrow::int32(), false),
      arrow::field("t_time_id", arrow::utf8(), false),
      arrow::field("t_time", arrow::int32()),
      arrow::field("t_hour", arrow::int32()),
      arrow::field("t_minute", arrow::int32()),
      arrow::field("t_second", arrow::int32()),
      arrow::field("t_am_pm", arrow::utf8()),
      arrow::field("t_shift", arrow::utf8()),
      arrow::field("t_sub_shift", arrow::utf8()),
      arrow::field("t_meal_time", arrow::utf8()),
  });
}

}  // namespace

struct TimeDimGenerator::Impl {
  explicit Impl(GeneratorOptions options)
      : options_(std::move(options)),
        schema_(BuildTimeDimSchema()),
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
            .RowCount(TableId::kTimeDim);
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
  internal::TimeDimRowGenerator row_generator_;
};

TimeDimGenerator::TimeDimGenerator(GeneratorOptions options)
    : impl_(std::make_unique<Impl>(std::move(options))) {}

arrow::Status TimeDimGenerator::Init() { return impl_->Init(); }

TimeDimGenerator::~TimeDimGenerator() = default;

std::shared_ptr<arrow::Schema> TimeDimGenerator::schema() const {
  return impl_->schema_;
}

std::string_view TimeDimGenerator::name() const {
  return TableIdToString(TableId::kTimeDim);
}

std::string_view TimeDimGenerator::suite_name() const { return "tpcds"; }

arrow::Status TimeDimGenerator::Next(std::shared_ptr<arrow::RecordBatch>* out) {
  if (impl_->remaining_rows_ == 0) {
    *out = nullptr;
    return arrow::Status::OK();
  }

  const int64_t batch_rows =
      std::min(impl_->remaining_rows_, impl_->options_.chunk_size);

  arrow::MemoryPool* pool = arrow::default_memory_pool();
  arrow::Int32Builder t_time_sk(pool);
  arrow::StringBuilder t_time_id(pool);
  arrow::Int32Builder t_time(pool);
  arrow::Int32Builder t_hour(pool);
  arrow::Int32Builder t_minute(pool);
  arrow::Int32Builder t_second(pool);
  arrow::StringBuilder t_am_pm(pool);
  arrow::StringBuilder t_shift(pool);
  arrow::StringBuilder t_sub_shift(pool);
  arrow::StringBuilder t_meal_time(pool);

  TPCDS_RETURN_NOT_OK(t_time_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(t_time_id.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(t_time.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(t_hour.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(t_minute.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(t_second.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(t_am_pm.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(t_shift.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(t_sub_shift.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(t_meal_time.Reserve(batch_rows));

  for (int64_t i = 0; i < batch_rows; ++i) {
    int64_t row_number = impl_->current_row_ + 1;
    internal::TimeDimRowData row =
        impl_->row_generator_.GenerateRow(row_number);

    TPCDS_RETURN_NOT_OK(t_time_sk.Append(row.time_sk));
    TPCDS_RETURN_NOT_OK(t_time_id.Append(row.time_id));
    TPCDS_RETURN_NOT_OK(t_time.Append(row.time));
    TPCDS_RETURN_NOT_OK(t_hour.Append(row.hour));
    TPCDS_RETURN_NOT_OK(t_minute.Append(row.minute));
    TPCDS_RETURN_NOT_OK(t_second.Append(row.second));
    TPCDS_RETURN_NOT_OK(t_am_pm.Append(row.am_pm));
    TPCDS_RETURN_NOT_OK(t_shift.Append(row.shift));
    TPCDS_RETURN_NOT_OK(t_sub_shift.Append(row.sub_shift));
    TPCDS_RETURN_NOT_OK(t_meal_time.Append(row.meal_time));

    ++impl_->current_row_;
    --impl_->remaining_rows_;
  }

  std::shared_ptr<arrow::Array> t_time_sk_array;
  std::shared_ptr<arrow::Array> t_time_id_array;
  std::shared_ptr<arrow::Array> t_time_array;
  std::shared_ptr<arrow::Array> t_hour_array;
  std::shared_ptr<arrow::Array> t_minute_array;
  std::shared_ptr<arrow::Array> t_second_array;
  std::shared_ptr<arrow::Array> t_am_pm_array;
  std::shared_ptr<arrow::Array> t_shift_array;
  std::shared_ptr<arrow::Array> t_sub_shift_array;
  std::shared_ptr<arrow::Array> t_meal_time_array;

  TPCDS_RETURN_NOT_OK(t_time_sk.Finish(&t_time_sk_array));
  TPCDS_RETURN_NOT_OK(t_time_id.Finish(&t_time_id_array));
  TPCDS_RETURN_NOT_OK(t_time.Finish(&t_time_array));
  TPCDS_RETURN_NOT_OK(t_hour.Finish(&t_hour_array));
  TPCDS_RETURN_NOT_OK(t_minute.Finish(&t_minute_array));
  TPCDS_RETURN_NOT_OK(t_second.Finish(&t_second_array));
  TPCDS_RETURN_NOT_OK(t_am_pm.Finish(&t_am_pm_array));
  TPCDS_RETURN_NOT_OK(t_shift.Finish(&t_shift_array));
  TPCDS_RETURN_NOT_OK(t_sub_shift.Finish(&t_sub_shift_array));
  TPCDS_RETURN_NOT_OK(t_meal_time.Finish(&t_meal_time_array));

  std::vector<std::shared_ptr<arrow::Array>> arrays = {
      t_time_sk_array,   t_time_id_array,  t_time_array,  t_hour_array,
      t_minute_array,    t_second_array,   t_am_pm_array, t_shift_array,
      t_sub_shift_array, t_meal_time_array};

  return impl_->column_selection_.MakeRecordBatch(batch_rows, std::move(arrays),
                                                  out);
}

int64_t TimeDimGenerator::total_rows() const { return impl_->total_rows_; }

int64_t TimeDimGenerator::remaining_rows() const {
  return impl_->remaining_rows_;
}

int64_t TimeDimGenerator::TotalRows(double scale_factor) {
  return internal::Scaling(scale_factor).RowCount(TableId::kTimeDim);
}

}  // namespace benchgen::tpcds
