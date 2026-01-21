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

#include "generators/date_generator.h"

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include "benchgen/arrow_compat.h"
#include "benchgen/generator_options.h"
#include "benchgen/table.h"
#include "generators/date_row_generator.h"
#include "util/column_selection.h"
#include "utils/scaling.h"

namespace benchgen::ssb {
namespace {

#define SSB_RETURN_NOT_OK(status)       \
  do {                                  \
    ::arrow::Status _status = (status); \
    if (!_status.ok()) {                \
      return _status;                   \
    }                                   \
  } while (false)

std::shared_ptr<arrow::Schema> BuildDateSchema() {
  return arrow::schema({
      arrow::field("d_datekey", arrow::int32(), false),
      arrow::field("d_date", arrow::utf8(), false),
      arrow::field("d_dayofweek", arrow::utf8(), false),
      arrow::field("d_month", arrow::utf8(), false),
      arrow::field("d_year", arrow::int32(), false),
      arrow::field("d_yearmonthnum", arrow::int32(), false),
      arrow::field("d_yearmonth", arrow::utf8(), false),
      arrow::field("d_daynuminweek", arrow::int32(), false),
      arrow::field("d_daynuminmonth", arrow::int32(), false),
      arrow::field("d_daynuminyear", arrow::int32(), false),
      arrow::field("d_monthnuminyear", arrow::int32(), false),
      arrow::field("d_weeknuminyear", arrow::int32(), false),
      arrow::field("d_sellingseason", arrow::utf8(), false),
      arrow::field("d_lastdayinweekfl", arrow::utf8(), false),
      arrow::field("d_lastdayinmonthfl", arrow::utf8(), false),
      arrow::field("d_holidayfl", arrow::utf8(), false),
      arrow::field("d_weekdayfl", arrow::utf8(), false),
  });
}

}  // namespace

struct DateGenerator::Impl {
  explicit Impl(GeneratorOptions options)
      : options_(std::move(options)),
        schema_(BuildDateSchema()),
        row_generator_(options_.scale_factor, options_.seed_mode) {}

  arrow::Status Init() {
    if (options_.chunk_size <= 0) {
      return arrow::Status::Invalid("chunk_size must be positive");
    }
    SSB_RETURN_NOT_OK(row_generator_.Init());

    auto status = column_selection_.Init(schema_, options_.column_names);
    if (!status.ok()) {
      return status;
    }
    schema_ = column_selection_.schema();

    total_rows_ = internal::RowCount(TableId::kDate, options_.scale_factor);
    if (total_rows_ < 0) {
      return arrow::Status::Invalid("failed to compute row count for date");
    }
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

    row_generator_.SkipRows(options_.start_row);
    return arrow::Status::OK();
  }

  GeneratorOptions options_;
  int64_t total_rows_ = 0;
  int64_t remaining_rows_ = 0;
  int64_t current_row_ = 0;
  std::shared_ptr<arrow::Schema> schema_;
  ::benchgen::internal::ColumnSelection column_selection_;
  internal::DateRowGenerator row_generator_;
};

DateGenerator::DateGenerator(GeneratorOptions options)
    : impl_(std::make_unique<Impl>(std::move(options))) {}

DateGenerator::~DateGenerator() = default;

arrow::Status DateGenerator::Init() { return impl_->Init(); }

std::string_view DateGenerator::name() const {
  return TableIdToString(TableId::kDate);
}

std::string_view DateGenerator::suite_name() const { return "ssb"; }

std::shared_ptr<arrow::Schema> DateGenerator::schema() const {
  return impl_->schema_;
}

arrow::Status DateGenerator::Next(std::shared_ptr<arrow::RecordBatch>* out) {
  if (impl_->remaining_rows_ == 0) {
    *out = nullptr;
    return arrow::Status::OK();
  }

  const int64_t batch_rows =
      std::min(impl_->remaining_rows_, impl_->options_.chunk_size);

  arrow::MemoryPool* pool = arrow::default_memory_pool();
  arrow::Int32Builder d_datekey(pool);
  arrow::StringBuilder d_date(pool);
  arrow::StringBuilder d_dayofweek(pool);
  arrow::StringBuilder d_month(pool);
  arrow::Int32Builder d_year(pool);
  arrow::Int32Builder d_yearmonthnum(pool);
  arrow::StringBuilder d_yearmonth(pool);
  arrow::Int32Builder d_daynuminweek(pool);
  arrow::Int32Builder d_daynuminmonth(pool);
  arrow::Int32Builder d_daynuminyear(pool);
  arrow::Int32Builder d_monthnuminyear(pool);
  arrow::Int32Builder d_weeknuminyear(pool);
  arrow::StringBuilder d_sellingseason(pool);
  arrow::StringBuilder d_lastdayinweekfl(pool);
  arrow::StringBuilder d_lastdayinmonthfl(pool);
  arrow::StringBuilder d_holidayfl(pool);
  arrow::StringBuilder d_weekdayfl(pool);

  SSB_RETURN_NOT_OK(d_datekey.Reserve(batch_rows));
  SSB_RETURN_NOT_OK(d_date.Reserve(batch_rows));
  SSB_RETURN_NOT_OK(d_dayofweek.Reserve(batch_rows));
  SSB_RETURN_NOT_OK(d_month.Reserve(batch_rows));
  SSB_RETURN_NOT_OK(d_year.Reserve(batch_rows));
  SSB_RETURN_NOT_OK(d_yearmonthnum.Reserve(batch_rows));
  SSB_RETURN_NOT_OK(d_yearmonth.Reserve(batch_rows));
  SSB_RETURN_NOT_OK(d_daynuminweek.Reserve(batch_rows));
  SSB_RETURN_NOT_OK(d_daynuminmonth.Reserve(batch_rows));
  SSB_RETURN_NOT_OK(d_daynuminyear.Reserve(batch_rows));
  SSB_RETURN_NOT_OK(d_monthnuminyear.Reserve(batch_rows));
  SSB_RETURN_NOT_OK(d_weeknuminyear.Reserve(batch_rows));
  SSB_RETURN_NOT_OK(d_sellingseason.Reserve(batch_rows));
  SSB_RETURN_NOT_OK(d_lastdayinweekfl.Reserve(batch_rows));
  SSB_RETURN_NOT_OK(d_lastdayinmonthfl.Reserve(batch_rows));
  SSB_RETURN_NOT_OK(d_holidayfl.Reserve(batch_rows));
  SSB_RETURN_NOT_OK(d_weekdayfl.Reserve(batch_rows));

  for (int64_t i = 0; i < batch_rows; ++i) {
    int64_t row_number = impl_->current_row_ + 1;
    internal::date_t row;
    impl_->row_generator_.GenerateRow(row_number, &row);

    SSB_RETURN_NOT_OK(d_datekey.Append(static_cast<int32_t>(row.datekey)));
    SSB_RETURN_NOT_OK(d_date.Append(row.date));
    SSB_RETURN_NOT_OK(d_dayofweek.Append(row.dayofweek));
    SSB_RETURN_NOT_OK(d_month.Append(row.month));
    SSB_RETURN_NOT_OK(d_year.Append(static_cast<int32_t>(row.year)));
    SSB_RETURN_NOT_OK(
        d_yearmonthnum.Append(static_cast<int32_t>(row.yearmonthnum)));
    SSB_RETURN_NOT_OK(d_yearmonth.Append(row.yearmonth));
    SSB_RETURN_NOT_OK(
        d_daynuminweek.Append(static_cast<int32_t>(row.daynuminweek)));
    SSB_RETURN_NOT_OK(
        d_daynuminmonth.Append(static_cast<int32_t>(row.daynuminmonth)));
    SSB_RETURN_NOT_OK(
        d_daynuminyear.Append(static_cast<int32_t>(row.daynuminyear)));
    SSB_RETURN_NOT_OK(
        d_monthnuminyear.Append(static_cast<int32_t>(row.monthnuminyear)));
    SSB_RETURN_NOT_OK(
        d_weeknuminyear.Append(static_cast<int32_t>(row.weeknuminyear)));
    SSB_RETURN_NOT_OK(
        d_sellingseason.Append(std::string_view(row.sellingseason, row.slen)));
    SSB_RETURN_NOT_OK(d_lastdayinweekfl.Append(row.lastdayinweekfl));
    SSB_RETURN_NOT_OK(d_lastdayinmonthfl.Append(row.lastdayinmonthfl));
    SSB_RETURN_NOT_OK(d_holidayfl.Append(row.holidayfl));
    SSB_RETURN_NOT_OK(d_weekdayfl.Append(row.weekdayfl));

    ++impl_->current_row_;
  }

  impl_->remaining_rows_ -= batch_rows;

  std::vector<std::shared_ptr<arrow::Array>> columns;
  columns.reserve(17);
  std::shared_ptr<arrow::Array> array;
  SSB_RETURN_NOT_OK(d_datekey.Finish(&array));
  columns.push_back(array);
  SSB_RETURN_NOT_OK(d_date.Finish(&array));
  columns.push_back(array);
  SSB_RETURN_NOT_OK(d_dayofweek.Finish(&array));
  columns.push_back(array);
  SSB_RETURN_NOT_OK(d_month.Finish(&array));
  columns.push_back(array);
  SSB_RETURN_NOT_OK(d_year.Finish(&array));
  columns.push_back(array);
  SSB_RETURN_NOT_OK(d_yearmonthnum.Finish(&array));
  columns.push_back(array);
  SSB_RETURN_NOT_OK(d_yearmonth.Finish(&array));
  columns.push_back(array);
  SSB_RETURN_NOT_OK(d_daynuminweek.Finish(&array));
  columns.push_back(array);
  SSB_RETURN_NOT_OK(d_daynuminmonth.Finish(&array));
  columns.push_back(array);
  SSB_RETURN_NOT_OK(d_daynuminyear.Finish(&array));
  columns.push_back(array);
  SSB_RETURN_NOT_OK(d_monthnuminyear.Finish(&array));
  columns.push_back(array);
  SSB_RETURN_NOT_OK(d_weeknuminyear.Finish(&array));
  columns.push_back(array);
  SSB_RETURN_NOT_OK(d_sellingseason.Finish(&array));
  columns.push_back(array);
  SSB_RETURN_NOT_OK(d_lastdayinweekfl.Finish(&array));
  columns.push_back(array);
  SSB_RETURN_NOT_OK(d_lastdayinmonthfl.Finish(&array));
  columns.push_back(array);
  SSB_RETURN_NOT_OK(d_holidayfl.Finish(&array));
  columns.push_back(array);
  SSB_RETURN_NOT_OK(d_weekdayfl.Finish(&array));
  columns.push_back(array);

  return impl_->column_selection_.MakeRecordBatch(batch_rows,
                                                  std::move(columns), out);
}

}  // namespace benchgen::ssb
