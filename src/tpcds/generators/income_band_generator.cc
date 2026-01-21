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

#include "generators/income_band_generator.h"

#include <algorithm>

#include "benchgen/table.h"
#include "distribution/scaling.h"
#include "generators/income_band_row_generator.h"
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

std::shared_ptr<arrow::Schema> BuildIncomeBandSchema() {
  return arrow::schema({
      arrow::field("ib_income_band_sk", arrow::int64(), false),
      arrow::field("ib_lower_bound", arrow::int32()),
      arrow::field("ib_upper_bound", arrow::int32()),
  });
}

}  // namespace

struct IncomeBandGenerator::Impl {
  explicit Impl(GeneratorOptions options)
      : options_(std::move(options)),
        schema_(BuildIncomeBandSchema()),
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
            .RowCount(TableId::kIncomeBand);
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
  internal::IncomeBandRowGenerator row_generator_;
};

IncomeBandGenerator::IncomeBandGenerator(GeneratorOptions options)
    : impl_(std::make_unique<Impl>(std::move(options))) {}

arrow::Status IncomeBandGenerator::Init() { return impl_->Init(); }

IncomeBandGenerator::~IncomeBandGenerator() = default;

std::shared_ptr<arrow::Schema> IncomeBandGenerator::schema() const {
  return impl_->schema_;
}

std::string_view IncomeBandGenerator::name() const {
  return TableIdToString(TableId::kIncomeBand);
}

std::string_view IncomeBandGenerator::suite_name() const { return "tpcds"; }

arrow::Status IncomeBandGenerator::Next(
    std::shared_ptr<arrow::RecordBatch>* out) {
  if (impl_->remaining_rows_ == 0) {
    *out = nullptr;
    return arrow::Status::OK();
  }

  const int64_t batch_rows =
      std::min(impl_->remaining_rows_, impl_->options_.chunk_size);

  arrow::MemoryPool* pool = arrow::default_memory_pool();
  arrow::Int64Builder ib_income_band_sk(pool);
  arrow::Int32Builder ib_lower_bound(pool);
  arrow::Int32Builder ib_upper_bound(pool);

  TPCDS_RETURN_NOT_OK(ib_income_band_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(ib_lower_bound.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(ib_upper_bound.Reserve(batch_rows));

  for (int64_t i = 0; i < batch_rows; ++i) {
    int64_t row_number = impl_->current_row_ + 1;
    internal::IncomeBandRowData row =
        impl_->row_generator_.GenerateRow(row_number);

    TPCDS_RETURN_NOT_OK(ib_income_band_sk.Append(row.income_band_sk));
    TPCDS_RETURN_NOT_OK(ib_lower_bound.Append(row.lower_bound));
    TPCDS_RETURN_NOT_OK(ib_upper_bound.Append(row.upper_bound));

    ++impl_->current_row_;
    --impl_->remaining_rows_;
  }

  std::shared_ptr<arrow::Array> ib_income_band_sk_array;
  std::shared_ptr<arrow::Array> ib_lower_bound_array;
  std::shared_ptr<arrow::Array> ib_upper_bound_array;

  TPCDS_RETURN_NOT_OK(ib_income_band_sk.Finish(&ib_income_band_sk_array));
  TPCDS_RETURN_NOT_OK(ib_lower_bound.Finish(&ib_lower_bound_array));
  TPCDS_RETURN_NOT_OK(ib_upper_bound.Finish(&ib_upper_bound_array));

  std::vector<std::shared_ptr<arrow::Array>> arrays = {
      ib_income_band_sk_array, ib_lower_bound_array, ib_upper_bound_array};

  return impl_->column_selection_.MakeRecordBatch(batch_rows, std::move(arrays),
                                                  out);
}

int64_t IncomeBandGenerator::total_rows() const { return impl_->total_rows_; }

int64_t IncomeBandGenerator::remaining_rows() const {
  return impl_->remaining_rows_;
}

int64_t IncomeBandGenerator::TotalRows(double scale_factor) {
  return internal::Scaling(scale_factor).RowCount(TableId::kIncomeBand);
}

}  // namespace benchgen::tpcds
