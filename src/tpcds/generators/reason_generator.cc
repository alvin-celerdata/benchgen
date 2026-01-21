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

#include "generators/reason_generator.h"

#include <algorithm>

#include "benchgen/table.h"
#include "distribution/scaling.h"
#include "generators/reason_row_generator.h"
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

std::shared_ptr<arrow::Schema> BuildReasonSchema() {
  return arrow::schema({
      arrow::field("r_reason_sk", arrow::int64(), false),
      arrow::field("r_reason_id", arrow::utf8(), false),
      arrow::field("r_reason_desc", arrow::utf8()),
  });
}

}  // namespace

struct ReasonGenerator::Impl {
  explicit Impl(GeneratorOptions options)
      : options_(std::move(options)),
        schema_(BuildReasonSchema()),
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
            .RowCount(TableId::kReason);
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
  internal::ReasonRowGenerator row_generator_;
};

ReasonGenerator::ReasonGenerator(GeneratorOptions options)
    : impl_(std::make_unique<Impl>(std::move(options))) {}

arrow::Status ReasonGenerator::Init() { return impl_->Init(); }

ReasonGenerator::~ReasonGenerator() = default;

std::shared_ptr<arrow::Schema> ReasonGenerator::schema() const {
  return impl_->schema_;
}

std::string_view ReasonGenerator::name() const {
  return TableIdToString(TableId::kReason);
}

std::string_view ReasonGenerator::suite_name() const { return "tpcds"; }

arrow::Status ReasonGenerator::Next(std::shared_ptr<arrow::RecordBatch>* out) {
  if (impl_->remaining_rows_ == 0) {
    *out = nullptr;
    return arrow::Status::OK();
  }

  const int64_t batch_rows =
      std::min(impl_->remaining_rows_, impl_->options_.chunk_size);

  arrow::MemoryPool* pool = arrow::default_memory_pool();
  arrow::Int64Builder r_reason_sk(pool);
  arrow::StringBuilder r_reason_id(pool);
  arrow::StringBuilder r_reason_desc(pool);

  TPCDS_RETURN_NOT_OK(r_reason_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(r_reason_id.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(r_reason_desc.Reserve(batch_rows));

  for (int64_t i = 0; i < batch_rows; ++i) {
    int64_t row_number = impl_->current_row_ + 1;
    internal::ReasonRowData row = impl_->row_generator_.GenerateRow(row_number);

    TPCDS_RETURN_NOT_OK(r_reason_sk.Append(row.reason_sk));
    TPCDS_RETURN_NOT_OK(r_reason_id.Append(row.reason_id));
    TPCDS_RETURN_NOT_OK(r_reason_desc.Append(row.reason_description));

    ++impl_->current_row_;
    --impl_->remaining_rows_;
  }

  std::shared_ptr<arrow::Array> r_reason_sk_array;
  std::shared_ptr<arrow::Array> r_reason_id_array;
  std::shared_ptr<arrow::Array> r_reason_desc_array;

  TPCDS_RETURN_NOT_OK(r_reason_sk.Finish(&r_reason_sk_array));
  TPCDS_RETURN_NOT_OK(r_reason_id.Finish(&r_reason_id_array));
  TPCDS_RETURN_NOT_OK(r_reason_desc.Finish(&r_reason_desc_array));

  std::vector<std::shared_ptr<arrow::Array>> arrays = {
      r_reason_sk_array, r_reason_id_array, r_reason_desc_array};

  return impl_->column_selection_.MakeRecordBatch(batch_rows, std::move(arrays),
                                                  out);
}

int64_t ReasonGenerator::total_rows() const { return impl_->total_rows_; }

int64_t ReasonGenerator::remaining_rows() const {
  return impl_->remaining_rows_;
}

int64_t ReasonGenerator::TotalRows(double scale_factor) {
  return internal::Scaling(scale_factor).RowCount(TableId::kReason);
}

}  // namespace benchgen::tpcds
