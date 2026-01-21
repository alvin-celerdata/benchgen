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

#include "generators/partsupp_generator.h"

#include <algorithm>

#include "benchgen/arrow_compat.h"
#include "benchgen/table.h"
#include "generators/partsupp_row_generator.h"
#include "util/column_selection.h"

namespace benchgen::tpch {
namespace {

#define TPCH_RETURN_NOT_OK(status)      \
  do {                                  \
    ::arrow::Status _status = (status); \
    if (!_status.ok()) {                \
      return _status;                   \
    }                                   \
  } while (false)

std::shared_ptr<arrow::Schema> BuildPartSuppSchema() {
  auto money_type = arrow::decimal128(15, 2);
  return arrow::schema({
      arrow::field("ps_partkey", arrow::int64(), false),
      arrow::field("ps_suppkey", arrow::int64(), false),
      arrow::field("ps_availqty", arrow::int32(), false),
      arrow::field("ps_supplycost", money_type, false),
      arrow::field("ps_comment", arrow::utf8(), false),
  });
}

}  // namespace

struct PartSuppGenerator::Impl {
  explicit Impl(GeneratorOptions options)
      : options_(std::move(options)),
        schema_(BuildPartSuppSchema()),
        row_generator_(options_.scale_factor, options_.seed_mode) {}

  arrow::Status Init() {
    if (options_.chunk_size <= 0) {
      return arrow::Status::Invalid("chunk_size must be positive");
    }

    auto status = row_generator_.Init();
    if (!status.ok()) {
      return status;
    }

    status = column_selection_.Init(schema_, options_.column_names);
    if (!status.ok()) {
      return status;
    }
    schema_ = column_selection_.schema();

    total_rows_ = row_generator_.total_rows();
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
  internal::PartSuppRowGenerator row_generator_;
};

PartSuppGenerator::PartSuppGenerator(GeneratorOptions options)
    : impl_(std::make_unique<Impl>(std::move(options))) {}

PartSuppGenerator::~PartSuppGenerator() = default;

arrow::Status PartSuppGenerator::Init() { return impl_->Init(); }

std::shared_ptr<arrow::Schema> PartSuppGenerator::schema() const {
  return impl_->schema_;
}

std::string_view PartSuppGenerator::name() const {
  return TableIdToString(TableId::kPartSupp);
}

std::string_view PartSuppGenerator::suite_name() const { return "tpch"; }

arrow::Status PartSuppGenerator::Next(
    std::shared_ptr<arrow::RecordBatch>* out) {
  if (impl_->remaining_rows_ == 0) {
    *out = nullptr;
    return arrow::Status::OK();
  }

  const int64_t batch_rows =
      std::min(impl_->remaining_rows_, impl_->options_.chunk_size);

  arrow::MemoryPool* pool = arrow::default_memory_pool();
  auto money_type = arrow::decimal128(15, 2);
  arrow::Int64Builder ps_partkey(pool);
  arrow::Int64Builder ps_suppkey(pool);
  arrow::Int32Builder ps_availqty(pool);
  arrow::Decimal128Builder ps_supplycost(money_type, pool);
  arrow::StringBuilder ps_comment(pool);

  TPCH_RETURN_NOT_OK(ps_partkey.Reserve(batch_rows));
  TPCH_RETURN_NOT_OK(ps_suppkey.Reserve(batch_rows));
  TPCH_RETURN_NOT_OK(ps_availqty.Reserve(batch_rows));
  TPCH_RETURN_NOT_OK(ps_supplycost.Reserve(batch_rows));
  TPCH_RETURN_NOT_OK(ps_comment.Reserve(batch_rows));

  internal::PartSuppRow row;
  int64_t produced = 0;
  while (produced < batch_rows && impl_->remaining_rows_ > 0) {
    if (!impl_->row_generator_.NextRow(&row)) {
      break;
    }
    TPCH_RETURN_NOT_OK(ps_partkey.Append(row.partkey));
    TPCH_RETURN_NOT_OK(ps_suppkey.Append(row.suppkey));
    TPCH_RETURN_NOT_OK(ps_availqty.Append(row.availqty));
    TPCH_RETURN_NOT_OK(ps_supplycost.Append(arrow::Decimal128(row.supplycost)));
    TPCH_RETURN_NOT_OK(ps_comment.Append(row.comment));

    ++impl_->current_row_;
    --impl_->remaining_rows_;
    ++produced;
  }

  std::vector<std::shared_ptr<arrow::Array>> columns;
  columns.reserve(5);
  std::shared_ptr<arrow::Array> ps_partkey_array;
  std::shared_ptr<arrow::Array> ps_suppkey_array;
  std::shared_ptr<arrow::Array> ps_availqty_array;
  std::shared_ptr<arrow::Array> ps_supplycost_array;
  std::shared_ptr<arrow::Array> ps_comment_array;

  TPCH_RETURN_NOT_OK(ps_partkey.Finish(&ps_partkey_array));
  TPCH_RETURN_NOT_OK(ps_suppkey.Finish(&ps_suppkey_array));
  TPCH_RETURN_NOT_OK(ps_availqty.Finish(&ps_availqty_array));
  TPCH_RETURN_NOT_OK(ps_supplycost.Finish(&ps_supplycost_array));
  TPCH_RETURN_NOT_OK(ps_comment.Finish(&ps_comment_array));

  columns.push_back(ps_partkey_array);
  columns.push_back(ps_suppkey_array);
  columns.push_back(ps_availqty_array);
  columns.push_back(ps_supplycost_array);
  columns.push_back(ps_comment_array);

  return impl_->column_selection_.MakeRecordBatch(produced, std::move(columns),
                                                  out);
}

int64_t PartSuppGenerator::total_rows() const { return impl_->total_rows_; }

int64_t PartSuppGenerator::remaining_rows() const {
  return impl_->remaining_rows_;
}

int64_t PartSuppGenerator::TotalRows(double scale_factor) {
  GeneratorOptions options;
  options.scale_factor = scale_factor;
  internal::PartSuppRowGenerator generator(
      scale_factor, options.seed_mode);
  auto status = generator.Init();
  if (!status.ok()) {
    return -1;
  }
  return generator.total_rows();
}

}  // namespace benchgen::tpch
