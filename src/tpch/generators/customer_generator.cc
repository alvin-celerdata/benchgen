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

#include "generators/customer_generator.h"

#include <algorithm>

#include "benchgen/arrow_compat.h"
#include "benchgen/table.h"
#include "generators/customer_row_generator.h"
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

std::shared_ptr<arrow::Schema> BuildCustomerSchema() {
  auto money_type = arrow::decimal128(15, 2);
  return arrow::schema({
      arrow::field("c_custkey", arrow::int64(), false),
      arrow::field("c_name", arrow::utf8(), false),
      arrow::field("c_address", arrow::utf8(), false),
      arrow::field("c_nationkey", arrow::int64(), false),
      arrow::field("c_phone", arrow::utf8(), false),
      arrow::field("c_acctbal", money_type, false),
      arrow::field("c_mktsegment", arrow::utf8(), false),
      arrow::field("c_comment", arrow::utf8(), false),
  });
}

}  // namespace

struct CustomerGenerator::Impl {
  explicit Impl(GeneratorOptions options)
      : options_(std::move(options)),
        schema_(BuildCustomerSchema()),
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
  internal::CustomerRowGenerator row_generator_;
};

CustomerGenerator::CustomerGenerator(GeneratorOptions options)
    : impl_(std::make_unique<Impl>(std::move(options))) {}

CustomerGenerator::~CustomerGenerator() = default;

arrow::Status CustomerGenerator::Init() { return impl_->Init(); }

std::shared_ptr<arrow::Schema> CustomerGenerator::schema() const {
  return impl_->schema_;
}

std::string_view CustomerGenerator::name() const {
  return TableIdToString(TableId::kCustomer);
}

std::string_view CustomerGenerator::suite_name() const { return "tpch"; }

arrow::Status CustomerGenerator::Next(
    std::shared_ptr<arrow::RecordBatch>* out) {
  if (impl_->remaining_rows_ == 0) {
    *out = nullptr;
    return arrow::Status::OK();
  }

  const int64_t batch_rows =
      std::min(impl_->remaining_rows_, impl_->options_.chunk_size);

  arrow::MemoryPool* pool = arrow::default_memory_pool();
  auto money_type = arrow::decimal128(15, 2);
  arrow::Int64Builder c_custkey(pool);
  arrow::StringBuilder c_name(pool);
  arrow::StringBuilder c_address(pool);
  arrow::Int64Builder c_nationkey(pool);
  arrow::StringBuilder c_phone(pool);
  arrow::Decimal128Builder c_acctbal(money_type, pool);
  arrow::StringBuilder c_mktsegment(pool);
  arrow::StringBuilder c_comment(pool);

  TPCH_RETURN_NOT_OK(c_custkey.Reserve(batch_rows));
  TPCH_RETURN_NOT_OK(c_name.Reserve(batch_rows));
  TPCH_RETURN_NOT_OK(c_address.Reserve(batch_rows));
  TPCH_RETURN_NOT_OK(c_nationkey.Reserve(batch_rows));
  TPCH_RETURN_NOT_OK(c_phone.Reserve(batch_rows));
  TPCH_RETURN_NOT_OK(c_acctbal.Reserve(batch_rows));
  TPCH_RETURN_NOT_OK(c_mktsegment.Reserve(batch_rows));
  TPCH_RETURN_NOT_OK(c_comment.Reserve(batch_rows));

  internal::CustomerRow row;
  for (int64_t i = 0; i < batch_rows; ++i) {
    int64_t row_number = impl_->current_row_ + 1;
    impl_->row_generator_.GenerateRow(row_number, &row);

    TPCH_RETURN_NOT_OK(c_custkey.Append(row.custkey));
    TPCH_RETURN_NOT_OK(c_name.Append(row.name));
    TPCH_RETURN_NOT_OK(c_address.Append(row.address));
    TPCH_RETURN_NOT_OK(c_nationkey.Append(row.nationkey));
    TPCH_RETURN_NOT_OK(c_phone.Append(row.phone));
    TPCH_RETURN_NOT_OK(c_acctbal.Append(arrow::Decimal128(row.acctbal)));
    TPCH_RETURN_NOT_OK(c_mktsegment.Append(row.mktsegment));
    TPCH_RETURN_NOT_OK(c_comment.Append(row.comment));

    ++impl_->current_row_;
    --impl_->remaining_rows_;
  }

  std::vector<std::shared_ptr<arrow::Array>> columns;
  columns.reserve(8);
  std::shared_ptr<arrow::Array> c_custkey_array;
  std::shared_ptr<arrow::Array> c_name_array;
  std::shared_ptr<arrow::Array> c_address_array;
  std::shared_ptr<arrow::Array> c_nationkey_array;
  std::shared_ptr<arrow::Array> c_phone_array;
  std::shared_ptr<arrow::Array> c_acctbal_array;
  std::shared_ptr<arrow::Array> c_mktsegment_array;
  std::shared_ptr<arrow::Array> c_comment_array;

  TPCH_RETURN_NOT_OK(c_custkey.Finish(&c_custkey_array));
  TPCH_RETURN_NOT_OK(c_name.Finish(&c_name_array));
  TPCH_RETURN_NOT_OK(c_address.Finish(&c_address_array));
  TPCH_RETURN_NOT_OK(c_nationkey.Finish(&c_nationkey_array));
  TPCH_RETURN_NOT_OK(c_phone.Finish(&c_phone_array));
  TPCH_RETURN_NOT_OK(c_acctbal.Finish(&c_acctbal_array));
  TPCH_RETURN_NOT_OK(c_mktsegment.Finish(&c_mktsegment_array));
  TPCH_RETURN_NOT_OK(c_comment.Finish(&c_comment_array));

  columns.push_back(c_custkey_array);
  columns.push_back(c_name_array);
  columns.push_back(c_address_array);
  columns.push_back(c_nationkey_array);
  columns.push_back(c_phone_array);
  columns.push_back(c_acctbal_array);
  columns.push_back(c_mktsegment_array);
  columns.push_back(c_comment_array);

  return impl_->column_selection_.MakeRecordBatch(batch_rows,
                                                  std::move(columns), out);
}

int64_t CustomerGenerator::total_rows() const { return impl_->total_rows_; }

int64_t CustomerGenerator::remaining_rows() const {
  return impl_->remaining_rows_;
}

int64_t CustomerGenerator::TotalRows(double scale_factor) {
  GeneratorOptions options;
  options.scale_factor = scale_factor;
  internal::CustomerRowGenerator generator(
      scale_factor, options.seed_mode);
  auto status = generator.Init();
  if (!status.ok()) {
    return -1;
  }
  return generator.total_rows();
}

}  // namespace benchgen::tpch
