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

#include "generators/orders_generator.h"

#include <algorithm>
#include <string>

#include "benchgen/arrow_compat.h"
#include "benchgen/table.h"
#include "generators/orders_row_generator.h"
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

std::shared_ptr<arrow::Schema> BuildOrdersSchema() {
  auto money_type = arrow::decimal128(15, 2);
  return arrow::schema({
      arrow::field("o_orderkey", arrow::int64(), false),
      arrow::field("o_custkey", arrow::int64(), false),
      arrow::field("o_orderstatus", arrow::utf8(), false),
      arrow::field("o_totalprice", money_type, false),
      arrow::field("o_orderdate", arrow::utf8(), false),
      arrow::field("o_orderpriority", arrow::utf8(), false),
      arrow::field("o_clerk", arrow::utf8(), false),
      arrow::field("o_shippriority", arrow::int32(), false),
      arrow::field("o_comment", arrow::utf8(), false),
  });
}

}  // namespace

struct OrdersGenerator::Impl {
  explicit Impl(GeneratorOptions options)
      : options_(std::move(options)),
        schema_(BuildOrdersSchema()),
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
  internal::OrdersRowGenerator row_generator_;
};

OrdersGenerator::OrdersGenerator(GeneratorOptions options)
    : impl_(std::make_unique<Impl>(std::move(options))) {}

OrdersGenerator::~OrdersGenerator() = default;

arrow::Status OrdersGenerator::Init() { return impl_->Init(); }

std::shared_ptr<arrow::Schema> OrdersGenerator::schema() const {
  return impl_->schema_;
}

std::string_view OrdersGenerator::name() const {
  return TableIdToString(TableId::kOrders);
}

std::string_view OrdersGenerator::suite_name() const { return "tpch"; }

arrow::Status OrdersGenerator::Next(std::shared_ptr<arrow::RecordBatch>* out) {
  if (impl_->remaining_rows_ == 0) {
    *out = nullptr;
    return arrow::Status::OK();
  }

  const int64_t batch_rows =
      std::min(impl_->remaining_rows_, impl_->options_.chunk_size);

  arrow::MemoryPool* pool = arrow::default_memory_pool();
  auto money_type = arrow::decimal128(15, 2);
  arrow::Int64Builder o_orderkey(pool);
  arrow::Int64Builder o_custkey(pool);
  arrow::StringBuilder o_orderstatus(pool);
  arrow::Decimal128Builder o_totalprice(money_type, pool);
  arrow::StringBuilder o_orderdate(pool);
  arrow::StringBuilder o_orderpriority(pool);
  arrow::StringBuilder o_clerk(pool);
  arrow::Int32Builder o_shippriority(pool);
  arrow::StringBuilder o_comment(pool);

  TPCH_RETURN_NOT_OK(o_orderkey.Reserve(batch_rows));
  TPCH_RETURN_NOT_OK(o_custkey.Reserve(batch_rows));
  TPCH_RETURN_NOT_OK(o_orderstatus.Reserve(batch_rows));
  TPCH_RETURN_NOT_OK(o_totalprice.Reserve(batch_rows));
  TPCH_RETURN_NOT_OK(o_orderdate.Reserve(batch_rows));
  TPCH_RETURN_NOT_OK(o_orderpriority.Reserve(batch_rows));
  TPCH_RETURN_NOT_OK(o_clerk.Reserve(batch_rows));
  TPCH_RETURN_NOT_OK(o_shippriority.Reserve(batch_rows));
  TPCH_RETURN_NOT_OK(o_comment.Reserve(batch_rows));

  internal::OrderRow row;
  for (int64_t i = 0; i < batch_rows; ++i) {
    int64_t row_number = impl_->current_row_ + 1;
    impl_->row_generator_.GenerateRow(row_number, &row);

    TPCH_RETURN_NOT_OK(o_orderkey.Append(row.orderkey));
    TPCH_RETURN_NOT_OK(o_custkey.Append(row.custkey));
    TPCH_RETURN_NOT_OK(o_orderstatus.Append(std::string(1, row.orderstatus)));
    TPCH_RETURN_NOT_OK(o_totalprice.Append(arrow::Decimal128(row.totalprice)));
    TPCH_RETURN_NOT_OK(o_orderdate.Append(row.orderdate));
    TPCH_RETURN_NOT_OK(o_orderpriority.Append(row.orderpriority));
    TPCH_RETURN_NOT_OK(o_clerk.Append(row.clerk));
    TPCH_RETURN_NOT_OK(o_shippriority.Append(row.shippriority));
    TPCH_RETURN_NOT_OK(o_comment.Append(row.comment));

    ++impl_->current_row_;
    --impl_->remaining_rows_;
  }

  std::vector<std::shared_ptr<arrow::Array>> columns;
  columns.reserve(9);
  std::shared_ptr<arrow::Array> o_orderkey_array;
  std::shared_ptr<arrow::Array> o_custkey_array;
  std::shared_ptr<arrow::Array> o_orderstatus_array;
  std::shared_ptr<arrow::Array> o_totalprice_array;
  std::shared_ptr<arrow::Array> o_orderdate_array;
  std::shared_ptr<arrow::Array> o_orderpriority_array;
  std::shared_ptr<arrow::Array> o_clerk_array;
  std::shared_ptr<arrow::Array> o_shippriority_array;
  std::shared_ptr<arrow::Array> o_comment_array;

  TPCH_RETURN_NOT_OK(o_orderkey.Finish(&o_orderkey_array));
  TPCH_RETURN_NOT_OK(o_custkey.Finish(&o_custkey_array));
  TPCH_RETURN_NOT_OK(o_orderstatus.Finish(&o_orderstatus_array));
  TPCH_RETURN_NOT_OK(o_totalprice.Finish(&o_totalprice_array));
  TPCH_RETURN_NOT_OK(o_orderdate.Finish(&o_orderdate_array));
  TPCH_RETURN_NOT_OK(o_orderpriority.Finish(&o_orderpriority_array));
  TPCH_RETURN_NOT_OK(o_clerk.Finish(&o_clerk_array));
  TPCH_RETURN_NOT_OK(o_shippriority.Finish(&o_shippriority_array));
  TPCH_RETURN_NOT_OK(o_comment.Finish(&o_comment_array));

  columns.push_back(o_orderkey_array);
  columns.push_back(o_custkey_array);
  columns.push_back(o_orderstatus_array);
  columns.push_back(o_totalprice_array);
  columns.push_back(o_orderdate_array);
  columns.push_back(o_orderpriority_array);
  columns.push_back(o_clerk_array);
  columns.push_back(o_shippriority_array);
  columns.push_back(o_comment_array);

  return impl_->column_selection_.MakeRecordBatch(batch_rows,
                                                  std::move(columns), out);
}

int64_t OrdersGenerator::total_rows() const { return impl_->total_rows_; }

int64_t OrdersGenerator::remaining_rows() const {
  return impl_->remaining_rows_;
}

int64_t OrdersGenerator::TotalRows(double scale_factor) {
  GeneratorOptions options;
  options.scale_factor = scale_factor;
  internal::OrdersRowGenerator generator(scale_factor, options.seed_mode);
  auto status = generator.Init();
  if (!status.ok()) {
    return -1;
  }
  return generator.total_rows();
}

}  // namespace benchgen::tpch
