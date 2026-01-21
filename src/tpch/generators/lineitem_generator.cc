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

#include "generators/lineitem_generator.h"

#include <algorithm>
#include <string>

#include "benchgen/arrow_compat.h"
#include "benchgen/table.h"
#include "generators/lineitem_row_generator.h"
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

std::shared_ptr<arrow::Schema> BuildLineItemSchema() {
  auto money_type = arrow::decimal128(15, 2);
  auto pct_type = arrow::decimal128(4, 2);
  return arrow::schema({
      arrow::field("l_orderkey", arrow::int64(), false),
      arrow::field("l_partkey", arrow::int64(), false),
      arrow::field("l_suppkey", arrow::int64(), false),
      arrow::field("l_linenumber", arrow::int32(), false),
      arrow::field("l_quantity", arrow::int64(), false),
      arrow::field("l_extendedprice", money_type, false),
      arrow::field("l_discount", pct_type, false),
      arrow::field("l_tax", pct_type, false),
      arrow::field("l_returnflag", arrow::utf8(), false),
      arrow::field("l_linestatus", arrow::utf8(), false),
      arrow::field("l_shipdate", arrow::utf8(), false),
      arrow::field("l_commitdate", arrow::utf8(), false),
      arrow::field("l_receiptdate", arrow::utf8(), false),
      arrow::field("l_shipinstruct", arrow::utf8(), false),
      arrow::field("l_shipmode", arrow::utf8(), false),
      arrow::field("l_comment", arrow::utf8(), false),
  });
}

}  // namespace

struct LineItemGenerator::Impl {
  explicit Impl(GeneratorOptions options)
      : options_(std::move(options)),
        schema_(BuildLineItemSchema()),
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

    total_rows_ = -1;
    if (options_.start_row < 0) {
      return arrow::Status::Invalid("start_row must be non-negative");
    }

    if (options_.row_count < 0) {
      remaining_rows_ = -1;
    } else {
      remaining_rows_ = options_.row_count;
    }

    row_generator_.SkipRows(options_.start_row);

    return arrow::Status::OK();
  }

  GeneratorOptions options_;
  int64_t total_rows_ = -1;
  int64_t remaining_rows_ = -1;
  std::shared_ptr<arrow::Schema> schema_;
  ::benchgen::internal::ColumnSelection column_selection_;
  internal::LineItemRowGenerator row_generator_;
};

LineItemGenerator::LineItemGenerator(GeneratorOptions options)
    : impl_(std::make_unique<Impl>(std::move(options))) {}

LineItemGenerator::~LineItemGenerator() = default;

arrow::Status LineItemGenerator::Init() { return impl_->Init(); }

std::shared_ptr<arrow::Schema> LineItemGenerator::schema() const {
  return impl_->schema_;
}

std::string_view LineItemGenerator::name() const {
  return TableIdToString(TableId::kLineItem);
}

std::string_view LineItemGenerator::suite_name() const { return "tpch"; }

arrow::Status LineItemGenerator::Next(
    std::shared_ptr<arrow::RecordBatch>* out) {
  if (impl_->remaining_rows_ == 0) {
    *out = nullptr;
    return arrow::Status::OK();
  }
  int64_t batch_rows = impl_->options_.chunk_size;
  if (impl_->remaining_rows_ > 0) {
    batch_rows = std::min(batch_rows, impl_->remaining_rows_);
  }

  arrow::MemoryPool* pool = arrow::default_memory_pool();
  auto money_type = arrow::decimal128(15, 2);
  auto pct_type = arrow::decimal128(4, 2);
  arrow::Int64Builder l_orderkey(pool);
  arrow::Int64Builder l_partkey(pool);
  arrow::Int64Builder l_suppkey(pool);
  arrow::Int32Builder l_linenumber(pool);
  arrow::Int64Builder l_quantity(pool);
  arrow::Decimal128Builder l_extendedprice(money_type, pool);
  arrow::Decimal128Builder l_discount(pct_type, pool);
  arrow::Decimal128Builder l_tax(pct_type, pool);
  arrow::StringBuilder l_returnflag(pool);
  arrow::StringBuilder l_linestatus(pool);
  arrow::StringBuilder l_shipdate(pool);
  arrow::StringBuilder l_commitdate(pool);
  arrow::StringBuilder l_receiptdate(pool);
  arrow::StringBuilder l_shipinstruct(pool);
  arrow::StringBuilder l_shipmode(pool);
  arrow::StringBuilder l_comment(pool);

  TPCH_RETURN_NOT_OK(l_orderkey.Reserve(batch_rows));
  TPCH_RETURN_NOT_OK(l_partkey.Reserve(batch_rows));
  TPCH_RETURN_NOT_OK(l_suppkey.Reserve(batch_rows));
  TPCH_RETURN_NOT_OK(l_linenumber.Reserve(batch_rows));
  TPCH_RETURN_NOT_OK(l_quantity.Reserve(batch_rows));
  TPCH_RETURN_NOT_OK(l_extendedprice.Reserve(batch_rows));
  TPCH_RETURN_NOT_OK(l_discount.Reserve(batch_rows));
  TPCH_RETURN_NOT_OK(l_tax.Reserve(batch_rows));
  TPCH_RETURN_NOT_OK(l_returnflag.Reserve(batch_rows));
  TPCH_RETURN_NOT_OK(l_linestatus.Reserve(batch_rows));
  TPCH_RETURN_NOT_OK(l_shipdate.Reserve(batch_rows));
  TPCH_RETURN_NOT_OK(l_commitdate.Reserve(batch_rows));
  TPCH_RETURN_NOT_OK(l_receiptdate.Reserve(batch_rows));
  TPCH_RETURN_NOT_OK(l_shipinstruct.Reserve(batch_rows));
  TPCH_RETURN_NOT_OK(l_shipmode.Reserve(batch_rows));
  TPCH_RETURN_NOT_OK(l_comment.Reserve(batch_rows));

  internal::LineItemRow row;
  int64_t produced = 0;
  while (produced < batch_rows) {
    if (impl_->remaining_rows_ == 0) {
      break;
    }
    if (!impl_->row_generator_.NextRow(&row)) {
      impl_->remaining_rows_ = 0;
      break;
    }
    TPCH_RETURN_NOT_OK(l_orderkey.Append(row.orderkey));
    TPCH_RETURN_NOT_OK(l_partkey.Append(row.partkey));
    TPCH_RETURN_NOT_OK(l_suppkey.Append(row.suppkey));
    TPCH_RETURN_NOT_OK(l_linenumber.Append(row.linenumber));
    TPCH_RETURN_NOT_OK(l_quantity.Append(row.quantity));
    TPCH_RETURN_NOT_OK(
        l_extendedprice.Append(arrow::Decimal128(row.extendedprice)));
    TPCH_RETURN_NOT_OK(l_discount.Append(arrow::Decimal128(row.discount)));
    TPCH_RETURN_NOT_OK(l_tax.Append(arrow::Decimal128(row.tax)));
    TPCH_RETURN_NOT_OK(l_returnflag.Append(std::string(1, row.returnflag)));
    TPCH_RETURN_NOT_OK(l_linestatus.Append(std::string(1, row.linestatus)));
    TPCH_RETURN_NOT_OK(l_shipdate.Append(row.shipdate));
    TPCH_RETURN_NOT_OK(l_commitdate.Append(row.commitdate));
    TPCH_RETURN_NOT_OK(l_receiptdate.Append(row.receiptdate));
    TPCH_RETURN_NOT_OK(l_shipinstruct.Append(row.shipinstruct));
    TPCH_RETURN_NOT_OK(l_shipmode.Append(row.shipmode));
    TPCH_RETURN_NOT_OK(l_comment.Append(row.comment));

    ++produced;
    if (impl_->remaining_rows_ > 0) {
      --impl_->remaining_rows_;
    }
  }

  if (produced == 0) {
    *out = nullptr;
    return arrow::Status::OK();
  }

  std::vector<std::shared_ptr<arrow::Array>> columns;
  columns.reserve(16);
  std::shared_ptr<arrow::Array> l_orderkey_array;
  std::shared_ptr<arrow::Array> l_partkey_array;
  std::shared_ptr<arrow::Array> l_suppkey_array;
  std::shared_ptr<arrow::Array> l_linenumber_array;
  std::shared_ptr<arrow::Array> l_quantity_array;
  std::shared_ptr<arrow::Array> l_extendedprice_array;
  std::shared_ptr<arrow::Array> l_discount_array;
  std::shared_ptr<arrow::Array> l_tax_array;
  std::shared_ptr<arrow::Array> l_returnflag_array;
  std::shared_ptr<arrow::Array> l_linestatus_array;
  std::shared_ptr<arrow::Array> l_shipdate_array;
  std::shared_ptr<arrow::Array> l_commitdate_array;
  std::shared_ptr<arrow::Array> l_receiptdate_array;
  std::shared_ptr<arrow::Array> l_shipinstruct_array;
  std::shared_ptr<arrow::Array> l_shipmode_array;
  std::shared_ptr<arrow::Array> l_comment_array;

  TPCH_RETURN_NOT_OK(l_orderkey.Finish(&l_orderkey_array));
  TPCH_RETURN_NOT_OK(l_partkey.Finish(&l_partkey_array));
  TPCH_RETURN_NOT_OK(l_suppkey.Finish(&l_suppkey_array));
  TPCH_RETURN_NOT_OK(l_linenumber.Finish(&l_linenumber_array));
  TPCH_RETURN_NOT_OK(l_quantity.Finish(&l_quantity_array));
  TPCH_RETURN_NOT_OK(l_extendedprice.Finish(&l_extendedprice_array));
  TPCH_RETURN_NOT_OK(l_discount.Finish(&l_discount_array));
  TPCH_RETURN_NOT_OK(l_tax.Finish(&l_tax_array));
  TPCH_RETURN_NOT_OK(l_returnflag.Finish(&l_returnflag_array));
  TPCH_RETURN_NOT_OK(l_linestatus.Finish(&l_linestatus_array));
  TPCH_RETURN_NOT_OK(l_shipdate.Finish(&l_shipdate_array));
  TPCH_RETURN_NOT_OK(l_commitdate.Finish(&l_commitdate_array));
  TPCH_RETURN_NOT_OK(l_receiptdate.Finish(&l_receiptdate_array));
  TPCH_RETURN_NOT_OK(l_shipinstruct.Finish(&l_shipinstruct_array));
  TPCH_RETURN_NOT_OK(l_shipmode.Finish(&l_shipmode_array));
  TPCH_RETURN_NOT_OK(l_comment.Finish(&l_comment_array));

  columns.push_back(l_orderkey_array);
  columns.push_back(l_partkey_array);
  columns.push_back(l_suppkey_array);
  columns.push_back(l_linenumber_array);
  columns.push_back(l_quantity_array);
  columns.push_back(l_extendedprice_array);
  columns.push_back(l_discount_array);
  columns.push_back(l_tax_array);
  columns.push_back(l_returnflag_array);
  columns.push_back(l_linestatus_array);
  columns.push_back(l_shipdate_array);
  columns.push_back(l_commitdate_array);
  columns.push_back(l_receiptdate_array);
  columns.push_back(l_shipinstruct_array);
  columns.push_back(l_shipmode_array);
  columns.push_back(l_comment_array);

  return impl_->column_selection_.MakeRecordBatch(produced, std::move(columns),
                                                  out);
}

int64_t LineItemGenerator::total_rows() const { return impl_->total_rows_; }

int64_t LineItemGenerator::remaining_rows() const {
  return impl_->remaining_rows_;
}

int64_t LineItemGenerator::TotalRows(double scale_factor) {
  (void)scale_factor;
  return -1;
}

}  // namespace benchgen::tpch
