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

#include "generators/lineorder_generator.h"

#include <algorithm>
#include <cstring>
#include <memory>
#include <string>
#include <utility>

#include "benchgen/arrow_compat.h"
#include "benchgen/generator_options.h"
#include "benchgen/table.h"
#include "generators/lineorder_row_generator.h"
#include "util/column_selection.h"

namespace benchgen::ssb {
namespace {

#define SSB_RETURN_NOT_OK(status)       \
  do {                                  \
    ::arrow::Status _status = (status); \
    if (!_status.ok()) {                \
      return _status;                   \
    }                                   \
  } while (false)

std::shared_ptr<arrow::Schema> BuildLineorderSchema() {
  return arrow::schema({
      arrow::field("lo_orderkey", arrow::int64(), false),
      arrow::field("lo_linenumber", arrow::int32(), false),
      arrow::field("lo_custkey", arrow::int64(), false),
      arrow::field("lo_partkey", arrow::int64(), false),
      arrow::field("lo_suppkey", arrow::int64(), false),
      arrow::field("lo_orderdate", arrow::utf8(), false),
      arrow::field("lo_orderpriority", arrow::utf8(), false),
      arrow::field("lo_shippriority", arrow::int32(), false),
      arrow::field("lo_quantity", arrow::int32(), false),
      arrow::field("lo_extendedprice", arrow::int64(), false),
      arrow::field("lo_order_totalprice", arrow::int64(), false),
      arrow::field("lo_discount", arrow::int32(), false),
      arrow::field("lo_revenue", arrow::int64(), false),
      arrow::field("lo_supplycost", arrow::int64(), false),
      arrow::field("lo_tax", arrow::int32(), false),
      arrow::field("lo_commitdate", arrow::utf8(), false),
      arrow::field("lo_shipmode", arrow::utf8(), false),
  });
}

// dbgen order priority strings are at most 15 characters.
constexpr int32_t kDbgenOrderPriorityLen = 15;

}  // namespace

struct LineorderGenerator::Impl {
  explicit Impl(GeneratorOptions options)
      : options_(std::move(options)),
        schema_(BuildLineorderSchema()),
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

    if (options_.start_row < 0) {
      return arrow::Status::Invalid("start_row must be non-negative");
    }
    row_generator_.SkipRows(options_.start_row);

    if (options_.row_count < 0) {
      remaining_rows_ = -1;
    } else {
      remaining_rows_ = options_.row_count;
    }

    return arrow::Status::OK();
  }

  GeneratorOptions options_;
  int64_t remaining_rows_ = -1;
  std::shared_ptr<arrow::Schema> schema_;
  ::benchgen::internal::ColumnSelection column_selection_;
  internal::LineorderRowGenerator row_generator_;
};

LineorderGenerator::LineorderGenerator(GeneratorOptions options)
    : impl_(std::make_unique<Impl>(std::move(options))) {}

LineorderGenerator::~LineorderGenerator() = default;

arrow::Status LineorderGenerator::Init() { return impl_->Init(); }

std::string_view LineorderGenerator::name() const {
  return TableIdToString(TableId::kLineorder);
}

std::string_view LineorderGenerator::suite_name() const { return "ssb"; }

std::shared_ptr<arrow::Schema> LineorderGenerator::schema() const {
  return impl_->schema_;
}

arrow::Status LineorderGenerator::Next(
    std::shared_ptr<arrow::RecordBatch>* out) {
  if (impl_->remaining_rows_ == 0) {
    *out = nullptr;
    return arrow::Status::OK();
  }

  int64_t target_rows = impl_->options_.chunk_size;
  if (impl_->remaining_rows_ > 0) {
    target_rows = std::min(target_rows, impl_->remaining_rows_);
  }

  arrow::MemoryPool* pool = arrow::default_memory_pool();
  arrow::Int64Builder lo_orderkey(pool);
  arrow::Int32Builder lo_linenumber(pool);
  arrow::Int64Builder lo_custkey(pool);
  arrow::Int64Builder lo_partkey(pool);
  arrow::Int64Builder lo_suppkey(pool);
  arrow::StringBuilder lo_orderdate(pool);
  arrow::StringBuilder lo_orderpriority(pool);
  arrow::Int32Builder lo_shippriority(pool);
  arrow::Int32Builder lo_quantity(pool);
  arrow::Int64Builder lo_extendedprice(pool);
  arrow::Int64Builder lo_order_totalprice(pool);
  arrow::Int32Builder lo_discount(pool);
  arrow::Int64Builder lo_revenue(pool);
  arrow::Int64Builder lo_supplycost(pool);
  arrow::Int32Builder lo_tax(pool);
  arrow::StringBuilder lo_commitdate(pool);
  arrow::StringBuilder lo_shipmode(pool);

  SSB_RETURN_NOT_OK(lo_orderkey.Reserve(target_rows));
  SSB_RETURN_NOT_OK(lo_linenumber.Reserve(target_rows));
  SSB_RETURN_NOT_OK(lo_custkey.Reserve(target_rows));
  SSB_RETURN_NOT_OK(lo_partkey.Reserve(target_rows));
  SSB_RETURN_NOT_OK(lo_suppkey.Reserve(target_rows));
  SSB_RETURN_NOT_OK(lo_orderdate.Reserve(target_rows));
  SSB_RETURN_NOT_OK(lo_orderpriority.Reserve(target_rows));
  SSB_RETURN_NOT_OK(lo_shippriority.Reserve(target_rows));
  SSB_RETURN_NOT_OK(lo_quantity.Reserve(target_rows));
  SSB_RETURN_NOT_OK(lo_extendedprice.Reserve(target_rows));
  SSB_RETURN_NOT_OK(lo_order_totalprice.Reserve(target_rows));
  SSB_RETURN_NOT_OK(lo_discount.Reserve(target_rows));
  SSB_RETURN_NOT_OK(lo_revenue.Reserve(target_rows));
  SSB_RETURN_NOT_OK(lo_supplycost.Reserve(target_rows));
  SSB_RETURN_NOT_OK(lo_tax.Reserve(target_rows));
  SSB_RETURN_NOT_OK(lo_commitdate.Reserve(target_rows));
  SSB_RETURN_NOT_OK(lo_shipmode.Reserve(target_rows));

  int64_t produced = 0;
  const internal::lineorder_t* row = nullptr;
  while (produced < target_rows && impl_->row_generator_.NextRow(&row)) {
    SSB_RETURN_NOT_OK(lo_orderkey.Append(static_cast<int64_t>(row->okey)));
    SSB_RETURN_NOT_OK(
        lo_linenumber.Append(static_cast<int32_t>(row->linenumber)));
    SSB_RETURN_NOT_OK(lo_custkey.Append(row->custkey));
    SSB_RETURN_NOT_OK(lo_partkey.Append(row->partkey));
    SSB_RETURN_NOT_OK(lo_suppkey.Append(row->suppkey));
    SSB_RETURN_NOT_OK(lo_orderdate.Append(row->orderdate));
    int32_t priority_len = static_cast<int32_t>(
        std::min<size_t>(std::strlen(row->opriority), kDbgenOrderPriorityLen));
    SSB_RETURN_NOT_OK(lo_orderpriority.Append(row->opriority, priority_len));
    SSB_RETURN_NOT_OK(
        lo_shippriority.Append(static_cast<int32_t>(row->ship_priority)));
    SSB_RETURN_NOT_OK(lo_quantity.Append(static_cast<int32_t>(row->quantity)));
    SSB_RETURN_NOT_OK(
        lo_extendedprice.Append(static_cast<int64_t>(row->extended_price)));
    SSB_RETURN_NOT_OK(lo_order_totalprice.Append(
        static_cast<int64_t>(row->order_totalprice)));
    SSB_RETURN_NOT_OK(lo_discount.Append(static_cast<int32_t>(row->discount)));
    SSB_RETURN_NOT_OK(lo_revenue.Append(static_cast<int64_t>(row->revenue)));
    SSB_RETURN_NOT_OK(
        lo_supplycost.Append(static_cast<int64_t>(row->supp_cost)));
    SSB_RETURN_NOT_OK(lo_tax.Append(static_cast<int32_t>(row->tax)));
    SSB_RETURN_NOT_OK(lo_commitdate.Append(row->commit_date));
    SSB_RETURN_NOT_OK(lo_shipmode.Append(row->shipmode));
    ++produced;
  }

  if (produced == 0) {
    *out = nullptr;
    return arrow::Status::OK();
  }

  if (impl_->remaining_rows_ > 0) {
    impl_->remaining_rows_ -= produced;
  }

  std::vector<std::shared_ptr<arrow::Array>> columns;
  columns.reserve(17);
  std::shared_ptr<arrow::Array> array;
  SSB_RETURN_NOT_OK(lo_orderkey.Finish(&array));
  columns.push_back(array);
  SSB_RETURN_NOT_OK(lo_linenumber.Finish(&array));
  columns.push_back(array);
  SSB_RETURN_NOT_OK(lo_custkey.Finish(&array));
  columns.push_back(array);
  SSB_RETURN_NOT_OK(lo_partkey.Finish(&array));
  columns.push_back(array);
  SSB_RETURN_NOT_OK(lo_suppkey.Finish(&array));
  columns.push_back(array);
  SSB_RETURN_NOT_OK(lo_orderdate.Finish(&array));
  columns.push_back(array);
  SSB_RETURN_NOT_OK(lo_orderpriority.Finish(&array));
  columns.push_back(array);
  SSB_RETURN_NOT_OK(lo_shippriority.Finish(&array));
  columns.push_back(array);
  SSB_RETURN_NOT_OK(lo_quantity.Finish(&array));
  columns.push_back(array);
  SSB_RETURN_NOT_OK(lo_extendedprice.Finish(&array));
  columns.push_back(array);
  SSB_RETURN_NOT_OK(lo_order_totalprice.Finish(&array));
  columns.push_back(array);
  SSB_RETURN_NOT_OK(lo_discount.Finish(&array));
  columns.push_back(array);
  SSB_RETURN_NOT_OK(lo_revenue.Finish(&array));
  columns.push_back(array);
  SSB_RETURN_NOT_OK(lo_supplycost.Finish(&array));
  columns.push_back(array);
  SSB_RETURN_NOT_OK(lo_tax.Finish(&array));
  columns.push_back(array);
  SSB_RETURN_NOT_OK(lo_commitdate.Finish(&array));
  columns.push_back(array);
  SSB_RETURN_NOT_OK(lo_shipmode.Finish(&array));
  columns.push_back(array);

  return impl_->column_selection_.MakeRecordBatch(produced, std::move(columns),
                                                  out);
}

}  // namespace benchgen::ssb
