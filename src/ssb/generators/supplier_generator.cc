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

#include "generators/supplier_generator.h"

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include "benchgen/arrow_compat.h"
#include "benchgen/generator_options.h"
#include "benchgen/table.h"
#include "generators/supplier_row_generator.h"
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

std::shared_ptr<arrow::Schema> BuildSupplierSchema() {
  return arrow::schema({
      arrow::field("s_suppkey", arrow::int64(), false),
      arrow::field("s_name", arrow::utf8(), false),
      arrow::field("s_address", arrow::utf8(), false),
      arrow::field("s_city", arrow::utf8(), false),
      arrow::field("s_nation", arrow::utf8(), false),
      arrow::field("s_region", arrow::utf8(), false),
      arrow::field("s_phone", arrow::utf8(), false),
  });
}

}  // namespace

struct SupplierGenerator::Impl {
  explicit Impl(GeneratorOptions options)
      : options_(std::move(options)),
        schema_(BuildSupplierSchema()),
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

    total_rows_ = internal::RowCount(TableId::kSupplier, options_.scale_factor);
    if (total_rows_ < 0) {
      return arrow::Status::Invalid("failed to compute row count for supplier");
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
  internal::SupplierRowGenerator row_generator_;
};

SupplierGenerator::SupplierGenerator(GeneratorOptions options)
    : impl_(std::make_unique<Impl>(std::move(options))) {}

SupplierGenerator::~SupplierGenerator() = default;

arrow::Status SupplierGenerator::Init() { return impl_->Init(); }

std::string_view SupplierGenerator::name() const {
  return TableIdToString(TableId::kSupplier);
}

std::string_view SupplierGenerator::suite_name() const { return "ssb"; }

std::shared_ptr<arrow::Schema> SupplierGenerator::schema() const {
  return impl_->schema_;
}

arrow::Status SupplierGenerator::Next(
    std::shared_ptr<arrow::RecordBatch>* out) {
  if (impl_->remaining_rows_ == 0) {
    *out = nullptr;
    return arrow::Status::OK();
  }

  const int64_t batch_rows =
      std::min(impl_->remaining_rows_, impl_->options_.chunk_size);

  arrow::MemoryPool* pool = arrow::default_memory_pool();
  arrow::Int64Builder s_suppkey(pool);
  arrow::StringBuilder s_name(pool);
  arrow::StringBuilder s_address(pool);
  arrow::StringBuilder s_city(pool);
  arrow::StringBuilder s_nation(pool);
  arrow::StringBuilder s_region(pool);
  arrow::StringBuilder s_phone(pool);

  SSB_RETURN_NOT_OK(s_suppkey.Reserve(batch_rows));
  SSB_RETURN_NOT_OK(s_name.Reserve(batch_rows));
  SSB_RETURN_NOT_OK(s_address.Reserve(batch_rows));
  SSB_RETURN_NOT_OK(s_city.Reserve(batch_rows));
  SSB_RETURN_NOT_OK(s_nation.Reserve(batch_rows));
  SSB_RETURN_NOT_OK(s_region.Reserve(batch_rows));
  SSB_RETURN_NOT_OK(s_phone.Reserve(batch_rows));

  for (int64_t i = 0; i < batch_rows; ++i) {
    int64_t row_number = impl_->current_row_ + 1;
    internal::supplier_t row;
    impl_->row_generator_.GenerateRow(row_number, &row);

    SSB_RETURN_NOT_OK(s_suppkey.Append(row.suppkey));
    SSB_RETURN_NOT_OK(s_name.Append(row.name));
    SSB_RETURN_NOT_OK(
        s_address.Append(std::string_view(row.address, row.alen)));
    SSB_RETURN_NOT_OK(s_city.Append(row.city));
    SSB_RETURN_NOT_OK(s_nation.Append(row.nation_name));
    SSB_RETURN_NOT_OK(s_region.Append(row.region_name));
    SSB_RETURN_NOT_OK(s_phone.Append(row.phone));

    ++impl_->current_row_;
  }

  impl_->remaining_rows_ -= batch_rows;

  std::vector<std::shared_ptr<arrow::Array>> columns;
  columns.reserve(7);
  std::shared_ptr<arrow::Array> array;
  SSB_RETURN_NOT_OK(s_suppkey.Finish(&array));
  columns.push_back(array);
  SSB_RETURN_NOT_OK(s_name.Finish(&array));
  columns.push_back(array);
  SSB_RETURN_NOT_OK(s_address.Finish(&array));
  columns.push_back(array);
  SSB_RETURN_NOT_OK(s_city.Finish(&array));
  columns.push_back(array);
  SSB_RETURN_NOT_OK(s_nation.Finish(&array));
  columns.push_back(array);
  SSB_RETURN_NOT_OK(s_region.Finish(&array));
  columns.push_back(array);
  SSB_RETURN_NOT_OK(s_phone.Finish(&array));
  columns.push_back(array);

  return impl_->column_selection_.MakeRecordBatch(batch_rows,
                                                  std::move(columns), out);
}

}  // namespace benchgen::ssb
