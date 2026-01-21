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
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include "benchgen/arrow_compat.h"
#include "benchgen/generator_options.h"
#include "benchgen/table.h"
#include "generators/customer_row_generator.h"
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

std::shared_ptr<arrow::Schema> BuildCustomerSchema() {
  return arrow::schema({
      arrow::field("c_custkey", arrow::int64(), false),
      arrow::field("c_name", arrow::utf8(), false),
      arrow::field("c_address", arrow::utf8(), false),
      arrow::field("c_city", arrow::utf8(), false),
      arrow::field("c_nation", arrow::utf8(), false),
      arrow::field("c_region", arrow::utf8(), false),
      arrow::field("c_phone", arrow::utf8(), false),
      arrow::field("c_mktsegment", arrow::utf8(), false),
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
    SSB_RETURN_NOT_OK(row_generator_.Init());

    auto status = column_selection_.Init(schema_, options_.column_names);
    if (!status.ok()) {
      return status;
    }
    schema_ = column_selection_.schema();

    total_rows_ = internal::RowCount(TableId::kCustomer, options_.scale_factor);
    if (total_rows_ < 0) {
      return arrow::Status::Invalid("failed to compute row count for customer");
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
  internal::CustomerRowGenerator row_generator_;
};

CustomerGenerator::CustomerGenerator(GeneratorOptions options)
    : impl_(std::make_unique<Impl>(std::move(options))) {}

CustomerGenerator::~CustomerGenerator() = default;

arrow::Status CustomerGenerator::Init() { return impl_->Init(); }

std::string_view CustomerGenerator::name() const {
  return TableIdToString(TableId::kCustomer);
}

std::string_view CustomerGenerator::suite_name() const { return "ssb"; }

std::shared_ptr<arrow::Schema> CustomerGenerator::schema() const {
  return impl_->schema_;
}

arrow::Status CustomerGenerator::Next(
    std::shared_ptr<arrow::RecordBatch>* out) {
  if (impl_->remaining_rows_ == 0) {
    *out = nullptr;
    return arrow::Status::OK();
  }

  const int64_t batch_rows =
      std::min(impl_->remaining_rows_, impl_->options_.chunk_size);

  arrow::MemoryPool* pool = arrow::default_memory_pool();
  arrow::Int64Builder c_custkey(pool);
  arrow::StringBuilder c_name(pool);
  arrow::StringBuilder c_address(pool);
  arrow::StringBuilder c_city(pool);
  arrow::StringBuilder c_nation(pool);
  arrow::StringBuilder c_region(pool);
  arrow::StringBuilder c_phone(pool);
  arrow::StringBuilder c_mktsegment(pool);

  SSB_RETURN_NOT_OK(c_custkey.Reserve(batch_rows));
  SSB_RETURN_NOT_OK(c_name.Reserve(batch_rows));
  SSB_RETURN_NOT_OK(c_address.Reserve(batch_rows));
  SSB_RETURN_NOT_OK(c_city.Reserve(batch_rows));
  SSB_RETURN_NOT_OK(c_nation.Reserve(batch_rows));
  SSB_RETURN_NOT_OK(c_region.Reserve(batch_rows));
  SSB_RETURN_NOT_OK(c_phone.Reserve(batch_rows));
  SSB_RETURN_NOT_OK(c_mktsegment.Reserve(batch_rows));

  for (int64_t i = 0; i < batch_rows; ++i) {
    int64_t row_number = impl_->current_row_ + 1;
    internal::customer_t row;
    impl_->row_generator_.GenerateRow(row_number, &row);

    SSB_RETURN_NOT_OK(c_custkey.Append(row.custkey));
    SSB_RETURN_NOT_OK(c_name.Append(row.name));
    SSB_RETURN_NOT_OK(
        c_address.Append(std::string_view(row.address, row.alen)));
    SSB_RETURN_NOT_OK(c_city.Append(row.city));
    SSB_RETURN_NOT_OK(c_nation.Append(row.nation_name));
    SSB_RETURN_NOT_OK(c_region.Append(row.region_name));
    SSB_RETURN_NOT_OK(c_phone.Append(row.phone));
    SSB_RETURN_NOT_OK(c_mktsegment.Append(row.mktsegment));

    ++impl_->current_row_;
  }

  impl_->remaining_rows_ -= batch_rows;

  std::vector<std::shared_ptr<arrow::Array>> columns;
  columns.reserve(8);
  std::shared_ptr<arrow::Array> array;
  SSB_RETURN_NOT_OK(c_custkey.Finish(&array));
  columns.push_back(array);
  SSB_RETURN_NOT_OK(c_name.Finish(&array));
  columns.push_back(array);
  SSB_RETURN_NOT_OK(c_address.Finish(&array));
  columns.push_back(array);
  SSB_RETURN_NOT_OK(c_city.Finish(&array));
  columns.push_back(array);
  SSB_RETURN_NOT_OK(c_nation.Finish(&array));
  columns.push_back(array);
  SSB_RETURN_NOT_OK(c_region.Finish(&array));
  columns.push_back(array);
  SSB_RETURN_NOT_OK(c_phone.Finish(&array));
  columns.push_back(array);
  SSB_RETURN_NOT_OK(c_mktsegment.Finish(&array));
  columns.push_back(array);

  return impl_->column_selection_.MakeRecordBatch(batch_rows,
                                                  std::move(columns), out);
}

}  // namespace benchgen::ssb
