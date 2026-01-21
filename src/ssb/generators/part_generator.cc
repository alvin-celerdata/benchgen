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

#include "generators/part_generator.h"

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include "benchgen/arrow_compat.h"
#include "benchgen/generator_options.h"
#include "benchgen/table.h"
#include "generators/part_row_generator.h"
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

std::shared_ptr<arrow::Schema> BuildPartSchema() {
  return arrow::schema({
      arrow::field("p_partkey", arrow::int64(), false),
      arrow::field("p_name", arrow::utf8(), false),
      arrow::field("p_mfgr", arrow::utf8(), false),
      arrow::field("p_category", arrow::utf8(), false),
      arrow::field("p_brand", arrow::utf8(), false),
      arrow::field("p_color", arrow::utf8(), false),
      arrow::field("p_type", arrow::utf8(), false),
      arrow::field("p_size", arrow::int32(), false),
      arrow::field("p_container", arrow::utf8(), false),
  });
}

}  // namespace

struct PartGenerator::Impl {
  explicit Impl(GeneratorOptions options)
      : options_(std::move(options)),
        schema_(BuildPartSchema()),
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

    total_rows_ = internal::RowCount(TableId::kPart, options_.scale_factor);
    if (total_rows_ < 0) {
      return arrow::Status::Invalid("failed to compute row count for part");
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
  internal::PartRowGenerator row_generator_;
};

PartGenerator::PartGenerator(GeneratorOptions options)
    : impl_(std::make_unique<Impl>(std::move(options))) {}

PartGenerator::~PartGenerator() = default;

arrow::Status PartGenerator::Init() { return impl_->Init(); }

std::string_view PartGenerator::name() const {
  return TableIdToString(TableId::kPart);
}

std::string_view PartGenerator::suite_name() const { return "ssb"; }

std::shared_ptr<arrow::Schema> PartGenerator::schema() const {
  return impl_->schema_;
}

arrow::Status PartGenerator::Next(std::shared_ptr<arrow::RecordBatch>* out) {
  if (impl_->remaining_rows_ == 0) {
    *out = nullptr;
    return arrow::Status::OK();
  }

  const int64_t batch_rows =
      std::min(impl_->remaining_rows_, impl_->options_.chunk_size);

  arrow::MemoryPool* pool = arrow::default_memory_pool();
  arrow::Int64Builder p_partkey(pool);
  arrow::StringBuilder p_name(pool);
  arrow::StringBuilder p_mfgr(pool);
  arrow::StringBuilder p_category(pool);
  arrow::StringBuilder p_brand(pool);
  arrow::StringBuilder p_color(pool);
  arrow::StringBuilder p_type(pool);
  arrow::Int32Builder p_size(pool);
  arrow::StringBuilder p_container(pool);

  SSB_RETURN_NOT_OK(p_partkey.Reserve(batch_rows));
  SSB_RETURN_NOT_OK(p_name.Reserve(batch_rows));
  SSB_RETURN_NOT_OK(p_mfgr.Reserve(batch_rows));
  SSB_RETURN_NOT_OK(p_category.Reserve(batch_rows));
  SSB_RETURN_NOT_OK(p_brand.Reserve(batch_rows));
  SSB_RETURN_NOT_OK(p_color.Reserve(batch_rows));
  SSB_RETURN_NOT_OK(p_type.Reserve(batch_rows));
  SSB_RETURN_NOT_OK(p_size.Reserve(batch_rows));
  SSB_RETURN_NOT_OK(p_container.Reserve(batch_rows));

  for (int64_t i = 0; i < batch_rows; ++i) {
    int64_t row_number = impl_->current_row_ + 1;
    internal::part_t row;
    impl_->row_generator_.GenerateRow(row_number, &row);

    SSB_RETURN_NOT_OK(p_partkey.Append(row.partkey));
    SSB_RETURN_NOT_OK(p_name.Append(row.name));
    SSB_RETURN_NOT_OK(p_mfgr.Append(row.mfgr));
    SSB_RETURN_NOT_OK(p_category.Append(row.category));
    SSB_RETURN_NOT_OK(p_brand.Append(row.brand));
    SSB_RETURN_NOT_OK(p_color.Append(std::string_view(row.color, row.clen)));
    SSB_RETURN_NOT_OK(p_type.Append(std::string_view(row.type, row.tlen)));
    SSB_RETURN_NOT_OK(p_size.Append(static_cast<int32_t>(row.size)));
    SSB_RETURN_NOT_OK(p_container.Append(row.container));

    ++impl_->current_row_;
  }

  impl_->remaining_rows_ -= batch_rows;

  std::vector<std::shared_ptr<arrow::Array>> columns;
  columns.reserve(9);
  std::shared_ptr<arrow::Array> array;
  SSB_RETURN_NOT_OK(p_partkey.Finish(&array));
  columns.push_back(array);
  SSB_RETURN_NOT_OK(p_name.Finish(&array));
  columns.push_back(array);
  SSB_RETURN_NOT_OK(p_mfgr.Finish(&array));
  columns.push_back(array);
  SSB_RETURN_NOT_OK(p_category.Finish(&array));
  columns.push_back(array);
  SSB_RETURN_NOT_OK(p_brand.Finish(&array));
  columns.push_back(array);
  SSB_RETURN_NOT_OK(p_color.Finish(&array));
  columns.push_back(array);
  SSB_RETURN_NOT_OK(p_type.Finish(&array));
  columns.push_back(array);
  SSB_RETURN_NOT_OK(p_size.Finish(&array));
  columns.push_back(array);
  SSB_RETURN_NOT_OK(p_container.Finish(&array));
  columns.push_back(array);

  return impl_->column_selection_.MakeRecordBatch(batch_rows,
                                                  std::move(columns), out);
}

}  // namespace benchgen::ssb
