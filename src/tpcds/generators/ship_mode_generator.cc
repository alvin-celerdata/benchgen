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

#include "generators/ship_mode_generator.h"

#include <algorithm>

#include "benchgen/table.h"
#include "distribution/scaling.h"
#include "generators/ship_mode_row_generator.h"
#include "util/column_selection.h"
#include "utils/columns.h"
#include "utils/null_utils.h"
#include "utils/tables.h"

namespace benchgen::tpcds {
namespace {

#define TPCDS_RETURN_NOT_OK(status)     \
  do {                                  \
    ::arrow::Status _status = (status); \
    if (!_status.ok()) {                \
      return _status;                   \
    }                                   \
  } while (false)

std::shared_ptr<arrow::Schema> BuildShipModeSchema() {
  return arrow::schema({
      arrow::field("sm_ship_mode_sk", arrow::int64(), false),
      arrow::field("sm_ship_mode_id", arrow::utf8(), false),
      arrow::field("sm_type", arrow::utf8()),
      arrow::field("sm_code", arrow::utf8()),
      arrow::field("sm_carrier", arrow::utf8()),
      arrow::field("sm_contract", arrow::utf8()),
  });
}

}  // namespace

struct ShipModeGenerator::Impl {
  explicit Impl(GeneratorOptions options)
      : options_(std::move(options)),
        schema_(BuildShipModeSchema()),
        row_generator_(options_.scale_factor) {}

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
            .RowCount(TableId::kShipMode);
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
  internal::ShipModeRowGenerator row_generator_;
};

ShipModeGenerator::ShipModeGenerator(GeneratorOptions options)
    : impl_(std::make_unique<Impl>(std::move(options))) {}

arrow::Status ShipModeGenerator::Init() { return impl_->Init(); }

ShipModeGenerator::~ShipModeGenerator() = default;

std::shared_ptr<arrow::Schema> ShipModeGenerator::schema() const {
  return impl_->schema_;
}

std::string_view ShipModeGenerator::name() const {
  return TableIdToString(TableId::kShipMode);
}

std::string_view ShipModeGenerator::suite_name() const { return "tpcds"; }

arrow::Status ShipModeGenerator::Next(
    std::shared_ptr<arrow::RecordBatch>* out) {
  if (impl_->remaining_rows_ == 0) {
    *out = nullptr;
    return arrow::Status::OK();
  }

  const int64_t batch_rows =
      std::min(impl_->remaining_rows_, impl_->options_.chunk_size);

  arrow::MemoryPool* pool = arrow::default_memory_pool();
  arrow::Int64Builder sm_ship_mode_sk(pool);
  arrow::StringBuilder sm_ship_mode_id(pool);
  arrow::StringBuilder sm_type(pool);
  arrow::StringBuilder sm_code(pool);
  arrow::StringBuilder sm_carrier(pool);
  arrow::StringBuilder sm_contract(pool);

  TPCDS_RETURN_NOT_OK(sm_ship_mode_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(sm_ship_mode_id.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(sm_type.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(sm_code.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(sm_carrier.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(sm_contract.Reserve(batch_rows));

  for (int64_t i = 0; i < batch_rows; ++i) {
    int64_t row_number = impl_->current_row_ + 1;
    internal::ShipModeRowData row =
        impl_->row_generator_.GenerateRow(row_number);

    auto is_null = [&](int column_id) {
      return internal::IsNull(row.null_bitmap, SHIP_MODE, column_id);
    };

    if (is_null(SM_SHIP_MODE_SK)) {
      TPCDS_RETURN_NOT_OK(sm_ship_mode_sk.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(sm_ship_mode_sk.Append(row.ship_mode_sk));
    }

    if (is_null(SM_SHIP_MODE_ID)) {
      TPCDS_RETURN_NOT_OK(sm_ship_mode_id.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(sm_ship_mode_id.Append(row.ship_mode_id));
    }

    if (is_null(SM_TYPE)) {
      TPCDS_RETURN_NOT_OK(sm_type.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(sm_type.Append(row.type));
    }

    if (is_null(SM_CODE)) {
      TPCDS_RETURN_NOT_OK(sm_code.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(sm_code.Append(row.code));
    }

    if (is_null(SM_CARRIER)) {
      TPCDS_RETURN_NOT_OK(sm_carrier.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(sm_carrier.Append(row.carrier));
    }

    if (is_null(SM_CONTRACT)) {
      TPCDS_RETURN_NOT_OK(sm_contract.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(sm_contract.Append(row.contract));
    }

    impl_->row_generator_.ConsumeRemainingSeedsForRow();
    ++impl_->current_row_;
    --impl_->remaining_rows_;
  }

  std::vector<std::shared_ptr<arrow::Array>> arrays;
  arrays.reserve(6);
  std::shared_ptr<arrow::Array> array;

  TPCDS_RETURN_NOT_OK(sm_ship_mode_sk.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(sm_ship_mode_id.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(sm_type.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(sm_code.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(sm_carrier.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(sm_contract.Finish(&array));
  arrays.push_back(array);

  return impl_->column_selection_.MakeRecordBatch(batch_rows, std::move(arrays),
                                                  out);
}

int64_t ShipModeGenerator::total_rows() const { return impl_->total_rows_; }

int64_t ShipModeGenerator::remaining_rows() const {
  return impl_->remaining_rows_;
}

int64_t ShipModeGenerator::TotalRows(double scale_factor) {
  return internal::Scaling(scale_factor).RowCount(TableId::kShipMode);
}

}  // namespace benchgen::tpcds
