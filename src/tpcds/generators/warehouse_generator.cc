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

#include "generators/warehouse_generator.h"

#include <algorithm>
#include <cstdio>
#include <stdexcept>
#include <string>

#include "distribution/scaling.h"
#include "generators/warehouse_row_generator.h"
#include "util/column_selection.h"
#include "utils/columns.h"
#include "utils/null_utils.h"
#include "utils/tables.h"

namespace benchgen::tpcds {
namespace {

std::string FormatStreetName(const internal::Address& address) {
  return address.street_name1 + " " + address.street_name2;
}

std::string FormatZip(int zip) {
  char buffer[6] = {};
  std::snprintf(buffer, sizeof(buffer), "%05d", zip);
  return std::string(buffer);
}

std::shared_ptr<arrow::Schema> BuildWarehouseSchema() {
  return arrow::schema({
      arrow::field("w_warehouse_sk", arrow::int64(), false),
      arrow::field("w_warehouse_id", arrow::utf8(), false),
      arrow::field("w_warehouse_name", arrow::utf8()),
      arrow::field("w_warehouse_sq_ft", arrow::int32()),
      arrow::field("w_street_number", arrow::utf8()),
      arrow::field("w_street_name", arrow::utf8()),
      arrow::field("w_street_type", arrow::utf8()),
      arrow::field("w_suite_number", arrow::utf8()),
      arrow::field("w_city", arrow::utf8()),
      arrow::field("w_county", arrow::utf8()),
      arrow::field("w_state", arrow::utf8()),
      arrow::field("w_zip", arrow::utf8()),
      arrow::field("w_country", arrow::utf8()),
      arrow::field("w_gmt_offset", arrow::float32()),
  });
}

}  // namespace

struct WarehouseGenerator::Impl {
  explicit Impl(GeneratorOptions options)
      : options_(std::move(options)),
        schema_(BuildWarehouseSchema()),
        row_generator_(options_.scale_factor) {
    if (options_.chunk_size <= 0) {
      throw std::invalid_argument("chunk_size must be positive");
    }
    auto status = column_selection_.Init(schema_, options_.column_names);
    if (!status.ok()) {
      throw std::invalid_argument(status.ToString());
    }
    schema_ = column_selection_.schema();
    total_rows_ =
        internal::Scaling(options_.scale_factor)
            .RowCountByTableNumber(WAREHOUSE);
    if (options_.start_row < 0) {
      throw std::invalid_argument("start_row must be non-negative");
    }
    if (options_.start_row >= total_rows_) {
      remaining_rows_ = 0;
      current_row_ = options_.start_row;
      return;
    }
    current_row_ = options_.start_row;
    if (options_.row_count < 0) {
      remaining_rows_ = total_rows_ - options_.start_row;
    } else {
      remaining_rows_ =
          std::min(options_.row_count, total_rows_ - options_.start_row);
    }
    row_generator_.SkipRows(options_.start_row);
  }

  GeneratorOptions options_;
  int64_t total_rows_ = 0;
  int64_t remaining_rows_ = 0;
  int64_t current_row_ = 0;
  std::shared_ptr<arrow::Schema> schema_;
  ::benchgen::internal::ColumnSelection column_selection_;
  internal::WarehouseRowGenerator row_generator_;
};

WarehouseGenerator::WarehouseGenerator(GeneratorOptions options)
    : impl_(std::make_unique<Impl>(std::move(options))) {}

WarehouseGenerator::~WarehouseGenerator() = default;

std::shared_ptr<arrow::Schema> WarehouseGenerator::schema() const {
  return impl_->schema_;
}

std::string_view WarehouseGenerator::name() const {
  return TableIdToString(TableId::kWarehouse);
}

std::string_view WarehouseGenerator::suite_name() const { return "tpcds"; }

arrow::Status WarehouseGenerator::Next(
    std::shared_ptr<arrow::RecordBatch>* out) {
  if (impl_->remaining_rows_ == 0) {
    *out = nullptr;
    return arrow::Status::OK();
  }

  const int64_t batch_rows =
      std::min(impl_->remaining_rows_, impl_->options_.chunk_size);

  arrow::MemoryPool* pool = arrow::default_memory_pool();
  arrow::Int64Builder w_warehouse_sk(pool);
  arrow::StringBuilder w_warehouse_id(pool);
  arrow::StringBuilder w_warehouse_name(pool);
  arrow::Int32Builder w_warehouse_sq_ft(pool);
  arrow::StringBuilder w_street_number(pool);
  arrow::StringBuilder w_street_name(pool);
  arrow::StringBuilder w_street_type(pool);
  arrow::StringBuilder w_suite_number(pool);
  arrow::StringBuilder w_city(pool);
  arrow::StringBuilder w_county(pool);
  arrow::StringBuilder w_state(pool);
  arrow::StringBuilder w_zip(pool);
  arrow::StringBuilder w_country(pool);
  arrow::FloatBuilder w_gmt_offset(pool);

#define TPCDS_RETURN_NOT_OK(status)   \
  do {                                \
    arrow::Status _status = (status); \
    if (!_status.ok()) {              \
      return _status;                 \
    }                                 \
  } while (false)

  TPCDS_RETURN_NOT_OK(w_warehouse_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(w_warehouse_id.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(w_warehouse_name.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(w_warehouse_sq_ft.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(w_street_number.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(w_street_name.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(w_street_type.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(w_suite_number.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(w_city.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(w_county.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(w_state.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(w_zip.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(w_country.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(w_gmt_offset.Reserve(batch_rows));

  for (int64_t i = 0; i < batch_rows; ++i) {
    int64_t row_number = impl_->current_row_ + 1;
    internal::WarehouseRowData row =
        impl_->row_generator_.GenerateRow(row_number);

    auto is_null = [&](int column_id) {
      return internal::IsNull(row.null_bitmap, WAREHOUSE, column_id);
    };

    if (is_null(W_WAREHOUSE_SK)) {
      TPCDS_RETURN_NOT_OK(w_warehouse_sk.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(w_warehouse_sk.Append(row.warehouse_sk));
    }

    if (is_null(W_WAREHOUSE_ID)) {
      TPCDS_RETURN_NOT_OK(w_warehouse_id.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(w_warehouse_id.Append(row.warehouse_id));
    }

    if (is_null(W_WAREHOUSE_NAME)) {
      TPCDS_RETURN_NOT_OK(w_warehouse_name.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(w_warehouse_name.Append(row.warehouse_name));
    }

    if (is_null(W_WAREHOUSE_SQ_FT)) {
      TPCDS_RETURN_NOT_OK(w_warehouse_sq_ft.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(w_warehouse_sq_ft.Append(row.warehouse_sq_ft));
    }

    if (is_null(W_ADDRESS_STREET_NUM)) {
      TPCDS_RETURN_NOT_OK(w_street_number.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(
          w_street_number.Append(std::to_string(row.address.street_num)));
    }

    if (is_null(W_ADDRESS_STREET_NAME1)) {
      TPCDS_RETURN_NOT_OK(w_street_name.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(w_street_name.Append(FormatStreetName(row.address)));
    }

    if (is_null(W_ADDRESS_STREET_TYPE)) {
      TPCDS_RETURN_NOT_OK(w_street_type.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(w_street_type.Append(row.address.street_type));
    }

    if (is_null(W_ADDRESS_SUITE_NUM)) {
      TPCDS_RETURN_NOT_OK(w_suite_number.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(w_suite_number.Append(row.address.suite_num));
    }

    if (is_null(W_ADDRESS_CITY)) {
      TPCDS_RETURN_NOT_OK(w_city.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(w_city.Append(row.address.city));
    }

    if (is_null(W_ADDRESS_COUNTY)) {
      TPCDS_RETURN_NOT_OK(w_county.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(w_county.Append(row.address.county));
    }

    if (is_null(W_ADDRESS_STATE)) {
      TPCDS_RETURN_NOT_OK(w_state.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(w_state.Append(row.address.state));
    }

    if (is_null(W_ADDRESS_ZIP)) {
      TPCDS_RETURN_NOT_OK(w_zip.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(w_zip.Append(FormatZip(row.address.zip)));
    }

    if (is_null(W_ADDRESS_COUNTRY)) {
      TPCDS_RETURN_NOT_OK(w_country.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(w_country.Append(row.address.country));
    }

    if (is_null(W_ADDRESS_GMT_OFFSET)) {
      TPCDS_RETURN_NOT_OK(w_gmt_offset.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(
          w_gmt_offset.Append(static_cast<float>(row.address.gmt_offset)));
    }

    impl_->row_generator_.ConsumeRemainingSeedsForRow();
    ++impl_->current_row_;
    --impl_->remaining_rows_;
  }

  std::vector<std::shared_ptr<arrow::Array>> arrays;
  arrays.reserve(14);
  std::shared_ptr<arrow::Array> array;

  TPCDS_RETURN_NOT_OK(w_warehouse_sk.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(w_warehouse_id.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(w_warehouse_name.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(w_warehouse_sq_ft.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(w_street_number.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(w_street_name.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(w_street_type.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(w_suite_number.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(w_city.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(w_county.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(w_state.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(w_zip.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(w_country.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(w_gmt_offset.Finish(&array));
  arrays.push_back(array);

  return impl_->column_selection_.MakeRecordBatch(batch_rows, std::move(arrays),
                                                  out);
}

int64_t WarehouseGenerator::total_rows() const { return impl_->total_rows_; }

int64_t WarehouseGenerator::remaining_rows() const {
  return impl_->remaining_rows_;
}

int64_t WarehouseGenerator::TotalRows(double scale_factor) {
  return internal::Scaling(scale_factor).RowCountByTableNumber(WAREHOUSE);
}

}  // namespace benchgen::tpcds
