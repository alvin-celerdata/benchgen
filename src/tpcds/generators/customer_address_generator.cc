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

#include "generators/customer_address_generator.h"

#include <algorithm>

#include "distribution/scaling.h"
#include "generators/customer_address_row_generator.h"
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

std::shared_ptr<arrow::Schema> BuildCustomerAddressSchema() {
  return arrow::schema({
      arrow::field("ca_address_sk", arrow::int64(), false),
      arrow::field("ca_address_id", arrow::utf8(), false),
      arrow::field("ca_street_number", arrow::int32()),
      arrow::field("ca_street_name", arrow::utf8()),
      arrow::field("ca_street_type", arrow::utf8()),
      arrow::field("ca_suite_number", arrow::utf8()),
      arrow::field("ca_city", arrow::utf8()),
      arrow::field("ca_county", arrow::utf8()),
      arrow::field("ca_state", arrow::utf8()),
      arrow::field("ca_zip", arrow::utf8()),
      arrow::field("ca_country", arrow::utf8()),
      arrow::field("ca_gmt_offset", arrow::int32()),
      arrow::field("ca_location_type", arrow::utf8()),
  });
}

}  // namespace

struct CustomerAddressGenerator::Impl {
  explicit Impl(GeneratorOptions options)
      : options_(std::move(options)),
        schema_(BuildCustomerAddressSchema()),
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
            .RowCountByTableNumber(CUSTOMER_ADDRESS);
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
  internal::CustomerAddressRowGenerator row_generator_;
};

CustomerAddressGenerator::CustomerAddressGenerator(GeneratorOptions options)
    : impl_(std::make_unique<Impl>(std::move(options))) {}

arrow::Status CustomerAddressGenerator::Init() { return impl_->Init(); }

CustomerAddressGenerator::~CustomerAddressGenerator() = default;

std::shared_ptr<arrow::Schema> CustomerAddressGenerator::schema() const {
  return impl_->schema_;
}

std::string_view CustomerAddressGenerator::name() const {
  return TableIdToString(TableId::kCustomerAddress);
}

std::string_view CustomerAddressGenerator::suite_name() const {
  return "tpcds";
}

arrow::Status CustomerAddressGenerator::Next(
    std::shared_ptr<arrow::RecordBatch>* out) {
  if (impl_->remaining_rows_ == 0) {
    *out = nullptr;
    return arrow::Status::OK();
  }

  const int64_t batch_rows =
      std::min(impl_->remaining_rows_, impl_->options_.chunk_size);

  arrow::MemoryPool* pool = arrow::default_memory_pool();
  arrow::Int64Builder ca_address_sk(pool);
  arrow::StringBuilder ca_address_id(pool);
  arrow::Int32Builder ca_street_number(pool);
  arrow::StringBuilder ca_street_name(pool);
  arrow::StringBuilder ca_street_type(pool);
  arrow::StringBuilder ca_suite_number(pool);
  arrow::StringBuilder ca_city(pool);
  arrow::StringBuilder ca_county(pool);
  arrow::StringBuilder ca_state(pool);
  arrow::StringBuilder ca_zip(pool);
  arrow::StringBuilder ca_country(pool);
  arrow::Int32Builder ca_gmt_offset(pool);
  arrow::StringBuilder ca_location_type(pool);

  TPCDS_RETURN_NOT_OK(ca_address_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(ca_address_id.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(ca_street_number.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(ca_street_name.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(ca_street_type.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(ca_suite_number.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(ca_city.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(ca_county.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(ca_state.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(ca_zip.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(ca_country.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(ca_gmt_offset.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(ca_location_type.Reserve(batch_rows));

  for (int64_t i = 0; i < batch_rows; ++i) {
    int64_t row_number = impl_->current_row_ + 1;
    internal::CustomerAddressRowData row =
        impl_->row_generator_.GenerateRow(row_number);

    auto is_null = [&](int column_id) {
      return internal::IsNull(row.null_bitmap, CUSTOMER_ADDRESS, column_id);
    };

    if (is_null(CA_ADDRESS_SK)) {
      TPCDS_RETURN_NOT_OK(ca_address_sk.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(ca_address_sk.Append(row.address_sk));
    }

    if (is_null(CA_ADDRESS_ID)) {
      TPCDS_RETURN_NOT_OK(ca_address_id.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(ca_address_id.Append(row.address_id));
    }

    if (is_null(CA_ADDRESS_STREET_NUM)) {
      TPCDS_RETURN_NOT_OK(ca_street_number.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(ca_street_number.Append(row.street_num));
    }

    if (is_null(CA_ADDRESS_STREET_NAME1)) {
      TPCDS_RETURN_NOT_OK(ca_street_name.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(ca_street_name.Append(row.street_name));
    }

    if (is_null(CA_ADDRESS_STREET_TYPE)) {
      TPCDS_RETURN_NOT_OK(ca_street_type.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(ca_street_type.Append(row.street_type));
    }

    if (is_null(CA_ADDRESS_SUITE_NUM)) {
      TPCDS_RETURN_NOT_OK(ca_suite_number.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(ca_suite_number.Append(row.suite_num));
    }

    if (is_null(CA_ADDRESS_CITY)) {
      TPCDS_RETURN_NOT_OK(ca_city.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(ca_city.Append(row.city));
    }

    if (is_null(CA_ADDRESS_COUNTY)) {
      TPCDS_RETURN_NOT_OK(ca_county.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(ca_county.Append(row.county));
    }

    if (is_null(CA_ADDRESS_STATE)) {
      TPCDS_RETURN_NOT_OK(ca_state.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(ca_state.Append(row.state));
    }

    if (is_null(CA_ADDRESS_ZIP)) {
      TPCDS_RETURN_NOT_OK(ca_zip.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(ca_zip.Append(row.zip));
    }

    if (is_null(CA_ADDRESS_COUNTRY)) {
      TPCDS_RETURN_NOT_OK(ca_country.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(ca_country.Append(row.country));
    }

    if (is_null(CA_ADDRESS_GMT_OFFSET)) {
      TPCDS_RETURN_NOT_OK(ca_gmt_offset.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(ca_gmt_offset.Append(row.gmt_offset));
    }

    if (is_null(CA_LOCATION_TYPE)) {
      TPCDS_RETURN_NOT_OK(ca_location_type.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(ca_location_type.Append(row.location_type));
    }

    impl_->row_generator_.ConsumeRemainingSeedsForRow();
    ++impl_->current_row_;
    --impl_->remaining_rows_;
  }

  std::vector<std::shared_ptr<arrow::Array>> arrays;
  arrays.reserve(13);
  std::shared_ptr<arrow::Array> array;

  TPCDS_RETURN_NOT_OK(ca_address_sk.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(ca_address_id.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(ca_street_number.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(ca_street_name.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(ca_street_type.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(ca_suite_number.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(ca_city.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(ca_county.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(ca_state.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(ca_zip.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(ca_country.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(ca_gmt_offset.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(ca_location_type.Finish(&array));
  arrays.push_back(array);

  return impl_->column_selection_.MakeRecordBatch(batch_rows, std::move(arrays),
                                                  out);
}

int64_t CustomerAddressGenerator::total_rows() const {
  return impl_->total_rows_;
}

int64_t CustomerAddressGenerator::remaining_rows() const {
  return impl_->remaining_rows_;
}

int64_t CustomerAddressGenerator::TotalRows(double scale_factor) {
  return internal::Scaling(scale_factor)
      .RowCountByTableNumber(CUSTOMER_ADDRESS);
}

}  // namespace benchgen::tpcds
