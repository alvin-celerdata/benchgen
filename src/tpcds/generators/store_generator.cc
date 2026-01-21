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

#include "generators/store_generator.h"

#include <algorithm>
#include <cstdio>
#include <stdexcept>
#include <string>

#include "distribution/scaling.h"
#include "generators/store_row_generator.h"
#include "util/column_selection.h"
#include "utils/columns.h"
#include "utils/date.h"
#include "utils/null_utils.h"
#include "utils/tables.h"

namespace benchgen::tpcds {
namespace {

int32_t Date32FromJulian(int32_t julian) {
  return internal::Date::DaysSinceEpoch(internal::Date::FromJulianDays(julian));
}

std::string FormatStreetName(const internal::Address& address) {
  return address.street_name1 + " " + address.street_name2;
}

std::string FormatZip(int zip) {
  char buffer[6] = {};
  std::snprintf(buffer, sizeof(buffer), "%05d", zip);
  return std::string(buffer);
}

std::shared_ptr<arrow::Schema> BuildStoreSchema() {
  return arrow::schema({
      arrow::field("s_store_sk", arrow::int64(), false),
      arrow::field("s_store_id", arrow::utf8(), false),
      arrow::field("s_rec_start_date", arrow::date32()),
      arrow::field("s_rec_end_date", arrow::date32()),
      arrow::field("s_closed_date_sk", arrow::int32()),
      arrow::field("s_store_name", arrow::utf8()),
      arrow::field("s_number_employees", arrow::int32()),
      arrow::field("s_floor_space", arrow::int32()),
      arrow::field("s_hours", arrow::utf8()),
      arrow::field("s_manager", arrow::utf8()),
      arrow::field("s_market_id", arrow::int32()),
      arrow::field("s_geography_class", arrow::utf8()),
      arrow::field("s_market_desc", arrow::utf8()),
      arrow::field("s_market_manager", arrow::utf8()),
      arrow::field("s_division_id", arrow::int32()),
      arrow::field("s_division_name", arrow::utf8()),
      arrow::field("s_company_id", arrow::int32()),
      arrow::field("s_company_name", arrow::utf8()),
      arrow::field("s_street_number", arrow::utf8()),
      arrow::field("s_street_name", arrow::utf8()),
      arrow::field("s_street_type", arrow::utf8()),
      arrow::field("s_suite_number", arrow::utf8()),
      arrow::field("s_city", arrow::utf8()),
      arrow::field("s_county", arrow::utf8()),
      arrow::field("s_state", arrow::utf8()),
      arrow::field("s_zip", arrow::utf8()),
      arrow::field("s_country", arrow::utf8()),
      arrow::field("s_gmt_offset", arrow::float32()),
      arrow::field("s_tax_precentage", arrow::smallest_decimal(5, 2)),
  });
}

}  // namespace

struct StoreGenerator::Impl {
  explicit Impl(GeneratorOptions options)
      : options_(std::move(options)),
        schema_(BuildStoreSchema()),
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
            .RowCountByTableNumber(STORE);
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
  internal::StoreRowGenerator row_generator_;
};

StoreGenerator::StoreGenerator(GeneratorOptions options)
    : impl_(std::make_unique<Impl>(std::move(options))) {}

StoreGenerator::~StoreGenerator() = default;

std::shared_ptr<arrow::Schema> StoreGenerator::schema() const {
  return impl_->schema_;
}

std::string_view StoreGenerator::name() const {
  return TableIdToString(TableId::kStore);
}

std::string_view StoreGenerator::suite_name() const { return "tpcds"; }

arrow::Status StoreGenerator::Next(std::shared_ptr<arrow::RecordBatch>* out) {
  if (impl_->remaining_rows_ == 0) {
    *out = nullptr;
    return arrow::Status::OK();
  }

  const int64_t batch_rows =
      std::min(impl_->remaining_rows_, impl_->options_.chunk_size);

  arrow::MemoryPool* pool = arrow::default_memory_pool();
  arrow::Int64Builder s_store_sk(pool);
  arrow::StringBuilder s_store_id(pool);
  arrow::Date32Builder s_rec_start_date_id(pool);
  arrow::Date32Builder s_rec_end_date_id(pool);
  arrow::Int32Builder s_closed_date_id(pool);
  arrow::StringBuilder s_store_name(pool);
  arrow::Int32Builder s_employees(pool);
  arrow::Int32Builder s_floor_space(pool);
  arrow::StringBuilder s_hours(pool);
  arrow::StringBuilder s_store_manager(pool);
  arrow::Int32Builder s_market_id(pool);
  arrow::StringBuilder s_geography_class(pool);
  arrow::StringBuilder s_market_desc(pool);
  arrow::StringBuilder s_market_manager(pool);
  arrow::Int32Builder s_division_id(pool);
  arrow::StringBuilder s_division_name(pool);
  arrow::Int32Builder s_company_id(pool);
  arrow::StringBuilder s_company_name(pool);
  arrow::StringBuilder s_street_number(pool);
  arrow::StringBuilder s_street_name(pool);
  arrow::StringBuilder s_street_type(pool);
  arrow::StringBuilder s_suite_number(pool);
  arrow::StringBuilder s_city(pool);
  arrow::StringBuilder s_county(pool);
  arrow::StringBuilder s_state(pool);
  arrow::StringBuilder s_zip(pool);
  arrow::StringBuilder s_country(pool);
  arrow::FloatBuilder s_gmt_offset(pool);
  arrow::Decimal32Builder s_tax_percentage(arrow::smallest_decimal(5, 2), pool);

#define TPCDS_RETURN_NOT_OK(status)   \
  do {                                \
    arrow::Status _status = (status); \
    if (!_status.ok()) {              \
      return _status;                 \
    }                                 \
  } while (false)

  TPCDS_RETURN_NOT_OK(s_store_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(s_store_id.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(s_rec_start_date_id.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(s_rec_end_date_id.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(s_closed_date_id.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(s_store_name.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(s_employees.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(s_floor_space.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(s_hours.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(s_store_manager.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(s_market_id.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(s_geography_class.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(s_market_desc.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(s_market_manager.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(s_division_id.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(s_division_name.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(s_company_id.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(s_company_name.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(s_street_number.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(s_street_name.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(s_street_type.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(s_suite_number.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(s_city.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(s_county.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(s_state.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(s_zip.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(s_country.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(s_gmt_offset.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(s_tax_percentage.Reserve(batch_rows));

  for (int64_t i = 0; i < batch_rows; ++i) {
    int64_t row_number = impl_->current_row_ + 1;
    internal::StoreRowData row = impl_->row_generator_.GenerateRow(row_number);

    auto is_null = [&](int column_id) {
      return internal::IsNull(row.null_bitmap, STORE, column_id);
    };

    if (is_null(W_STORE_SK)) {
      TPCDS_RETURN_NOT_OK(s_store_sk.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(s_store_sk.Append(row.store_sk));
    }

    if (is_null(W_STORE_ID)) {
      TPCDS_RETURN_NOT_OK(s_store_id.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(s_store_id.Append(row.store_id));
    }

    if (is_null(W_STORE_REC_START_DATE_ID) || row.rec_start_date_id <= 0) {
      TPCDS_RETURN_NOT_OK(s_rec_start_date_id.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(
          s_rec_start_date_id.Append(Date32FromJulian(row.rec_start_date_id)));
    }

    if (is_null(W_STORE_REC_END_DATE_ID) || row.rec_end_date_id <= 0) {
      TPCDS_RETURN_NOT_OK(s_rec_end_date_id.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(
          s_rec_end_date_id.Append(Date32FromJulian(row.rec_end_date_id)));
    }

    if (is_null(W_STORE_CLOSED_DATE_ID) || row.closed_date_id == -1) {
      TPCDS_RETURN_NOT_OK(s_closed_date_id.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(s_closed_date_id.Append(row.closed_date_id));
    }

    if (is_null(W_STORE_NAME)) {
      TPCDS_RETURN_NOT_OK(s_store_name.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(s_store_name.Append(row.store_name));
    }

    if (is_null(W_STORE_EMPLOYEES)) {
      TPCDS_RETURN_NOT_OK(s_employees.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(s_employees.Append(row.employees));
    }

    if (is_null(W_STORE_FLOOR_SPACE)) {
      TPCDS_RETURN_NOT_OK(s_floor_space.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(s_floor_space.Append(row.floor_space));
    }

    if (is_null(W_STORE_HOURS)) {
      TPCDS_RETURN_NOT_OK(s_hours.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(s_hours.Append(row.hours));
    }

    if (is_null(W_STORE_MANAGER)) {
      TPCDS_RETURN_NOT_OK(s_store_manager.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(s_store_manager.Append(row.store_manager));
    }

    if (is_null(W_STORE_MARKET_ID)) {
      TPCDS_RETURN_NOT_OK(s_market_id.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(s_market_id.Append(row.market_id));
    }

    if (is_null(W_STORE_GEOGRAPHY_CLASS)) {
      TPCDS_RETURN_NOT_OK(s_geography_class.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(s_geography_class.Append(row.geography_class));
    }

    if (is_null(W_STORE_MARKET_DESC)) {
      TPCDS_RETURN_NOT_OK(s_market_desc.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(s_market_desc.Append(row.market_desc));
    }

    if (is_null(W_STORE_MARKET_MANAGER)) {
      TPCDS_RETURN_NOT_OK(s_market_manager.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(s_market_manager.Append(row.market_manager));
    }

    if (is_null(W_STORE_DIVISION_ID)) {
      TPCDS_RETURN_NOT_OK(s_division_id.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(s_division_id.Append(row.division_id));
    }

    if (is_null(W_STORE_DIVISION_NAME)) {
      TPCDS_RETURN_NOT_OK(s_division_name.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(s_division_name.Append(row.division_name));
    }

    if (is_null(W_STORE_COMPANY_ID)) {
      TPCDS_RETURN_NOT_OK(s_company_id.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(s_company_id.Append(row.company_id));
    }

    if (is_null(W_STORE_COMPANY_NAME)) {
      TPCDS_RETURN_NOT_OK(s_company_name.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(s_company_name.Append(row.company_name));
    }

    if (is_null(W_STORE_ADDRESS_STREET_NUM)) {
      TPCDS_RETURN_NOT_OK(s_street_number.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(
          s_street_number.Append(std::to_string(row.address.street_num)));
    }

    if (is_null(W_STORE_ADDRESS_STREET_NAME1)) {
      TPCDS_RETURN_NOT_OK(s_street_name.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(s_street_name.Append(FormatStreetName(row.address)));
    }

    if (is_null(W_STORE_ADDRESS_STREET_TYPE)) {
      TPCDS_RETURN_NOT_OK(s_street_type.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(s_street_type.Append(row.address.street_type));
    }

    if (is_null(W_STORE_ADDRESS_SUITE_NUM)) {
      TPCDS_RETURN_NOT_OK(s_suite_number.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(s_suite_number.Append(row.address.suite_num));
    }

    if (is_null(W_STORE_ADDRESS_CITY)) {
      TPCDS_RETURN_NOT_OK(s_city.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(s_city.Append(row.address.city));
    }

    if (is_null(W_STORE_ADDRESS_COUNTY)) {
      TPCDS_RETURN_NOT_OK(s_county.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(s_county.Append(row.address.county));
    }

    if (is_null(W_STORE_ADDRESS_STATE)) {
      TPCDS_RETURN_NOT_OK(s_state.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(s_state.Append(row.address.state));
    }

    if (is_null(W_STORE_ADDRESS_ZIP)) {
      TPCDS_RETURN_NOT_OK(s_zip.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(s_zip.Append(FormatZip(row.address.zip)));
    }

    if (is_null(W_STORE_ADDRESS_COUNTRY)) {
      TPCDS_RETURN_NOT_OK(s_country.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(s_country.Append(row.address.country));
    }

    if (is_null(W_STORE_ADDRESS_GMT_OFFSET)) {
      TPCDS_RETURN_NOT_OK(s_gmt_offset.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(
          s_gmt_offset.Append(static_cast<float>(row.address.gmt_offset)));
    }

    if (is_null(W_STORE_TAX_PERCENTAGE)) {
      TPCDS_RETURN_NOT_OK(s_tax_percentage.AppendNull());
    } else {
      arrow::Decimal32 tax_val(row.tax_percentage.number);
      TPCDS_RETURN_NOT_OK(s_tax_percentage.Append(tax_val));
    }

    impl_->row_generator_.ConsumeRemainingSeedsForRow();
    ++impl_->current_row_;
    --impl_->remaining_rows_;
  }

  std::vector<std::shared_ptr<arrow::Array>> arrays;
  arrays.reserve(29);
  std::shared_ptr<arrow::Array> array;

  TPCDS_RETURN_NOT_OK(s_store_sk.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(s_store_id.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(s_rec_start_date_id.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(s_rec_end_date_id.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(s_closed_date_id.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(s_store_name.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(s_employees.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(s_floor_space.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(s_hours.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(s_store_manager.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(s_market_id.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(s_geography_class.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(s_market_desc.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(s_market_manager.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(s_division_id.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(s_division_name.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(s_company_id.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(s_company_name.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(s_street_number.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(s_street_name.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(s_street_type.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(s_suite_number.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(s_city.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(s_county.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(s_state.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(s_zip.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(s_country.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(s_gmt_offset.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(s_tax_percentage.Finish(&array));
  arrays.push_back(array);

  return impl_->column_selection_.MakeRecordBatch(batch_rows, std::move(arrays),
                                                  out);
}

int64_t StoreGenerator::total_rows() const { return impl_->total_rows_; }

int64_t StoreGenerator::remaining_rows() const {
  return impl_->remaining_rows_;
}

int64_t StoreGenerator::TotalRows(double scale_factor) {
  return internal::Scaling(scale_factor).RowCountByTableNumber(STORE);
}

}  // namespace benchgen::tpcds
