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

#include "generators/call_center_generator.h"

#include <algorithm>
#include <cstdio>
#include <stdexcept>
#include <string>

#include "distribution/scaling.h"
#include "generators/call_center_row_generator.h"
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

std::shared_ptr<arrow::Schema> BuildCallCenterSchema() {
  return arrow::schema({
      arrow::field("cc_call_center_sk", arrow::int64(), false),
      arrow::field("cc_call_center_id", arrow::utf8(), false),
      arrow::field("cc_rec_start_date", arrow::date32()),
      arrow::field("cc_rec_end_date", arrow::date32()),
      arrow::field("cc_closed_date_sk", arrow::int32()),
      arrow::field("cc_open_date_sk", arrow::int32()),
      arrow::field("cc_name", arrow::utf8()),
      arrow::field("cc_class", arrow::utf8()),
      arrow::field("cc_employees", arrow::int32()),
      arrow::field("cc_sq_ft", arrow::int32()),
      arrow::field("cc_hours", arrow::utf8()),
      arrow::field("cc_manager", arrow::utf8()),
      arrow::field("cc_mkt_id", arrow::int32()),
      arrow::field("cc_mkt_class", arrow::utf8()),
      arrow::field("cc_mkt_desc", arrow::utf8()),
      arrow::field("cc_market_manager", arrow::utf8()),
      arrow::field("cc_division", arrow::int32()),
      arrow::field("cc_division_name", arrow::utf8()),
      arrow::field("cc_company", arrow::int32()),
      arrow::field("cc_company_name", arrow::utf8()),
      arrow::field("cc_street_number", arrow::utf8()),
      arrow::field("cc_street_name", arrow::utf8()),
      arrow::field("cc_street_type", arrow::utf8()),
      arrow::field("cc_suite_number", arrow::utf8()),
      arrow::field("cc_city", arrow::utf8()),
      arrow::field("cc_county", arrow::utf8()),
      arrow::field("cc_state", arrow::utf8()),
      arrow::field("cc_zip", arrow::utf8()),
      arrow::field("cc_country", arrow::utf8()),
      arrow::field("cc_gmt_offset", arrow::float32()),
      arrow::field("cc_tax_percentage", arrow::smallest_decimal(5, 2)),
  });
}

}  // namespace

struct CallCenterGenerator::Impl {
  explicit Impl(GeneratorOptions options)
      : options_(std::move(options)),
        schema_(BuildCallCenterSchema()),
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
            .RowCountByTableNumber(CALL_CENTER);
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
  internal::CallCenterRowGenerator row_generator_;
};

CallCenterGenerator::CallCenterGenerator(GeneratorOptions options)
    : impl_(std::make_unique<Impl>(std::move(options))) {}

CallCenterGenerator::~CallCenterGenerator() = default;

std::shared_ptr<arrow::Schema> CallCenterGenerator::schema() const {
  return impl_->schema_;
}

std::string_view CallCenterGenerator::name() const {
  return TableIdToString(TableId::kCallCenter);
}

std::string_view CallCenterGenerator::suite_name() const { return "tpcds"; }

arrow::Status CallCenterGenerator::Next(
    std::shared_ptr<arrow::RecordBatch>* out) {
  if (impl_->remaining_rows_ == 0) {
    *out = nullptr;
    return arrow::Status::OK();
  }

  const int64_t batch_rows =
      std::min(impl_->remaining_rows_, impl_->options_.chunk_size);

  arrow::MemoryPool* pool = arrow::default_memory_pool();
  arrow::Int64Builder cc_call_center_sk(pool);
  arrow::StringBuilder cc_call_center_id(pool);
  arrow::Date32Builder cc_rec_start_date_id(pool);
  arrow::Date32Builder cc_rec_end_date_id(pool);
  arrow::Int32Builder cc_closed_date_id(pool);
  arrow::Int32Builder cc_open_date_id(pool);
  arrow::StringBuilder cc_name(pool);
  arrow::StringBuilder cc_class(pool);
  arrow::Int32Builder cc_employees(pool);
  arrow::Int32Builder cc_sq_ft(pool);
  arrow::StringBuilder cc_hours(pool);
  arrow::StringBuilder cc_manager(pool);
  arrow::Int32Builder cc_market_id(pool);
  arrow::StringBuilder cc_market_class(pool);
  arrow::StringBuilder cc_market_desc(pool);
  arrow::StringBuilder cc_market_manager(pool);
  arrow::Int32Builder cc_division_id(pool);
  arrow::StringBuilder cc_division_name(pool);
  arrow::Int32Builder cc_company_id(pool);
  arrow::StringBuilder cc_company_name(pool);
  arrow::StringBuilder cc_street_number(pool);
  arrow::StringBuilder cc_street_name(pool);
  arrow::StringBuilder cc_street_type(pool);
  arrow::StringBuilder cc_suite_number(pool);
  arrow::StringBuilder cc_city(pool);
  arrow::StringBuilder cc_county(pool);
  arrow::StringBuilder cc_state(pool);
  arrow::StringBuilder cc_zip(pool);
  arrow::StringBuilder cc_country(pool);
  arrow::FloatBuilder cc_gmt_offset(pool);
  arrow::Decimal32Builder cc_tax_percentage(arrow::smallest_decimal(5, 2),
                                            pool);

#define TPCDS_RETURN_NOT_OK(status)   \
  do {                                \
    arrow::Status _status = (status); \
    if (!_status.ok()) {              \
      return _status;                 \
    }                                 \
  } while (false)

  TPCDS_RETURN_NOT_OK(cc_call_center_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cc_call_center_id.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cc_rec_start_date_id.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cc_rec_end_date_id.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cc_closed_date_id.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cc_open_date_id.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cc_name.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cc_class.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cc_employees.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cc_sq_ft.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cc_hours.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cc_manager.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cc_market_id.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cc_market_class.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cc_market_desc.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cc_market_manager.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cc_division_id.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cc_division_name.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cc_company_id.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cc_company_name.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cc_street_number.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cc_street_name.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cc_street_type.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cc_suite_number.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cc_city.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cc_county.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cc_state.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cc_zip.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cc_country.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cc_gmt_offset.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cc_tax_percentage.Reserve(batch_rows));

  for (int64_t i = 0; i < batch_rows; ++i) {
    int64_t row_number = impl_->current_row_ + 1;
    internal::CallCenterRowData row =
        impl_->row_generator_.GenerateRow(row_number);

    auto is_null = [&](int column_id) {
      return internal::IsNull(row.null_bitmap, CALL_CENTER, column_id);
    };

    if (is_null(CC_CALL_CENTER_SK)) {
      TPCDS_RETURN_NOT_OK(cc_call_center_sk.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(cc_call_center_sk.Append(row.call_center_sk));
    }

    if (is_null(CC_CALL_CENTER_ID)) {
      TPCDS_RETURN_NOT_OK(cc_call_center_id.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(cc_call_center_id.Append(row.call_center_id));
    }

    if (is_null(CC_REC_START_DATE_ID) || row.rec_start_date_id <= 0) {
      TPCDS_RETURN_NOT_OK(cc_rec_start_date_id.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(
          cc_rec_start_date_id.Append(Date32FromJulian(row.rec_start_date_id)));
    }

    if (is_null(CC_REC_END_DATE_ID) || row.rec_end_date_id <= 0) {
      TPCDS_RETURN_NOT_OK(cc_rec_end_date_id.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(
          cc_rec_end_date_id.Append(Date32FromJulian(row.rec_end_date_id)));
    }

    if (is_null(CC_CLOSED_DATE_ID) || row.closed_date_id == -1) {
      TPCDS_RETURN_NOT_OK(cc_closed_date_id.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(cc_closed_date_id.Append(row.closed_date_id));
    }

    if (is_null(CC_OPEN_DATE_ID) || row.open_date_id == -1) {
      TPCDS_RETURN_NOT_OK(cc_open_date_id.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(cc_open_date_id.Append(row.open_date_id));
    }

    if (is_null(CC_NAME)) {
      TPCDS_RETURN_NOT_OK(cc_name.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(cc_name.Append(row.name));
    }

    if (is_null(CC_CLASS)) {
      TPCDS_RETURN_NOT_OK(cc_class.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(cc_class.Append(row.class_name));
    }

    if (is_null(CC_EMPLOYEES)) {
      TPCDS_RETURN_NOT_OK(cc_employees.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(cc_employees.Append(row.employees));
    }

    if (is_null(CC_SQ_FT)) {
      TPCDS_RETURN_NOT_OK(cc_sq_ft.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(cc_sq_ft.Append(row.sq_ft));
    }

    if (is_null(CC_HOURS)) {
      TPCDS_RETURN_NOT_OK(cc_hours.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(cc_hours.Append(row.hours));
    }

    if (is_null(CC_MANAGER)) {
      TPCDS_RETURN_NOT_OK(cc_manager.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(cc_manager.Append(row.manager));
    }

    if (is_null(CC_MARKET_ID)) {
      TPCDS_RETURN_NOT_OK(cc_market_id.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(cc_market_id.Append(row.market_id));
    }

    if (is_null(CC_MARKET_CLASS)) {
      TPCDS_RETURN_NOT_OK(cc_market_class.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(cc_market_class.Append(row.market_class));
    }

    if (is_null(CC_MARKET_DESC)) {
      TPCDS_RETURN_NOT_OK(cc_market_desc.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(cc_market_desc.Append(row.market_desc));
    }

    if (is_null(CC_MARKET_MANAGER)) {
      TPCDS_RETURN_NOT_OK(cc_market_manager.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(cc_market_manager.Append(row.market_manager));
    }

    if (is_null(CC_DIVISION)) {
      TPCDS_RETURN_NOT_OK(cc_division_id.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(cc_division_id.Append(row.division_id));
    }

    if (is_null(CC_DIVISION_NAME)) {
      TPCDS_RETURN_NOT_OK(cc_division_name.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(cc_division_name.Append(row.division_name));
    }

    if (is_null(CC_COMPANY)) {
      TPCDS_RETURN_NOT_OK(cc_company_id.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(cc_company_id.Append(row.company));
    }

    if (is_null(CC_COMPANY_NAME)) {
      TPCDS_RETURN_NOT_OK(cc_company_name.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(cc_company_name.Append(row.company_name));
    }

    if (is_null(CC_STREET_NUMBER)) {
      TPCDS_RETURN_NOT_OK(cc_street_number.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(
          cc_street_number.Append(std::to_string(row.address.street_num)));
    }

    if (is_null(CC_STREET_NAME)) {
      TPCDS_RETURN_NOT_OK(cc_street_name.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(cc_street_name.Append(FormatStreetName(row.address)));
    }

    if (is_null(CC_STREET_TYPE)) {
      TPCDS_RETURN_NOT_OK(cc_street_type.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(cc_street_type.Append(row.address.street_type));
    }

    if (is_null(CC_SUITE_NUMBER)) {
      TPCDS_RETURN_NOT_OK(cc_suite_number.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(cc_suite_number.Append(row.address.suite_num));
    }

    if (is_null(CC_CITY)) {
      TPCDS_RETURN_NOT_OK(cc_city.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(cc_city.Append(row.address.city));
    }

    if (is_null(CC_COUNTY)) {
      TPCDS_RETURN_NOT_OK(cc_county.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(cc_county.Append(row.address.county));
    }

    if (is_null(CC_STATE)) {
      TPCDS_RETURN_NOT_OK(cc_state.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(cc_state.Append(row.address.state));
    }

    if (is_null(CC_ZIP)) {
      TPCDS_RETURN_NOT_OK(cc_zip.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(cc_zip.Append(FormatZip(row.address.zip)));
    }

    if (is_null(CC_COUNTRY)) {
      TPCDS_RETURN_NOT_OK(cc_country.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(cc_country.Append(row.address.country));
    }

    if (is_null(CC_GMT_OFFSET)) {
      TPCDS_RETURN_NOT_OK(cc_gmt_offset.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(
          cc_gmt_offset.Append(static_cast<float>(row.address.gmt_offset)));
    }

    if (is_null(CC_TAX_PERCENTAGE)) {
      TPCDS_RETURN_NOT_OK(cc_tax_percentage.AppendNull());
    } else {
      arrow::Decimal32 tax_val(row.tax_percentage.number);
      TPCDS_RETURN_NOT_OK(cc_tax_percentage.Append(tax_val));
    }

    impl_->row_generator_.ConsumeRemainingSeedsForRow();
    ++impl_->current_row_;
    --impl_->remaining_rows_;
  }

  std::vector<std::shared_ptr<arrow::Array>> arrays;
  arrays.reserve(30);
  std::shared_ptr<arrow::Array> array;

  TPCDS_RETURN_NOT_OK(cc_call_center_sk.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cc_call_center_id.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cc_rec_start_date_id.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cc_rec_end_date_id.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cc_closed_date_id.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cc_open_date_id.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cc_name.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cc_class.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cc_employees.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cc_sq_ft.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cc_hours.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cc_manager.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cc_market_id.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cc_market_class.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cc_market_desc.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cc_market_manager.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cc_division_id.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cc_division_name.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cc_company_id.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cc_company_name.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cc_street_number.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cc_street_name.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cc_street_type.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cc_suite_number.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cc_city.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cc_county.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cc_state.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cc_zip.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cc_country.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cc_gmt_offset.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cc_tax_percentage.Finish(&array));
  arrays.push_back(array);

  return impl_->column_selection_.MakeRecordBatch(batch_rows, std::move(arrays),
                                                  out);
}

int64_t CallCenterGenerator::total_rows() const { return impl_->total_rows_; }

int64_t CallCenterGenerator::remaining_rows() const {
  return impl_->remaining_rows_;
}

int64_t CallCenterGenerator::TotalRows(double scale_factor) {
  return internal::Scaling(scale_factor).RowCountByTableNumber(CALL_CENTER);
}

}  // namespace benchgen::tpcds
