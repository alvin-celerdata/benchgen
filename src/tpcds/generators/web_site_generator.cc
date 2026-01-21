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

#include "generators/web_site_generator.h"

#include <algorithm>
#include <cstdio>
#include <stdexcept>
#include <string>

#include "distribution/scaling.h"
#include "generators/web_site_row_generator.h"
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

std::shared_ptr<arrow::Schema> BuildWebSiteSchema() {
  return arrow::schema({
      arrow::field("web_site_sk", arrow::int64(), false),
      arrow::field("web_site_id", arrow::utf8(), false),
      arrow::field("web_rec_start_date", arrow::date32()),
      arrow::field("web_rec_end_date", arrow::date32()),
      arrow::field("web_name", arrow::utf8()),
      arrow::field("web_open_date_sk", arrow::int32()),
      arrow::field("web_close_date_sk", arrow::int32()),
      arrow::field("web_class", arrow::utf8()),
      arrow::field("web_manager", arrow::utf8()),
      arrow::field("web_mkt_id", arrow::int32()),
      arrow::field("web_mkt_class", arrow::utf8()),
      arrow::field("web_mkt_desc", arrow::utf8()),
      arrow::field("web_market_manager", arrow::utf8()),
      arrow::field("web_company_id", arrow::int32()),
      arrow::field("web_company_name", arrow::utf8()),
      arrow::field("web_street_number", arrow::utf8()),
      arrow::field("web_street_name", arrow::utf8()),
      arrow::field("web_street_type", arrow::utf8()),
      arrow::field("web_suite_number", arrow::utf8()),
      arrow::field("web_city", arrow::utf8()),
      arrow::field("web_county", arrow::utf8()),
      arrow::field("web_state", arrow::utf8()),
      arrow::field("web_zip", arrow::utf8()),
      arrow::field("web_country", arrow::utf8()),
      arrow::field("web_gmt_offset", arrow::float32()),
      arrow::field("web_tax_percentage", arrow::smallest_decimal(5, 2)),
  });
}

}  // namespace

struct WebSiteGenerator::Impl {
  explicit Impl(GeneratorOptions options)
      : options_(std::move(options)),
        schema_(BuildWebSiteSchema()),
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
            .RowCountByTableNumber(WEB_SITE);
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
  internal::WebSiteRowGenerator row_generator_;
};

WebSiteGenerator::WebSiteGenerator(GeneratorOptions options)
    : impl_(std::make_unique<Impl>(std::move(options))) {}

WebSiteGenerator::~WebSiteGenerator() = default;

std::shared_ptr<arrow::Schema> WebSiteGenerator::schema() const {
  return impl_->schema_;
}

std::string_view WebSiteGenerator::name() const {
  return TableIdToString(TableId::kWebSite);
}

std::string_view WebSiteGenerator::suite_name() const { return "tpcds"; }

arrow::Status WebSiteGenerator::Next(std::shared_ptr<arrow::RecordBatch>* out) {
  if (impl_->remaining_rows_ == 0) {
    *out = nullptr;
    return arrow::Status::OK();
  }

  const int64_t batch_rows =
      std::min(impl_->remaining_rows_, impl_->options_.chunk_size);

  arrow::MemoryPool* pool = arrow::default_memory_pool();
  arrow::Int64Builder web_site_sk(pool);
  arrow::StringBuilder web_site_id(pool);
  arrow::Date32Builder web_rec_start_date_id(pool);
  arrow::Date32Builder web_rec_end_date_id(pool);
  arrow::StringBuilder web_name(pool);
  arrow::Int32Builder web_open_date(pool);
  arrow::Int32Builder web_close_date(pool);
  arrow::StringBuilder web_class(pool);
  arrow::StringBuilder web_manager(pool);
  arrow::Int32Builder web_market_id(pool);
  arrow::StringBuilder web_market_class(pool);
  arrow::StringBuilder web_market_desc(pool);
  arrow::StringBuilder web_market_manager(pool);
  arrow::Int32Builder web_company_id(pool);
  arrow::StringBuilder web_company_name(pool);
  arrow::StringBuilder web_street_number(pool);
  arrow::StringBuilder web_street_name(pool);
  arrow::StringBuilder web_street_type(pool);
  arrow::StringBuilder web_suite_number(pool);
  arrow::StringBuilder web_city(pool);
  arrow::StringBuilder web_county(pool);
  arrow::StringBuilder web_state(pool);
  arrow::StringBuilder web_zip(pool);
  arrow::StringBuilder web_country(pool);
  arrow::FloatBuilder web_gmt_offset(pool);
  arrow::Decimal32Builder web_tax_percentage(arrow::smallest_decimal(5, 2),
                                             pool);

#define TPCDS_RETURN_NOT_OK(status)   \
  do {                                \
    arrow::Status _status = (status); \
    if (!_status.ok()) {              \
      return _status;                 \
    }                                 \
  } while (false)

  TPCDS_RETURN_NOT_OK(web_site_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(web_site_id.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(web_rec_start_date_id.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(web_rec_end_date_id.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(web_name.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(web_open_date.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(web_close_date.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(web_class.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(web_manager.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(web_market_id.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(web_market_class.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(web_market_desc.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(web_market_manager.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(web_company_id.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(web_company_name.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(web_street_number.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(web_street_name.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(web_street_type.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(web_suite_number.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(web_city.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(web_county.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(web_state.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(web_zip.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(web_country.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(web_gmt_offset.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(web_tax_percentage.Reserve(batch_rows));

  for (int64_t i = 0; i < batch_rows; ++i) {
    int64_t row_number = impl_->current_row_ + 1;
    internal::WebSiteRowData row =
        impl_->row_generator_.GenerateRow(row_number);

    auto is_null = [&](int column_id) {
      return internal::IsNull(row.null_bitmap, WEB_SITE, column_id);
    };

    if (is_null(WEB_SITE_SK)) {
      TPCDS_RETURN_NOT_OK(web_site_sk.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(web_site_sk.Append(row.site_sk));
    }

    if (is_null(WEB_SITE_ID)) {
      TPCDS_RETURN_NOT_OK(web_site_id.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(web_site_id.Append(row.site_id));
    }

    if (is_null(WEB_REC_START_DATE_ID) || row.rec_start_date_id <= 0) {
      TPCDS_RETURN_NOT_OK(web_rec_start_date_id.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(web_rec_start_date_id.Append(
          Date32FromJulian(row.rec_start_date_id)));
    }

    if (is_null(WEB_REC_END_DATE_ID) || row.rec_end_date_id <= 0) {
      TPCDS_RETURN_NOT_OK(web_rec_end_date_id.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(
          web_rec_end_date_id.Append(Date32FromJulian(row.rec_end_date_id)));
    }

    if (is_null(WEB_NAME)) {
      TPCDS_RETURN_NOT_OK(web_name.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(web_name.Append(row.name));
    }

    if (is_null(WEB_OPEN_DATE) || row.open_date == -1) {
      TPCDS_RETURN_NOT_OK(web_open_date.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(web_open_date.Append(row.open_date));
    }

    if (is_null(WEB_CLOSE_DATE) || row.close_date == -1) {
      TPCDS_RETURN_NOT_OK(web_close_date.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(web_close_date.Append(row.close_date));
    }

    if (is_null(WEB_CLASS)) {
      TPCDS_RETURN_NOT_OK(web_class.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(web_class.Append(row.class_name));
    }

    if (is_null(WEB_MANAGER)) {
      TPCDS_RETURN_NOT_OK(web_manager.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(web_manager.Append(row.manager));
    }

    if (is_null(WEB_MARKET_ID)) {
      TPCDS_RETURN_NOT_OK(web_market_id.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(web_market_id.Append(row.market_id));
    }

    if (is_null(WEB_MARKET_CLASS)) {
      TPCDS_RETURN_NOT_OK(web_market_class.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(web_market_class.Append(row.market_class));
    }

    if (is_null(WEB_MARKET_DESC)) {
      TPCDS_RETURN_NOT_OK(web_market_desc.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(web_market_desc.Append(row.market_desc));
    }

    if (is_null(WEB_MARKET_MANAGER)) {
      TPCDS_RETURN_NOT_OK(web_market_manager.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(web_market_manager.Append(row.market_manager));
    }

    if (is_null(WEB_COMPANY_ID)) {
      TPCDS_RETURN_NOT_OK(web_company_id.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(web_company_id.Append(row.company_id));
    }

    if (is_null(WEB_COMPANY_NAME)) {
      TPCDS_RETURN_NOT_OK(web_company_name.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(web_company_name.Append(row.company_name));
    }

    if (is_null(WEB_ADDRESS_STREET_NUM)) {
      TPCDS_RETURN_NOT_OK(web_street_number.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(
          web_street_number.Append(std::to_string(row.address.street_num)));
    }

    if (is_null(WEB_ADDRESS_STREET_NAME1)) {
      TPCDS_RETURN_NOT_OK(web_street_name.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(
          web_street_name.Append(FormatStreetName(row.address)));
    }

    if (is_null(WEB_ADDRESS_STREET_TYPE)) {
      TPCDS_RETURN_NOT_OK(web_street_type.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(web_street_type.Append(row.address.street_type));
    }

    if (is_null(WEB_ADDRESS_SUITE_NUM)) {
      TPCDS_RETURN_NOT_OK(web_suite_number.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(web_suite_number.Append(row.address.suite_num));
    }

    if (is_null(WEB_ADDRESS_CITY)) {
      TPCDS_RETURN_NOT_OK(web_city.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(web_city.Append(row.address.city));
    }

    if (is_null(WEB_ADDRESS_COUNTY)) {
      TPCDS_RETURN_NOT_OK(web_county.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(web_county.Append(row.address.county));
    }

    if (is_null(WEB_ADDRESS_STATE)) {
      TPCDS_RETURN_NOT_OK(web_state.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(web_state.Append(row.address.state));
    }

    if (is_null(WEB_ADDRESS_ZIP)) {
      TPCDS_RETURN_NOT_OK(web_zip.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(web_zip.Append(FormatZip(row.address.zip)));
    }

    if (is_null(WEB_ADDRESS_COUNTRY)) {
      TPCDS_RETURN_NOT_OK(web_country.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(web_country.Append(row.address.country));
    }

    if (is_null(WEB_ADDRESS_GMT_OFFSET)) {
      TPCDS_RETURN_NOT_OK(web_gmt_offset.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(
          web_gmt_offset.Append(static_cast<float>(row.address.gmt_offset)));
    }

    if (is_null(WEB_TAX_PERCENTAGE)) {
      TPCDS_RETURN_NOT_OK(web_tax_percentage.AppendNull());
    } else {
      arrow::Decimal32 tax_val(row.tax_percentage.number);
      TPCDS_RETURN_NOT_OK(web_tax_percentage.Append(tax_val));
    }

    impl_->row_generator_.ConsumeRemainingSeedsForRow();
    ++impl_->current_row_;
    --impl_->remaining_rows_;
  }

  std::vector<std::shared_ptr<arrow::Array>> arrays;
  arrays.reserve(26);
  std::shared_ptr<arrow::Array> array;

  TPCDS_RETURN_NOT_OK(web_site_sk.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(web_site_id.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(web_rec_start_date_id.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(web_rec_end_date_id.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(web_name.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(web_open_date.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(web_close_date.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(web_class.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(web_manager.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(web_market_id.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(web_market_class.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(web_market_desc.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(web_market_manager.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(web_company_id.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(web_company_name.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(web_street_number.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(web_street_name.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(web_street_type.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(web_suite_number.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(web_city.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(web_county.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(web_state.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(web_zip.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(web_country.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(web_gmt_offset.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(web_tax_percentage.Finish(&array));
  arrays.push_back(array);

  return impl_->column_selection_.MakeRecordBatch(batch_rows, std::move(arrays),
                                                  out);
}

int64_t WebSiteGenerator::total_rows() const { return impl_->total_rows_; }

int64_t WebSiteGenerator::remaining_rows() const {
  return impl_->remaining_rows_;
}

int64_t WebSiteGenerator::TotalRows(double scale_factor) {
  return internal::Scaling(scale_factor).RowCountByTableNumber(WEB_SITE);
}

}  // namespace benchgen::tpcds
