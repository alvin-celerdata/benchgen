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

#include "benchgen/arrow_compat.h"
#include "util/column_selection.h"
#include "utils/tpcds_internal.h"

namespace benchgen::tpcds {
namespace {

#define TPCDS_RETURN_NOT_OK(status)     \
  do {                                  \
    ::arrow::Status _status = (status); \
    if (!_status.ok()) {                \
      return _status;                   \
    }                                   \
  } while (false)

std::shared_ptr<arrow::Schema> BuildCustomerSchema() {
  return arrow::schema({
      arrow::field("c_customer_sk", arrow::int64(), false),
      arrow::field("c_customer_id", arrow::utf8(), false),
      arrow::field("c_current_cdemo_sk", arrow::int64()),
      arrow::field("c_current_hdemo_sk", arrow::int64()),
      arrow::field("c_current_addr_sk", arrow::int64()),
      arrow::field("c_first_shipto_date_sk", arrow::int32()),
      arrow::field("c_first_sales_date_sk", arrow::int32()),
      arrow::field("c_salutation", arrow::utf8()),
      arrow::field("c_first_name", arrow::utf8()),
      arrow::field("c_last_name", arrow::utf8()),
      arrow::field("c_preferred_cust_flag", arrow::boolean()),
      arrow::field("c_birth_day", arrow::int32()),
      arrow::field("c_birth_month", arrow::int32()),
      arrow::field("c_birth_year", arrow::int32()),
      arrow::field("c_birth_country", arrow::utf8()),
      arrow::field("c_login", arrow::utf8()),
      arrow::field("c_email_address", arrow::utf8()),
      arrow::field("c_last_review_date_sk", arrow::int32()),
  });
}

}  // namespace

struct CustomerGenerator::Impl {
  explicit Impl(GeneratorOptions options)
      : options_(std::move(options)),
        schema_(BuildCustomerSchema()),
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
            .RowCount(TableId::kCustomer);
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

arrow::Status CustomerGenerator::Init() { return impl_->Init(); }

CustomerGenerator::~CustomerGenerator() = default;

std::shared_ptr<arrow::Schema> CustomerGenerator::schema() const {
  return impl_->schema_;
}

std::string_view CustomerGenerator::name() const {
  return TableIdToString(TableId::kCustomer);
}

std::string_view CustomerGenerator::suite_name() const { return "tpcds"; }

arrow::Status CustomerGenerator::Next(
    std::shared_ptr<arrow::RecordBatch>* out) {
  if (impl_->remaining_rows_ == 0) {
    *out = nullptr;
    return arrow::Status::OK();
  }

  const int64_t batch_rows =
      std::min(impl_->remaining_rows_, impl_->options_.chunk_size);

  arrow::MemoryPool* pool = arrow::default_memory_pool();
  arrow::Int64Builder c_customer_sk(pool);
  arrow::StringBuilder c_customer_id(pool);
  arrow::Int64Builder c_current_cdemo_sk(pool);
  arrow::Int64Builder c_current_hdemo_sk(pool);
  arrow::Int64Builder c_current_addr_sk(pool);
  arrow::Int32Builder c_first_shipto_date_sk(pool);
  arrow::Int32Builder c_first_sales_date_sk(pool);
  arrow::StringBuilder c_salutation(pool);
  arrow::StringBuilder c_first_name(pool);
  arrow::StringBuilder c_last_name(pool);
  arrow::BooleanBuilder c_preferred_cust_flag(pool);
  arrow::Int32Builder c_birth_day(pool);
  arrow::Int32Builder c_birth_month(pool);
  arrow::Int32Builder c_birth_year(pool);
  arrow::StringBuilder c_birth_country(pool);
  arrow::StringBuilder c_login(pool);
  arrow::StringBuilder c_email_address(pool);
  arrow::Int32Builder c_last_review_date_sk(pool);

  TPCDS_RETURN_NOT_OK(c_customer_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(c_customer_id.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(c_current_cdemo_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(c_current_hdemo_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(c_current_addr_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(c_first_shipto_date_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(c_first_sales_date_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(c_salutation.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(c_first_name.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(c_last_name.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(c_preferred_cust_flag.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(c_birth_day.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(c_birth_month.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(c_birth_year.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(c_birth_country.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(c_login.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(c_email_address.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(c_last_review_date_sk.Reserve(batch_rows));

  for (int64_t i = 0; i < batch_rows; ++i) {
    int64_t row_number = impl_->current_row_ + 1;
    internal::CustomerRowData row =
        impl_->row_generator_.GenerateRow(row_number);

    auto is_null = [&](internal::CustomerGeneratorColumn column) {
      return internal::CustomerRowGenerator::IsNull(row.null_bitmap, column);
    };

    if (is_null(internal::CustomerGeneratorColumn::kCustomerSk)) {
      TPCDS_RETURN_NOT_OK(c_customer_sk.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(c_customer_sk.Append(row.customer_sk));
    }

    if (is_null(internal::CustomerGeneratorColumn::kCustomerId)) {
      TPCDS_RETURN_NOT_OK(c_customer_id.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(c_customer_id.Append(row.customer_id));
    }

    if (is_null(internal::CustomerGeneratorColumn::kCurrentCdemoSk)) {
      TPCDS_RETURN_NOT_OK(c_current_cdemo_sk.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(c_current_cdemo_sk.Append(row.current_cdemo_sk));
    }

    if (is_null(internal::CustomerGeneratorColumn::kCurrentHdemoSk)) {
      TPCDS_RETURN_NOT_OK(c_current_hdemo_sk.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(c_current_hdemo_sk.Append(row.current_hdemo_sk));
    }

    if (is_null(internal::CustomerGeneratorColumn::kCurrentAddrSk)) {
      TPCDS_RETURN_NOT_OK(c_current_addr_sk.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(c_current_addr_sk.Append(row.current_addr_sk));
    }

    if (is_null(internal::CustomerGeneratorColumn::kFirstShiptoDateId)) {
      TPCDS_RETURN_NOT_OK(c_first_shipto_date_sk.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(
          c_first_shipto_date_sk.Append(row.first_shipto_date_sk));
    }

    if (is_null(internal::CustomerGeneratorColumn::kFirstSalesDateId)) {
      TPCDS_RETURN_NOT_OK(c_first_sales_date_sk.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(
          c_first_sales_date_sk.Append(row.first_sales_date_sk));
    }

    if (is_null(internal::CustomerGeneratorColumn::kSalutation)) {
      TPCDS_RETURN_NOT_OK(c_salutation.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(c_salutation.Append(row.salutation));
    }

    if (is_null(internal::CustomerGeneratorColumn::kFirstName)) {
      TPCDS_RETURN_NOT_OK(c_first_name.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(c_first_name.Append(row.first_name));
    }

    if (is_null(internal::CustomerGeneratorColumn::kLastName)) {
      TPCDS_RETURN_NOT_OK(c_last_name.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(c_last_name.Append(row.last_name));
    }

    if (is_null(internal::CustomerGeneratorColumn::kPreferredCustFlag)) {
      TPCDS_RETURN_NOT_OK(c_preferred_cust_flag.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(
          c_preferred_cust_flag.Append(row.preferred_cust_flag));
    }

    if (is_null(internal::CustomerGeneratorColumn::kBirthDay)) {
      TPCDS_RETURN_NOT_OK(c_birth_day.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(c_birth_day.Append(row.birth_day));
    }

    if (is_null(internal::CustomerGeneratorColumn::kBirthMonth)) {
      TPCDS_RETURN_NOT_OK(c_birth_month.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(c_birth_month.Append(row.birth_month));
    }

    if (is_null(internal::CustomerGeneratorColumn::kBirthYear)) {
      TPCDS_RETURN_NOT_OK(c_birth_year.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(c_birth_year.Append(row.birth_year));
    }

    if (is_null(internal::CustomerGeneratorColumn::kBirthCountry)) {
      TPCDS_RETURN_NOT_OK(c_birth_country.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(c_birth_country.Append(row.birth_country));
    }

    // c_login is never set by the reference generator.
    TPCDS_RETURN_NOT_OK(c_login.AppendNull());

    if (is_null(internal::CustomerGeneratorColumn::kEmailAddress)) {
      TPCDS_RETURN_NOT_OK(c_email_address.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(c_email_address.Append(row.email_address));
    }

    if (is_null(internal::CustomerGeneratorColumn::kLastReviewDate)) {
      TPCDS_RETURN_NOT_OK(c_last_review_date_sk.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(
          c_last_review_date_sk.Append(row.last_review_date_sk));
    }

    impl_->row_generator_.ConsumeRemainingSeedsForRow();
    ++impl_->current_row_;
    --impl_->remaining_rows_;
  }

  std::vector<std::shared_ptr<arrow::Array>> arrays;
  arrays.reserve(18);
  std::shared_ptr<arrow::Array> array;

  TPCDS_RETURN_NOT_OK(c_customer_sk.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(c_customer_id.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(c_current_cdemo_sk.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(c_current_hdemo_sk.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(c_current_addr_sk.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(c_first_shipto_date_sk.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(c_first_sales_date_sk.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(c_salutation.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(c_first_name.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(c_last_name.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(c_preferred_cust_flag.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(c_birth_day.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(c_birth_month.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(c_birth_year.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(c_birth_country.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(c_login.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(c_email_address.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(c_last_review_date_sk.Finish(&array));
  arrays.push_back(array);

  return impl_->column_selection_.MakeRecordBatch(batch_rows, std::move(arrays),
                                                  out);
}

int64_t CustomerGenerator::total_rows() const { return impl_->total_rows_; }

int64_t CustomerGenerator::remaining_rows() const {
  return impl_->remaining_rows_;
}

int64_t CustomerGenerator::TotalRows(double scale_factor) {
  return internal::Scaling(scale_factor).RowCount(TableId::kCustomer);
}

}  // namespace benchgen::tpcds
