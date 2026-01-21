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

#include "generators/customer_demographics_generator.h"

#include <algorithm>

#include "benchgen/table.h"
#include "distribution/scaling.h"
#include "generators/customer_demographics_row_generator.h"
#include "util/column_selection.h"

namespace benchgen::tpcds {
namespace {

#define TPCDS_RETURN_NOT_OK(status)     \
  do {                                  \
    ::arrow::Status _status = (status); \
    if (!_status.ok()) {                \
      return _status;                   \
    }                                   \
  } while (false)

std::shared_ptr<arrow::Schema> BuildCustomerDemographicsSchema() {
  return arrow::schema({
      arrow::field("cd_demo_sk", arrow::int64(), false),
      arrow::field("cd_gender", arrow::utf8()),
      arrow::field("cd_marital_status", arrow::utf8()),
      arrow::field("cd_education_status", arrow::utf8()),
      arrow::field("cd_purchase_estimate", arrow::int32()),
      arrow::field("cd_credit_rating", arrow::utf8()),
      arrow::field("cd_dep_count", arrow::int32()),
      arrow::field("cd_dep_employed_count", arrow::int32()),
      arrow::field("cd_dep_college_count", arrow::int32()),
  });
}

}  // namespace

struct CustomerDemographicsGenerator::Impl {
  explicit Impl(GeneratorOptions options)
      : options_(std::move(options)),
        schema_(BuildCustomerDemographicsSchema()),
        row_generator_() {}

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
            .RowCount(TableId::kCustomerDemographics);
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
    return arrow::Status::OK();
  }

  GeneratorOptions options_;
  int64_t total_rows_ = 0;
  int64_t remaining_rows_ = 0;
  int64_t current_row_ = 0;
  std::shared_ptr<arrow::Schema> schema_;
  ::benchgen::internal::ColumnSelection column_selection_;
  internal::CustomerDemographicsRowGenerator row_generator_;
};

CustomerDemographicsGenerator::CustomerDemographicsGenerator(
    GeneratorOptions options)
    : impl_(std::make_unique<Impl>(std::move(options))) {}

arrow::Status CustomerDemographicsGenerator::Init() { return impl_->Init(); }

CustomerDemographicsGenerator::~CustomerDemographicsGenerator() = default;

std::shared_ptr<arrow::Schema> CustomerDemographicsGenerator::schema() const {
  return impl_->schema_;
}

std::string_view CustomerDemographicsGenerator::name() const {
  return TableIdToString(TableId::kCustomerDemographics);
}

std::string_view CustomerDemographicsGenerator::suite_name() const {
  return "tpcds";
}

arrow::Status CustomerDemographicsGenerator::Next(
    std::shared_ptr<arrow::RecordBatch>* out) {
  if (impl_->remaining_rows_ == 0) {
    *out = nullptr;
    return arrow::Status::OK();
  }

  const int64_t batch_rows =
      std::min(impl_->remaining_rows_, impl_->options_.chunk_size);

  arrow::MemoryPool* pool = arrow::default_memory_pool();
  arrow::Int64Builder cd_demo_sk(pool);
  arrow::StringBuilder cd_gender(pool);
  arrow::StringBuilder cd_marital_status(pool);
  arrow::StringBuilder cd_education_status(pool);
  arrow::Int32Builder cd_purchase_estimate(pool);
  arrow::StringBuilder cd_credit_rating(pool);
  arrow::Int32Builder cd_dep_count(pool);
  arrow::Int32Builder cd_dep_employed_count(pool);
  arrow::Int32Builder cd_dep_college_count(pool);

  TPCDS_RETURN_NOT_OK(cd_demo_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cd_gender.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cd_marital_status.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cd_education_status.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cd_purchase_estimate.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cd_credit_rating.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cd_dep_count.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cd_dep_employed_count.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cd_dep_college_count.Reserve(batch_rows));

  for (int64_t i = 0; i < batch_rows; ++i) {
    int64_t row_number = impl_->current_row_ + 1;
    internal::CustomerDemographicsRowData row =
        impl_->row_generator_.GenerateRow(row_number);

    TPCDS_RETURN_NOT_OK(cd_demo_sk.Append(row.demo_sk));
    TPCDS_RETURN_NOT_OK(cd_gender.Append(row.gender));
    TPCDS_RETURN_NOT_OK(cd_marital_status.Append(row.marital_status));
    TPCDS_RETURN_NOT_OK(cd_education_status.Append(row.education_status));
    TPCDS_RETURN_NOT_OK(cd_purchase_estimate.Append(row.purchase_estimate));
    TPCDS_RETURN_NOT_OK(cd_credit_rating.Append(row.credit_rating));
    TPCDS_RETURN_NOT_OK(cd_dep_count.Append(row.dep_count));
    TPCDS_RETURN_NOT_OK(cd_dep_employed_count.Append(row.dep_employed_count));
    TPCDS_RETURN_NOT_OK(cd_dep_college_count.Append(row.dep_college_count));

    ++impl_->current_row_;
    --impl_->remaining_rows_;
  }

  std::shared_ptr<arrow::Array> cd_demo_sk_array;
  std::shared_ptr<arrow::Array> cd_gender_array;
  std::shared_ptr<arrow::Array> cd_marital_status_array;
  std::shared_ptr<arrow::Array> cd_education_status_array;
  std::shared_ptr<arrow::Array> cd_purchase_estimate_array;
  std::shared_ptr<arrow::Array> cd_credit_rating_array;
  std::shared_ptr<arrow::Array> cd_dep_count_array;
  std::shared_ptr<arrow::Array> cd_dep_employed_count_array;
  std::shared_ptr<arrow::Array> cd_dep_college_count_array;

  TPCDS_RETURN_NOT_OK(cd_demo_sk.Finish(&cd_demo_sk_array));
  TPCDS_RETURN_NOT_OK(cd_gender.Finish(&cd_gender_array));
  TPCDS_RETURN_NOT_OK(cd_marital_status.Finish(&cd_marital_status_array));
  TPCDS_RETURN_NOT_OK(cd_education_status.Finish(&cd_education_status_array));
  TPCDS_RETURN_NOT_OK(cd_purchase_estimate.Finish(&cd_purchase_estimate_array));
  TPCDS_RETURN_NOT_OK(cd_credit_rating.Finish(&cd_credit_rating_array));
  TPCDS_RETURN_NOT_OK(cd_dep_count.Finish(&cd_dep_count_array));
  TPCDS_RETURN_NOT_OK(
      cd_dep_employed_count.Finish(&cd_dep_employed_count_array));
  TPCDS_RETURN_NOT_OK(cd_dep_college_count.Finish(&cd_dep_college_count_array));

  std::vector<std::shared_ptr<arrow::Array>> arrays = {
      cd_demo_sk_array,           cd_gender_array,
      cd_marital_status_array,    cd_education_status_array,
      cd_purchase_estimate_array, cd_credit_rating_array,
      cd_dep_count_array,         cd_dep_employed_count_array,
      cd_dep_college_count_array};

  return impl_->column_selection_.MakeRecordBatch(batch_rows, std::move(arrays),
                                                  out);
}

int64_t CustomerDemographicsGenerator::total_rows() const {
  return impl_->total_rows_;
}

int64_t CustomerDemographicsGenerator::remaining_rows() const {
  return impl_->remaining_rows_;
}

int64_t CustomerDemographicsGenerator::TotalRows(double scale_factor) {
  return internal::Scaling(scale_factor)
      .RowCount(TableId::kCustomerDemographics);
}

}  // namespace benchgen::tpcds
