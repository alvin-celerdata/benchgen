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

#include "generators/household_demographics_generator.h"

#include <algorithm>

#include "benchgen/table.h"
#include "distribution/scaling.h"
#include "generators/household_demographics_row_generator.h"
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

std::shared_ptr<arrow::Schema> BuildHouseholdDemographicsSchema() {
  return arrow::schema({
      arrow::field("hd_demo_sk", arrow::int64(), false),
      arrow::field("hd_income_band_sk", arrow::int64()),
      arrow::field("hd_buy_potential", arrow::utf8()),
      arrow::field("hd_dep_count", arrow::int32()),
      arrow::field("hd_vehicle_count", arrow::int32()),
  });
}

}  // namespace

struct HouseholdDemographicsGenerator::Impl {
  explicit Impl(GeneratorOptions options)
      : options_(std::move(options)),
        schema_(BuildHouseholdDemographicsSchema()),
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
            .RowCount(TableId::kHouseholdDemographics);
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
  internal::HouseholdDemographicsRowGenerator row_generator_;
};

HouseholdDemographicsGenerator::HouseholdDemographicsGenerator(
    GeneratorOptions options)
    : impl_(std::make_unique<Impl>(std::move(options))) {}

arrow::Status HouseholdDemographicsGenerator::Init() { return impl_->Init(); }

HouseholdDemographicsGenerator::~HouseholdDemographicsGenerator() = default;

std::shared_ptr<arrow::Schema> HouseholdDemographicsGenerator::schema() const {
  return impl_->schema_;
}

std::string_view HouseholdDemographicsGenerator::name() const {
  return TableIdToString(TableId::kHouseholdDemographics);
}

std::string_view HouseholdDemographicsGenerator::suite_name() const {
  return "tpcds";
}

arrow::Status HouseholdDemographicsGenerator::Next(
    std::shared_ptr<arrow::RecordBatch>* out) {
  if (impl_->remaining_rows_ == 0) {
    *out = nullptr;
    return arrow::Status::OK();
  }

  const int64_t batch_rows =
      std::min(impl_->remaining_rows_, impl_->options_.chunk_size);

  arrow::MemoryPool* pool = arrow::default_memory_pool();
  arrow::Int64Builder hd_demo_sk(pool);
  arrow::Int64Builder hd_income_band_sk(pool);
  arrow::StringBuilder hd_buy_potential(pool);
  arrow::Int32Builder hd_dep_count(pool);
  arrow::Int32Builder hd_vehicle_count(pool);

  TPCDS_RETURN_NOT_OK(hd_demo_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(hd_income_band_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(hd_buy_potential.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(hd_dep_count.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(hd_vehicle_count.Reserve(batch_rows));

  for (int64_t i = 0; i < batch_rows; ++i) {
    int64_t row_number = impl_->current_row_ + 1;
    internal::HouseholdDemographicsRowData row =
        impl_->row_generator_.GenerateRow(row_number);

    TPCDS_RETURN_NOT_OK(hd_demo_sk.Append(row.demo_sk));
    TPCDS_RETURN_NOT_OK(hd_income_band_sk.Append(row.income_band_sk));
    TPCDS_RETURN_NOT_OK(hd_buy_potential.Append(row.buy_potential));
    TPCDS_RETURN_NOT_OK(hd_dep_count.Append(row.dep_count));
    TPCDS_RETURN_NOT_OK(hd_vehicle_count.Append(row.vehicle_count));

    ++impl_->current_row_;
    --impl_->remaining_rows_;
  }

  std::shared_ptr<arrow::Array> hd_demo_sk_array;
  std::shared_ptr<arrow::Array> hd_income_band_sk_array;
  std::shared_ptr<arrow::Array> hd_buy_potential_array;
  std::shared_ptr<arrow::Array> hd_dep_count_array;
  std::shared_ptr<arrow::Array> hd_vehicle_count_array;

  TPCDS_RETURN_NOT_OK(hd_demo_sk.Finish(&hd_demo_sk_array));
  TPCDS_RETURN_NOT_OK(hd_income_band_sk.Finish(&hd_income_band_sk_array));
  TPCDS_RETURN_NOT_OK(hd_buy_potential.Finish(&hd_buy_potential_array));
  TPCDS_RETURN_NOT_OK(hd_dep_count.Finish(&hd_dep_count_array));
  TPCDS_RETURN_NOT_OK(hd_vehicle_count.Finish(&hd_vehicle_count_array));

  std::vector<std::shared_ptr<arrow::Array>> arrays = {
      hd_demo_sk_array, hd_income_band_sk_array, hd_buy_potential_array,
      hd_dep_count_array, hd_vehicle_count_array};

  return impl_->column_selection_.MakeRecordBatch(batch_rows, std::move(arrays),
                                                  out);
}

int64_t HouseholdDemographicsGenerator::total_rows() const {
  return impl_->total_rows_;
}

int64_t HouseholdDemographicsGenerator::remaining_rows() const {
  return impl_->remaining_rows_;
}

int64_t HouseholdDemographicsGenerator::TotalRows(double scale_factor) {
  return internal::Scaling(scale_factor)
      .RowCount(TableId::kHouseholdDemographics);
}

}  // namespace benchgen::tpcds
