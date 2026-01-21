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

#include "generators/catalog_page_generator.h"

#include <algorithm>
#include <stdexcept>

#include "distribution/scaling.h"
#include "generators/catalog_page_row_generator.h"
#include "util/column_selection.h"
#include "utils/columns.h"
#include "utils/null_utils.h"
#include "utils/tables.h"

namespace benchgen::tpcds {
namespace {

std::shared_ptr<arrow::Schema> BuildCatalogPageSchema() {
  return arrow::schema({
      arrow::field("cp_catalog_page_sk", arrow::int64(), false),
      arrow::field("cp_catalog_page_id", arrow::utf8(), false),
      arrow::field("cp_start_date_sk", arrow::int32()),
      arrow::field("cp_end_date_sk", arrow::int32()),
      arrow::field("cp_department", arrow::utf8()),
      arrow::field("cp_catalog_number", arrow::int32()),
      arrow::field("cp_catalog_page_number", arrow::int32()),
      arrow::field("cp_description", arrow::utf8()),
      arrow::field("cp_type", arrow::utf8()),
  });
}

}  // namespace

struct CatalogPageGenerator::Impl {
  explicit Impl(GeneratorOptions options)
      : options_(std::move(options)),
        schema_(BuildCatalogPageSchema()),
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
            .RowCountByTableNumber(CATALOG_PAGE);
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
  internal::CatalogPageRowGenerator row_generator_;
};

CatalogPageGenerator::CatalogPageGenerator(GeneratorOptions options)
    : impl_(std::make_unique<Impl>(std::move(options))) {}

CatalogPageGenerator::~CatalogPageGenerator() = default;

std::shared_ptr<arrow::Schema> CatalogPageGenerator::schema() const {
  return impl_->schema_;
}

std::string_view CatalogPageGenerator::name() const {
  return TableIdToString(TableId::kCatalogPage);
}

std::string_view CatalogPageGenerator::suite_name() const { return "tpcds"; }

arrow::Status CatalogPageGenerator::Next(
    std::shared_ptr<arrow::RecordBatch>* out) {
  if (impl_->remaining_rows_ == 0) {
    *out = nullptr;
    return arrow::Status::OK();
  }

  const int64_t batch_rows =
      std::min(impl_->remaining_rows_, impl_->options_.chunk_size);

  arrow::MemoryPool* pool = arrow::default_memory_pool();
  arrow::Int64Builder cp_catalog_page_sk(pool);
  arrow::StringBuilder cp_catalog_page_id(pool);
  arrow::Int32Builder cp_start_date_id(pool);
  arrow::Int32Builder cp_end_date_id(pool);
  arrow::StringBuilder cp_department(pool);
  arrow::Int32Builder cp_catalog_number(pool);
  arrow::Int32Builder cp_catalog_page_number(pool);
  arrow::StringBuilder cp_description(pool);
  arrow::StringBuilder cp_type(pool);

#define TPCDS_RETURN_NOT_OK(status)   \
  do {                                \
    arrow::Status _status = (status); \
    if (!_status.ok()) {              \
      return _status;                 \
    }                                 \
  } while (false)

  TPCDS_RETURN_NOT_OK(cp_catalog_page_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cp_catalog_page_id.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cp_start_date_id.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cp_end_date_id.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cp_department.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cp_catalog_number.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cp_catalog_page_number.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cp_description.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cp_type.Reserve(batch_rows));

  for (int64_t i = 0; i < batch_rows; ++i) {
    int64_t row_number = impl_->current_row_ + 1;
    internal::CatalogPageRowData row =
        impl_->row_generator_.GenerateRow(row_number);

    auto is_null = [&](int column_id) {
      return internal::IsNull(row.null_bitmap, CATALOG_PAGE, column_id);
    };

    if (is_null(CP_CATALOG_PAGE_SK)) {
      TPCDS_RETURN_NOT_OK(cp_catalog_page_sk.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(cp_catalog_page_sk.Append(row.catalog_page_sk));
    }

    if (is_null(CP_CATALOG_PAGE_ID)) {
      TPCDS_RETURN_NOT_OK(cp_catalog_page_id.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(cp_catalog_page_id.Append(row.catalog_page_id));
    }

    if (is_null(CP_START_DATE_ID)) {
      TPCDS_RETURN_NOT_OK(cp_start_date_id.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(cp_start_date_id.Append(row.start_date_id));
    }

    if (is_null(CP_END_DATE_ID)) {
      TPCDS_RETURN_NOT_OK(cp_end_date_id.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(cp_end_date_id.Append(row.end_date_id));
    }

    if (is_null(CP_DEPARTMENT)) {
      TPCDS_RETURN_NOT_OK(cp_department.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(cp_department.Append(row.department));
    }

    if (is_null(CP_CATALOG_NUMBER)) {
      TPCDS_RETURN_NOT_OK(cp_catalog_number.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(cp_catalog_number.Append(row.catalog_number));
    }

    if (is_null(CP_CATALOG_PAGE_NUMBER)) {
      TPCDS_RETURN_NOT_OK(cp_catalog_page_number.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(
          cp_catalog_page_number.Append(row.catalog_page_number));
    }

    if (is_null(CP_DESCRIPTION)) {
      TPCDS_RETURN_NOT_OK(cp_description.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(cp_description.Append(row.description));
    }

    if (is_null(CP_TYPE)) {
      TPCDS_RETURN_NOT_OK(cp_type.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(cp_type.Append(row.type));
    }

    impl_->row_generator_.ConsumeRemainingSeedsForRow();
    ++impl_->current_row_;
    --impl_->remaining_rows_;
  }

  std::vector<std::shared_ptr<arrow::Array>> arrays;
  arrays.reserve(9);
  std::shared_ptr<arrow::Array> array;

  TPCDS_RETURN_NOT_OK(cp_catalog_page_sk.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cp_catalog_page_id.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cp_start_date_id.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cp_end_date_id.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cp_department.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cp_catalog_number.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cp_catalog_page_number.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cp_description.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cp_type.Finish(&array));
  arrays.push_back(array);

  return impl_->column_selection_.MakeRecordBatch(batch_rows, std::move(arrays),
                                                  out);
}

int64_t CatalogPageGenerator::total_rows() const { return impl_->total_rows_; }

int64_t CatalogPageGenerator::remaining_rows() const {
  return impl_->remaining_rows_;
}

int64_t CatalogPageGenerator::TotalRows(double scale_factor) {
  return internal::Scaling(scale_factor).RowCountByTableNumber(CATALOG_PAGE);
}

}  // namespace benchgen::tpcds
