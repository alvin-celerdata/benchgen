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

#include "generators/item_generator.h"

#include <algorithm>
#include <stdexcept>

#include "distribution/scaling.h"
#include "generators/item_row_generator.h"
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

std::shared_ptr<arrow::Schema> BuildItemSchema() {
  return arrow::schema({
      arrow::field("i_item_sk", arrow::int64(), false),
      arrow::field("i_item_id", arrow::utf8(), false),
      arrow::field("i_rec_start_date", arrow::date32()),
      arrow::field("i_rec_end_date", arrow::date32()),
      arrow::field("i_item_desc", arrow::utf8()),
      arrow::field("i_current_price", arrow::smallest_decimal(7, 2)),
      arrow::field("i_wholesale_cost", arrow::smallest_decimal(7, 2)),
      arrow::field("i_brand_id", arrow::int64()),
      arrow::field("i_brand", arrow::utf8()),
      arrow::field("i_class_id", arrow::int64()),
      arrow::field("i_class", arrow::utf8()),
      arrow::field("i_category_id", arrow::int64()),
      arrow::field("i_category", arrow::utf8()),
      arrow::field("i_manufact_id", arrow::int64()),
      arrow::field("i_manufact", arrow::utf8()),
      arrow::field("i_size", arrow::utf8()),
      arrow::field("i_formulation", arrow::utf8()),
      arrow::field("i_color", arrow::utf8()),
      arrow::field("i_units", arrow::utf8()),
      arrow::field("i_container", arrow::utf8()),
      arrow::field("i_manager_id", arrow::int64()),
      arrow::field("i_product_name", arrow::utf8()),
  });
}

}  // namespace

struct ItemGenerator::Impl {
  explicit Impl(GeneratorOptions options)
      : options_(std::move(options)),
        schema_(BuildItemSchema()),
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
            .RowCountByTableNumber(ITEM);
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
  internal::ItemRowGenerator row_generator_;
};

ItemGenerator::ItemGenerator(GeneratorOptions options)
    : impl_(std::make_unique<Impl>(std::move(options))) {}

ItemGenerator::~ItemGenerator() = default;

std::shared_ptr<arrow::Schema> ItemGenerator::schema() const {
  return impl_->schema_;
}

std::string_view ItemGenerator::name() const {
  return TableIdToString(TableId::kItem);
}

std::string_view ItemGenerator::suite_name() const { return "tpcds"; }

arrow::Status ItemGenerator::Next(std::shared_ptr<arrow::RecordBatch>* out) {
  if (impl_->remaining_rows_ == 0) {
    *out = nullptr;
    return arrow::Status::OK();
  }

  const int64_t batch_rows =
      std::min(impl_->remaining_rows_, impl_->options_.chunk_size);

  arrow::MemoryPool* pool = arrow::default_memory_pool();
  arrow::Int64Builder i_item_sk(pool);
  arrow::StringBuilder i_item_id(pool);
  arrow::Date32Builder i_rec_start_date_id(pool);
  arrow::Date32Builder i_rec_end_date_id(pool);
  arrow::StringBuilder i_item_desc(pool);
  arrow::Decimal32Builder i_current_price(arrow::smallest_decimal(7, 2), pool);
  arrow::Decimal32Builder i_wholesale_cost(arrow::smallest_decimal(7, 2), pool);
  arrow::Int64Builder i_brand_id(pool);
  arrow::StringBuilder i_brand(pool);
  arrow::Int64Builder i_class_id(pool);
  arrow::StringBuilder i_class(pool);
  arrow::Int64Builder i_category_id(pool);
  arrow::StringBuilder i_category(pool);
  arrow::Int64Builder i_manufact_id(pool);
  arrow::StringBuilder i_manufact(pool);
  arrow::StringBuilder i_size(pool);
  arrow::StringBuilder i_formulation(pool);
  arrow::StringBuilder i_color(pool);
  arrow::StringBuilder i_units(pool);
  arrow::StringBuilder i_container(pool);
  arrow::Int64Builder i_manager_id(pool);
  arrow::StringBuilder i_product_name(pool);

#define TPCDS_RETURN_NOT_OK(status)   \
  do {                                \
    arrow::Status _status = (status); \
    if (!_status.ok()) {              \
      return _status;                 \
    }                                 \
  } while (false)

  TPCDS_RETURN_NOT_OK(i_item_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(i_item_id.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(i_rec_start_date_id.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(i_rec_end_date_id.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(i_item_desc.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(i_current_price.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(i_wholesale_cost.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(i_brand_id.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(i_brand.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(i_class_id.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(i_class.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(i_category_id.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(i_category.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(i_manufact_id.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(i_manufact.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(i_size.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(i_formulation.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(i_color.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(i_units.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(i_container.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(i_manager_id.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(i_product_name.Reserve(batch_rows));

  for (int64_t i = 0; i < batch_rows; ++i) {
    int64_t row_number = impl_->current_row_ + 1;
    internal::ItemRowData row = impl_->row_generator_.GenerateRow(row_number);

    auto is_null = [&](int column_id) {
      return internal::IsNull(row.null_bitmap, ITEM, column_id);
    };

    if (is_null(I_ITEM_SK)) {
      TPCDS_RETURN_NOT_OK(i_item_sk.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(i_item_sk.Append(row.item_sk));
    }

    if (is_null(I_ITEM_ID)) {
      TPCDS_RETURN_NOT_OK(i_item_id.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(i_item_id.Append(row.item_id));
    }

    if (is_null(I_REC_START_DATE_ID) || row.rec_start_date_id <= 0) {
      TPCDS_RETURN_NOT_OK(i_rec_start_date_id.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(
          i_rec_start_date_id.Append(Date32FromJulian(row.rec_start_date_id)));
    }

    if (is_null(I_REC_END_DATE_ID) || row.rec_end_date_id <= 0) {
      TPCDS_RETURN_NOT_OK(i_rec_end_date_id.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(
          i_rec_end_date_id.Append(Date32FromJulian(row.rec_end_date_id)));
    }

    if (is_null(I_ITEM_DESC)) {
      TPCDS_RETURN_NOT_OK(i_item_desc.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(i_item_desc.Append(row.item_desc));
    }

    if (is_null(I_CURRENT_PRICE)) {
      TPCDS_RETURN_NOT_OK(i_current_price.AppendNull());
    } else {
      arrow::Decimal32 price_val(row.current_price.number);
      TPCDS_RETURN_NOT_OK(i_current_price.Append(price_val));
    }

    if (is_null(I_WHOLESALE_COST)) {
      TPCDS_RETURN_NOT_OK(i_wholesale_cost.AppendNull());
    } else {
      arrow::Decimal32 cost_val(row.wholesale_cost.number);
      TPCDS_RETURN_NOT_OK(i_wholesale_cost.Append(cost_val));
    }

    if (is_null(I_BRAND_ID)) {
      TPCDS_RETURN_NOT_OK(i_brand_id.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(i_brand_id.Append(row.brand_id));
    }

    if (is_null(I_BRAND)) {
      TPCDS_RETURN_NOT_OK(i_brand.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(i_brand.Append(row.brand));
    }

    if (is_null(I_CLASS_ID)) {
      TPCDS_RETURN_NOT_OK(i_class_id.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(i_class_id.Append(row.class_id));
    }

    if (is_null(I_CLASS)) {
      TPCDS_RETURN_NOT_OK(i_class.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(i_class.Append(row.class_name));
    }

    if (is_null(I_CATEGORY_ID)) {
      TPCDS_RETURN_NOT_OK(i_category_id.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(i_category_id.Append(row.category_id));
    }

    if (is_null(I_CATEGORY)) {
      TPCDS_RETURN_NOT_OK(i_category.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(i_category.Append(row.category));
    }

    if (is_null(I_MANUFACT_ID)) {
      TPCDS_RETURN_NOT_OK(i_manufact_id.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(i_manufact_id.Append(row.manufact_id));
    }

    if (is_null(I_MANUFACT)) {
      TPCDS_RETURN_NOT_OK(i_manufact.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(i_manufact.Append(row.manufact));
    }

    if (is_null(I_SIZE)) {
      TPCDS_RETURN_NOT_OK(i_size.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(i_size.Append(row.size));
    }

    if (is_null(I_FORMULATION)) {
      TPCDS_RETURN_NOT_OK(i_formulation.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(i_formulation.Append(row.formulation));
    }

    if (is_null(I_COLOR)) {
      TPCDS_RETURN_NOT_OK(i_color.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(i_color.Append(row.color));
    }

    if (is_null(I_UNITS)) {
      TPCDS_RETURN_NOT_OK(i_units.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(i_units.Append(row.units));
    }

    if (is_null(I_CONTAINER)) {
      TPCDS_RETURN_NOT_OK(i_container.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(i_container.Append(row.container));
    }

    if (is_null(I_MANAGER_ID)) {
      TPCDS_RETURN_NOT_OK(i_manager_id.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(i_manager_id.Append(row.manager_id));
    }

    if (is_null(I_PRODUCT_NAME)) {
      TPCDS_RETURN_NOT_OK(i_product_name.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(i_product_name.Append(row.product_name));
    }

    impl_->row_generator_.ConsumeRemainingSeedsForRow();
    ++impl_->current_row_;
    --impl_->remaining_rows_;
  }

  std::vector<std::shared_ptr<arrow::Array>> arrays;
  arrays.reserve(22);
  std::shared_ptr<arrow::Array> array;

  TPCDS_RETURN_NOT_OK(i_item_sk.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(i_item_id.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(i_rec_start_date_id.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(i_rec_end_date_id.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(i_item_desc.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(i_current_price.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(i_wholesale_cost.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(i_brand_id.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(i_brand.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(i_class_id.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(i_class.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(i_category_id.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(i_category.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(i_manufact_id.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(i_manufact.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(i_size.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(i_formulation.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(i_color.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(i_units.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(i_container.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(i_manager_id.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(i_product_name.Finish(&array));
  arrays.push_back(array);

  return impl_->column_selection_.MakeRecordBatch(batch_rows, std::move(arrays),
                                                  out);
}

int64_t ItemGenerator::total_rows() const { return impl_->total_rows_; }

int64_t ItemGenerator::remaining_rows() const { return impl_->remaining_rows_; }

int64_t ItemGenerator::TotalRows(double scale_factor) {
  return internal::Scaling(scale_factor).RowCountByTableNumber(ITEM);
}

}  // namespace benchgen::tpcds
