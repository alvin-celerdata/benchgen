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

#include "generators/inventory_generator.h"

#include <algorithm>
#include <stdexcept>

#include "distribution/scaling.h"
#include "generators/inventory_row_generator.h"
#include "util/column_selection.h"
#include "utils/columns.h"
#include "utils/null_utils.h"
#include "utils/tables.h"

namespace benchgen::tpcds {
namespace {

std::shared_ptr<arrow::Schema> BuildInventorySchema() {
  return arrow::schema({
      arrow::field("inv_date_sk", arrow::int32(), false),
      arrow::field("inv_item_sk", arrow::int64(), false),
      arrow::field("inv_warehouse_sk", arrow::int64(), false),
      arrow::field("inv_quantity_on_hand", arrow::int32()),
  });
}

}  // namespace

struct InventoryGenerator::Impl {
  explicit Impl(GeneratorOptions options)
      : options_(std::move(options)),
        schema_(BuildInventorySchema()),
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
            .RowCountByTableNumber(INVENTORY);
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
  internal::InventoryRowGenerator row_generator_;
};

InventoryGenerator::InventoryGenerator(GeneratorOptions options)
    : impl_(std::make_unique<Impl>(std::move(options))) {}

InventoryGenerator::~InventoryGenerator() = default;

std::shared_ptr<arrow::Schema> InventoryGenerator::schema() const {
  return impl_->schema_;
}

std::string_view InventoryGenerator::name() const {
  return TableIdToString(TableId::kInventory);
}

std::string_view InventoryGenerator::suite_name() const { return "tpcds"; }

arrow::Status InventoryGenerator::Next(
    std::shared_ptr<arrow::RecordBatch>* out) {
  if (impl_->remaining_rows_ == 0) {
    *out = nullptr;
    return arrow::Status::OK();
  }

  const int64_t batch_rows =
      std::min(impl_->remaining_rows_, impl_->options_.chunk_size);

  arrow::MemoryPool* pool = arrow::default_memory_pool();
  arrow::Int32Builder inv_date_sk(pool);
  arrow::Int64Builder inv_item_sk(pool);
  arrow::Int64Builder inv_warehouse_sk(pool);
  arrow::Int32Builder inv_quantity_on_hand(pool);

#define TPCDS_RETURN_NOT_OK(status)   \
  do {                                \
    arrow::Status _status = (status); \
    if (!_status.ok()) {              \
      return _status;                 \
    }                                 \
  } while (false)

  TPCDS_RETURN_NOT_OK(inv_date_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(inv_item_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(inv_warehouse_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(inv_quantity_on_hand.Reserve(batch_rows));

  for (int64_t i = 0; i < batch_rows; ++i) {
    int64_t row_number = impl_->current_row_ + 1;
    internal::InventoryRowData row =
        impl_->row_generator_.GenerateRow(row_number);

    auto is_null = [&](int column_id) {
      return internal::IsNull(row.null_bitmap, INVENTORY, column_id);
    };

    if (is_null(INV_DATE_SK)) {
      TPCDS_RETURN_NOT_OK(inv_date_sk.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(inv_date_sk.Append(row.date_sk));
    }

    if (is_null(INV_ITEM_SK)) {
      TPCDS_RETURN_NOT_OK(inv_item_sk.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(inv_item_sk.Append(row.item_sk));
    }

    if (is_null(INV_WAREHOUSE_SK)) {
      TPCDS_RETURN_NOT_OK(inv_warehouse_sk.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(inv_warehouse_sk.Append(row.warehouse_sk));
    }

    if (is_null(INV_QUANTITY_ON_HAND)) {
      TPCDS_RETURN_NOT_OK(inv_quantity_on_hand.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(inv_quantity_on_hand.Append(row.quantity_on_hand));
    }

    impl_->row_generator_.ConsumeRemainingSeedsForRow();
    ++impl_->current_row_;
    --impl_->remaining_rows_;
  }

  std::vector<std::shared_ptr<arrow::Array>> arrays;
  arrays.reserve(4);
  std::shared_ptr<arrow::Array> array;

  TPCDS_RETURN_NOT_OK(inv_date_sk.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(inv_item_sk.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(inv_warehouse_sk.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(inv_quantity_on_hand.Finish(&array));
  arrays.push_back(array);

  return impl_->column_selection_.MakeRecordBatch(batch_rows, std::move(arrays),
                                                  out);
}

int64_t InventoryGenerator::total_rows() const { return impl_->total_rows_; }

int64_t InventoryGenerator::remaining_rows() const {
  return impl_->remaining_rows_;
}

int64_t InventoryGenerator::TotalRows(double scale_factor) {
  return internal::Scaling(scale_factor).RowCountByTableNumber(INVENTORY);
}

}  // namespace benchgen::tpcds
