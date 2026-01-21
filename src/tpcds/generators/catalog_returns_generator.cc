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

#include "generators/catalog_returns_generator.h"

#include <algorithm>
#include <stdexcept>
#include <string>

#include "distribution/scaling.h"
#include "generators/catalog_returns_row_generator.h"
#include "util/column_selection.h"
#include "utils/column_streams.h"
#include "utils/columns.h"
#include "utils/constants.h"
#include "utils/decimal.h"
#include "utils/null_utils.h"
#include "utils/random_number_stream.h"
#include "utils/random_utils.h"
#include "utils/tables.h"

namespace benchgen::tpcds {
namespace {

std::shared_ptr<arrow::Schema> BuildCatalogReturnsSchema() {
  return arrow::schema({
      arrow::field("cr_returned_date_sk", arrow::int32()),
      arrow::field("cr_returned_time_sk", arrow::int32()),
      arrow::field("cr_item_sk", arrow::int64()),
      arrow::field("cr_refunded_customer_sk", arrow::int64()),
      arrow::field("cr_refunded_cdemo_sk", arrow::int64()),
      arrow::field("cr_refunded_hdemo_sk", arrow::int64()),
      arrow::field("cr_refunded_addr_sk", arrow::int64()),
      arrow::field("cr_returning_customer_sk", arrow::int64()),
      arrow::field("cr_returning_cdemo_sk", arrow::int64()),
      arrow::field("cr_returning_hdemo_sk", arrow::int64()),
      arrow::field("cr_returning_addr_sk", arrow::int64()),
      arrow::field("cr_call_center_sk", arrow::int64()),
      arrow::field("cr_catalog_page_sk", arrow::int64()),
      arrow::field("cr_ship_mode_sk", arrow::int64()),
      arrow::field("cr_warehouse_sk", arrow::int64()),
      arrow::field("cr_reason_sk", arrow::int64()),
      arrow::field("cr_order_number", arrow::int64(), false),
      arrow::field("cr_return_quantity", arrow::int32()),
      arrow::field("cr_return_amount", arrow::smallest_decimal(7, 2)),
      arrow::field("cr_return_tax", arrow::smallest_decimal(7, 2)),
      arrow::field("cr_return_amt_inc_tax", arrow::smallest_decimal(7, 2)),
      arrow::field("cr_fee", arrow::smallest_decimal(7, 2)),
      arrow::field("cr_return_ship_cost", arrow::smallest_decimal(7, 2)),
      arrow::field("cr_refunded_cash", arrow::smallest_decimal(7, 2)),
      arrow::field("cr_reversed_charge", arrow::smallest_decimal(7, 2)),
      arrow::field("cr_store_credit", arrow::smallest_decimal(7, 2)),
      arrow::field("cr_net_loss", arrow::smallest_decimal(7, 2)),
  });
}

int64_t ComputeCatalogReturnsRows(double scale_factor) {
  internal::Scaling scaling(scale_factor);
  int64_t orders = scaling.RowCountByTableNumber(CATALOG_SALES);
  internal::RandomNumberStream order_stream(
      CS_ORDER_NUMBER, internal::SeedsPerRow(CS_ORDER_NUMBER));
  internal::RandomNumberStream return_stream(
      CR_IS_RETURNED, internal::SeedsPerRow(CR_IS_RETURNED));
  int64_t total = 0;
  for (int64_t i = 0; i < orders; ++i) {
    int line_items = internal::GenerateUniformRandomInt(4, 14, &order_stream);
    for (int item = 0; item < line_items; ++item) {
      if (internal::GenerateUniformRandomInt(0, 99, &return_stream) <
          CR_RETURN_PCT) {
        ++total;
      }
    }
    while (order_stream.seeds_used() < order_stream.seeds_per_row()) {
      internal::GenerateUniformRandomInt(1, 100, &order_stream);
    }
    order_stream.ResetSeedsUsed();
    while (return_stream.seeds_used() < return_stream.seeds_per_row()) {
      internal::GenerateUniformRandomInt(1, 100, &return_stream);
    }
    return_stream.ResetSeedsUsed();
  }
  return total;
}

}  // namespace

struct CatalogReturnsGenerator::Impl {
  explicit Impl(GeneratorOptions options)
      : options_(std::move(options)),
        schema_(BuildCatalogReturnsSchema()),
        row_generator_(options_.scale_factor) {
    if (options_.chunk_size <= 0) {
      throw std::invalid_argument("chunk_size must be positive");
    }
    auto status = column_selection_.Init(schema_, options_.column_names);
    if (!status.ok()) {
      throw std::invalid_argument(status.ToString());
    }
    schema_ = column_selection_.schema();
    total_rows_ = ComputeCatalogReturnsRows(options_.scale_factor);
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
  internal::CatalogReturnsRowGenerator row_generator_;
};

CatalogReturnsGenerator::CatalogReturnsGenerator(GeneratorOptions options)
    : impl_(std::make_unique<Impl>(std::move(options))) {}

CatalogReturnsGenerator::~CatalogReturnsGenerator() = default;

std::shared_ptr<arrow::Schema> CatalogReturnsGenerator::schema() const {
  return impl_->schema_;
}

std::string_view CatalogReturnsGenerator::name() const {
  return TableIdToString(TableId::kCatalogReturns);
}

std::string_view CatalogReturnsGenerator::suite_name() const { return "tpcds"; }

arrow::Status CatalogReturnsGenerator::Next(
    std::shared_ptr<arrow::RecordBatch>* out) {
  if (impl_->remaining_rows_ == 0) {
    *out = nullptr;
    return arrow::Status::OK();
  }

  const int64_t batch_rows =
      std::min(impl_->remaining_rows_, impl_->options_.chunk_size);

  arrow::MemoryPool* pool = arrow::default_memory_pool();
  arrow::Int32Builder cr_returned_date_sk(pool);
  arrow::Int32Builder cr_returned_time_sk(pool);
  arrow::Int64Builder cr_item_sk(pool);
  arrow::Int64Builder cr_refunded_customer_sk(pool);
  arrow::Int64Builder cr_refunded_cdemo_sk(pool);
  arrow::Int64Builder cr_refunded_hdemo_sk(pool);
  arrow::Int64Builder cr_refunded_addr_sk(pool);
  arrow::Int64Builder cr_returning_customer_sk(pool);
  arrow::Int64Builder cr_returning_cdemo_sk(pool);
  arrow::Int64Builder cr_returning_hdemo_sk(pool);
  arrow::Int64Builder cr_returning_addr_sk(pool);
  arrow::Int64Builder cr_call_center_sk(pool);
  arrow::Int64Builder cr_catalog_page_sk(pool);
  arrow::Int64Builder cr_ship_mode_sk(pool);
  arrow::Int64Builder cr_warehouse_sk(pool);
  arrow::Int64Builder cr_reason_sk(pool);
  arrow::Int64Builder cr_order_number(pool);
  arrow::Int32Builder cr_pricing_quantity(pool);
  arrow::Decimal32Builder cr_pricing_net_paid(arrow::smallest_decimal(7, 2),
                                              pool);
  arrow::Decimal32Builder cr_pricing_ext_tax(arrow::smallest_decimal(7, 2),
                                             pool);
  arrow::Decimal32Builder cr_pricing_net_paid_inc_tax(
      arrow::smallest_decimal(7, 2), pool);
  arrow::Decimal32Builder cr_pricing_fee(arrow::smallest_decimal(7, 2), pool);
  arrow::Decimal32Builder cr_pricing_ext_ship_cost(
      arrow::smallest_decimal(7, 2), pool);
  arrow::Decimal32Builder cr_pricing_refunded_cash(
      arrow::smallest_decimal(7, 2), pool);
  arrow::Decimal32Builder cr_pricing_reversed_charge(
      arrow::smallest_decimal(7, 2), pool);
  arrow::Decimal32Builder cr_pricing_store_credit(arrow::smallest_decimal(7, 2),
                                                  pool);
  arrow::Decimal32Builder cr_pricing_net_loss(arrow::smallest_decimal(7, 2),
                                              pool);

#define TPCDS_RETURN_NOT_OK(status)   \
  do {                                \
    arrow::Status _status = (status); \
    if (!_status.ok()) {              \
      return _status;                 \
    }                                 \
  } while (false)

  TPCDS_RETURN_NOT_OK(cr_returned_date_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cr_returned_time_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cr_item_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cr_refunded_customer_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cr_refunded_cdemo_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cr_refunded_hdemo_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cr_refunded_addr_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cr_returning_customer_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cr_returning_cdemo_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cr_returning_hdemo_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cr_returning_addr_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cr_call_center_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cr_catalog_page_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cr_ship_mode_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cr_warehouse_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cr_reason_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cr_order_number.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cr_pricing_quantity.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cr_pricing_net_paid.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cr_pricing_ext_tax.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cr_pricing_net_paid_inc_tax.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cr_pricing_fee.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cr_pricing_ext_ship_cost.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cr_pricing_refunded_cash.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cr_pricing_reversed_charge.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cr_pricing_store_credit.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cr_pricing_net_loss.Reserve(batch_rows));

  for (int64_t i = 0; i < batch_rows; ++i) {
    int64_t row_number = impl_->current_row_ + 1;
    internal::CatalogReturnsRowData row =
        impl_->row_generator_.GenerateRow(row_number);

    auto is_null = [&](int column_id) {
      return internal::IsNull(row.null_bitmap, CATALOG_RETURNS, column_id);
    };

    auto append_decimal = [&](arrow::Decimal32Builder& builder, int column_id,
                              const internal::Decimal& val) {
      if (is_null(column_id)) {
        return builder.AppendNull();
      }
      arrow::Decimal32 dec_val(val.number);
      return builder.Append(dec_val);
    };

    if (is_null(CR_RETURNED_DATE_SK)) {
      TPCDS_RETURN_NOT_OK(cr_returned_date_sk.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(cr_returned_date_sk.Append(row.returned_date_sk));
    }

    if (is_null(CR_RETURNED_TIME_SK)) {
      TPCDS_RETURN_NOT_OK(cr_returned_time_sk.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(cr_returned_time_sk.Append(row.returned_time_sk));
    }

    if (is_null(CR_ITEM_SK)) {
      TPCDS_RETURN_NOT_OK(cr_item_sk.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(cr_item_sk.Append(row.item_sk));
    }

    if (is_null(CR_REFUNDED_CUSTOMER_SK)) {
      TPCDS_RETURN_NOT_OK(cr_refunded_customer_sk.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(
          cr_refunded_customer_sk.Append(row.refunded_customer_sk));
    }

    if (is_null(CR_REFUNDED_CDEMO_SK)) {
      TPCDS_RETURN_NOT_OK(cr_refunded_cdemo_sk.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(cr_refunded_cdemo_sk.Append(row.refunded_cdemo_sk));
    }

    if (is_null(CR_REFUNDED_HDEMO_SK)) {
      TPCDS_RETURN_NOT_OK(cr_refunded_hdemo_sk.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(cr_refunded_hdemo_sk.Append(row.refunded_hdemo_sk));
    }

    if (is_null(CR_REFUNDED_ADDR_SK)) {
      TPCDS_RETURN_NOT_OK(cr_refunded_addr_sk.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(cr_refunded_addr_sk.Append(row.refunded_addr_sk));
    }

    if (is_null(CR_RETURNING_CUSTOMER_SK)) {
      TPCDS_RETURN_NOT_OK(cr_returning_customer_sk.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(
          cr_returning_customer_sk.Append(row.returning_customer_sk));
    }

    if (is_null(CR_RETURNING_CDEMO_SK)) {
      TPCDS_RETURN_NOT_OK(cr_returning_cdemo_sk.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(cr_returning_cdemo_sk.Append(row.returning_cdemo_sk));
    }

    if (is_null(CR_RETURNING_HDEMO_SK)) {
      TPCDS_RETURN_NOT_OK(cr_returning_hdemo_sk.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(cr_returning_hdemo_sk.Append(row.returning_hdemo_sk));
    }

    if (is_null(CR_RETURNING_ADDR_SK)) {
      TPCDS_RETURN_NOT_OK(cr_returning_addr_sk.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(cr_returning_addr_sk.Append(row.returning_addr_sk));
    }

    if (is_null(CR_CALL_CENTER_SK)) {
      TPCDS_RETURN_NOT_OK(cr_call_center_sk.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(cr_call_center_sk.Append(row.call_center_sk));
    }

    if (is_null(CR_CATALOG_PAGE_SK)) {
      TPCDS_RETURN_NOT_OK(cr_catalog_page_sk.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(cr_catalog_page_sk.Append(row.catalog_page_sk));
    }

    if (is_null(CR_SHIP_MODE_SK)) {
      TPCDS_RETURN_NOT_OK(cr_ship_mode_sk.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(cr_ship_mode_sk.Append(row.ship_mode_sk));
    }

    if (is_null(CR_WAREHOUSE_SK)) {
      TPCDS_RETURN_NOT_OK(cr_warehouse_sk.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(cr_warehouse_sk.Append(row.warehouse_sk));
    }

    if (is_null(CR_REASON_SK)) {
      TPCDS_RETURN_NOT_OK(cr_reason_sk.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(cr_reason_sk.Append(row.reason_sk));
    }

    TPCDS_RETURN_NOT_OK(cr_order_number.Append(row.order_number));

    if (is_null(CR_PRICING_QUANTITY)) {
      TPCDS_RETURN_NOT_OK(cr_pricing_quantity.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(cr_pricing_quantity.Append(row.pricing.quantity));
    }

    TPCDS_RETURN_NOT_OK(append_decimal(cr_pricing_net_paid, CR_PRICING_NET_PAID,
                                       row.pricing.net_paid));
    TPCDS_RETURN_NOT_OK(append_decimal(cr_pricing_ext_tax, CR_PRICING_EXT_TAX,
                                       row.pricing.ext_tax));
    TPCDS_RETURN_NOT_OK(append_decimal(cr_pricing_net_paid_inc_tax,
                                       CR_PRICING_NET_PAID_INC_TAX,
                                       row.pricing.net_paid_inc_tax));
    TPCDS_RETURN_NOT_OK(
        append_decimal(cr_pricing_fee, CR_PRICING_FEE, row.pricing.fee));
    TPCDS_RETURN_NOT_OK(append_decimal(cr_pricing_ext_ship_cost,
                                       CR_PRICING_EXT_SHIP_COST,
                                       row.pricing.ext_ship_cost));
    TPCDS_RETURN_NOT_OK(append_decimal(cr_pricing_refunded_cash,
                                       CR_PRICING_REFUNDED_CASH,
                                       row.pricing.refunded_cash));
    TPCDS_RETURN_NOT_OK(append_decimal(cr_pricing_reversed_charge,
                                       CR_PRICING_REVERSED_CHARGE,
                                       row.pricing.reversed_charge));
    TPCDS_RETURN_NOT_OK(append_decimal(cr_pricing_store_credit,
                                       CR_PRICING_STORE_CREDIT,
                                       row.pricing.store_credit));
    TPCDS_RETURN_NOT_OK(append_decimal(cr_pricing_net_loss, CR_PRICING_NET_LOSS,
                                       row.pricing.net_loss));

    impl_->row_generator_.ConsumeRemainingSeedsForRow();
    ++impl_->current_row_;
    --impl_->remaining_rows_;
  }

  std::vector<std::shared_ptr<arrow::Array>> arrays;
  arrays.reserve(27);
  std::shared_ptr<arrow::Array> array;

  TPCDS_RETURN_NOT_OK(cr_returned_date_sk.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cr_returned_time_sk.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cr_item_sk.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cr_refunded_customer_sk.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cr_refunded_cdemo_sk.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cr_refunded_hdemo_sk.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cr_refunded_addr_sk.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cr_returning_customer_sk.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cr_returning_cdemo_sk.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cr_returning_hdemo_sk.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cr_returning_addr_sk.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cr_call_center_sk.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cr_catalog_page_sk.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cr_ship_mode_sk.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cr_warehouse_sk.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cr_reason_sk.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cr_order_number.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cr_pricing_quantity.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cr_pricing_net_paid.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cr_pricing_ext_tax.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cr_pricing_net_paid_inc_tax.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cr_pricing_fee.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cr_pricing_ext_ship_cost.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cr_pricing_refunded_cash.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cr_pricing_reversed_charge.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cr_pricing_store_credit.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cr_pricing_net_loss.Finish(&array));
  arrays.push_back(array);

  return impl_->column_selection_.MakeRecordBatch(batch_rows, std::move(arrays),
                                                  out);
}

int64_t CatalogReturnsGenerator::total_rows() const {
  return impl_->total_rows_;
}

int64_t CatalogReturnsGenerator::remaining_rows() const {
  return impl_->remaining_rows_;
}

int64_t CatalogReturnsGenerator::TotalRows(double scale_factor) {
  return ComputeCatalogReturnsRows(scale_factor);
}

}  // namespace benchgen::tpcds
