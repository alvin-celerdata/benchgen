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

#include "generators/catalog_sales_generator.h"

#include <algorithm>
#include <stdexcept>
#include <string>

#include "distribution/scaling.h"
#include "generators/catalog_sales_row_generator.h"
#include "util/column_selection.h"
#include "utils/column_streams.h"
#include "utils/columns.h"
#include "utils/decimal.h"
#include "utils/null_utils.h"
#include "utils/random_number_stream.h"
#include "utils/random_utils.h"
#include "utils/tables.h"

namespace benchgen::tpcds {
namespace {

std::shared_ptr<arrow::Schema> BuildCatalogSalesSchema() {
  return arrow::schema({
      arrow::field("cs_sold_date_sk", arrow::int32()),
      arrow::field("cs_sold_time_sk", arrow::int32()),
      arrow::field("cs_ship_date_sk", arrow::int32()),
      arrow::field("cs_bill_customer_sk", arrow::int64()),
      arrow::field("cs_bill_cdemo_sk", arrow::int64()),
      arrow::field("cs_bill_hdemo_sk", arrow::int64()),
      arrow::field("cs_bill_addr_sk", arrow::int64()),
      arrow::field("cs_ship_customer_sk", arrow::int64()),
      arrow::field("cs_ship_cdemo_sk", arrow::int64()),
      arrow::field("cs_ship_hdemo_sk", arrow::int64()),
      arrow::field("cs_ship_addr_sk", arrow::int64()),
      arrow::field("cs_call_center_sk", arrow::int64()),
      arrow::field("cs_catalog_page_sk", arrow::int64()),
      arrow::field("cs_ship_mode_sk", arrow::int64()),
      arrow::field("cs_warehouse_sk", arrow::int64()),
      arrow::field("cs_item_sk", arrow::int64()),
      arrow::field("cs_promo_sk", arrow::int64()),
      arrow::field("cs_order_number", arrow::int64(), false),
      arrow::field("cs_quantity", arrow::int32()),
      arrow::field("cs_wholesale_cost", arrow::smallest_decimal(7, 2)),
      arrow::field("cs_list_price", arrow::smallest_decimal(7, 2)),
      arrow::field("cs_sales_price", arrow::smallest_decimal(7, 2)),
      arrow::field("cs_ext_discount_amt", arrow::smallest_decimal(7, 2)),
      arrow::field("cs_ext_sales_price", arrow::smallest_decimal(7, 2)),
      arrow::field("cs_ext_wholesale_cost", arrow::smallest_decimal(7, 2)),
      arrow::field("cs_ext_list_price", arrow::smallest_decimal(7, 2)),
      arrow::field("cs_ext_tax", arrow::smallest_decimal(7, 2)),
      arrow::field("cs_coupon_amt", arrow::smallest_decimal(7, 2)),
      arrow::field("cs_ext_ship_cost", arrow::smallest_decimal(7, 2)),
      arrow::field("cs_net_paid", arrow::smallest_decimal(7, 2)),
      arrow::field("cs_net_paid_inc_tax", arrow::smallest_decimal(7, 2)),
      arrow::field("cs_net_paid_inc_ship", arrow::smallest_decimal(7, 2)),
      arrow::field("cs_net_paid_inc_ship_tax", arrow::smallest_decimal(7, 2)),
      arrow::field("cs_net_profit", arrow::smallest_decimal(7, 2)),
  });
}

int64_t ComputeCatalogSalesLineItems(double scale_factor) {
  internal::Scaling scaling(scale_factor);
  int64_t orders = scaling.RowCountByTableNumber(CATALOG_SALES);
  internal::RandomNumberStream stream(CS_ORDER_NUMBER,
                                      internal::SeedsPerRow(CS_ORDER_NUMBER));
  int64_t total = 0;
  for (int64_t i = 0; i < orders; ++i) {
    total += internal::GenerateUniformRandomInt(4, 14, &stream);
    while (stream.seeds_used() < stream.seeds_per_row()) {
      internal::GenerateUniformRandomInt(1, 100, &stream);
    }
    stream.ResetSeedsUsed();
  }
  return total;
}

int64_t OrderNumberForRow(int64_t row_number, int column_id, int min_items,
                          int max_items) {
  if (row_number <= 1) {
    return 1;
  }
  internal::RandomNumberStream stream(column_id,
                                      internal::SeedsPerRow(column_id));
  int64_t ticket_start_row = 1;
  int64_t order_number = 1;
  while (true) {
    int items =
        internal::GenerateUniformRandomInt(min_items, max_items, &stream);
    while (stream.seeds_used() < stream.seeds_per_row()) {
      internal::GenerateUniformRandomInt(1, 100, &stream);
    }
    stream.ResetSeedsUsed();
    int64_t ticket_end_row = ticket_start_row + items - 1;
    if (row_number <= ticket_end_row) {
      return order_number;
    }
    ticket_start_row = ticket_end_row + 1;
    ++order_number;
  }
}

}  // namespace

struct CatalogSalesGenerator::Impl {
  explicit Impl(GeneratorOptions options)
      : options_(std::move(options)),
        schema_(BuildCatalogSalesSchema()),
        row_generator_(options_.scale_factor) {
    if (options_.chunk_size <= 0) {
      throw std::invalid_argument("chunk_size must be positive");
    }
    auto status = column_selection_.Init(schema_, options_.column_names);
    if (!status.ok()) {
      throw std::invalid_argument(status.ToString());
    }
    schema_ = column_selection_.schema();
    total_orders_ =
        internal::Scaling(options_.scale_factor)
            .RowCountByTableNumber(CATALOG_SALES);
    total_rows_ = ComputeCatalogSalesLineItems(options_.scale_factor);
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
    int64_t next_row = options_.start_row + 1;
    int64_t order_number = OrderNumberForRow(next_row, CS_ORDER_NUMBER, 4, 14);
    current_order_ = order_number - 1;
  }

  GeneratorOptions options_;
  int64_t total_orders_ = 0;
  int64_t total_rows_ = 0;
  int64_t remaining_rows_ = 0;
  int64_t current_row_ = 0;
  int64_t current_order_ = 0;
  std::shared_ptr<arrow::Schema> schema_;
  ::benchgen::internal::ColumnSelection column_selection_;
  internal::CatalogSalesRowGenerator row_generator_;
};

CatalogSalesGenerator::CatalogSalesGenerator(GeneratorOptions options)
    : impl_(std::make_unique<Impl>(std::move(options))) {}

CatalogSalesGenerator::~CatalogSalesGenerator() = default;

std::shared_ptr<arrow::Schema> CatalogSalesGenerator::schema() const {
  return impl_->schema_;
}

std::string_view CatalogSalesGenerator::name() const {
  return TableIdToString(TableId::kCatalogSales);
}

std::string_view CatalogSalesGenerator::suite_name() const { return "tpcds"; }

arrow::Status CatalogSalesGenerator::Next(
    std::shared_ptr<arrow::RecordBatch>* out) {
  if (impl_->remaining_rows_ == 0) {
    *out = nullptr;
    return arrow::Status::OK();
  }

  const int64_t batch_rows =
      std::min(impl_->remaining_rows_, impl_->options_.chunk_size);

  arrow::MemoryPool* pool = arrow::default_memory_pool();
  arrow::Int32Builder cs_sold_date_sk(pool);
  arrow::Int32Builder cs_sold_time_sk(pool);
  arrow::Int32Builder cs_ship_date_sk(pool);
  arrow::Int64Builder cs_bill_customer_sk(pool);
  arrow::Int64Builder cs_bill_cdemo_sk(pool);
  arrow::Int64Builder cs_bill_hdemo_sk(pool);
  arrow::Int64Builder cs_bill_addr_sk(pool);
  arrow::Int64Builder cs_ship_customer_sk(pool);
  arrow::Int64Builder cs_ship_cdemo_sk(pool);
  arrow::Int64Builder cs_ship_hdemo_sk(pool);
  arrow::Int64Builder cs_ship_addr_sk(pool);
  arrow::Int64Builder cs_call_center_sk(pool);
  arrow::Int64Builder cs_catalog_page_sk(pool);
  arrow::Int64Builder cs_ship_mode_sk(pool);
  arrow::Int64Builder cs_warehouse_sk(pool);
  arrow::Int64Builder cs_sold_item_sk(pool);
  arrow::Int64Builder cs_promo_sk(pool);
  arrow::Int64Builder cs_order_number(pool);
  arrow::Int32Builder cs_pricing_quantity(pool);
  arrow::Decimal32Builder cs_pricing_wholesale_cost(
      arrow::smallest_decimal(7, 2), pool);
  arrow::Decimal32Builder cs_pricing_list_price(arrow::smallest_decimal(7, 2),
                                                pool);
  arrow::Decimal32Builder cs_pricing_sales_price(arrow::smallest_decimal(7, 2),
                                                 pool);
  arrow::Decimal32Builder cs_pricing_ext_sales_price(
      arrow::smallest_decimal(7, 2), pool);
  arrow::Decimal32Builder cs_pricing_ext_discount_amt(
      arrow::smallest_decimal(7, 2), pool);
  arrow::Decimal32Builder cs_pricing_ext_wholesale_cost(
      arrow::smallest_decimal(7, 2), pool);
  arrow::Decimal32Builder cs_pricing_ext_list_price(
      arrow::smallest_decimal(7, 2), pool);
  arrow::Decimal32Builder cs_pricing_ext_tax(arrow::smallest_decimal(7, 2),
                                             pool);
  arrow::Decimal32Builder cs_pricing_coupon_amt(arrow::smallest_decimal(7, 2),
                                                pool);
  arrow::Decimal32Builder cs_pricing_ext_ship_cost(
      arrow::smallest_decimal(7, 2), pool);
  arrow::Decimal32Builder cs_pricing_net_paid(arrow::smallest_decimal(7, 2),
                                              pool);
  arrow::Decimal32Builder cs_pricing_net_paid_inc_tax(
      arrow::smallest_decimal(7, 2), pool);
  arrow::Decimal32Builder cs_pricing_net_paid_inc_ship(
      arrow::smallest_decimal(7, 2), pool);
  arrow::Decimal32Builder cs_pricing_net_paid_inc_ship_tax(
      arrow::smallest_decimal(7, 2), pool);
  arrow::Decimal32Builder cs_pricing_net_profit(arrow::smallest_decimal(7, 2),
                                                pool);

#define TPCDS_RETURN_NOT_OK(status)   \
  do {                                \
    arrow::Status _status = (status); \
    if (!_status.ok()) {              \
      return _status;                 \
    }                                 \
  } while (false)

  TPCDS_RETURN_NOT_OK(cs_sold_date_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cs_sold_time_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cs_ship_date_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cs_bill_customer_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cs_bill_cdemo_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cs_bill_hdemo_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cs_bill_addr_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cs_ship_customer_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cs_ship_cdemo_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cs_ship_hdemo_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cs_ship_addr_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cs_call_center_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cs_catalog_page_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cs_ship_mode_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cs_warehouse_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cs_sold_item_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cs_promo_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cs_order_number.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cs_pricing_quantity.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cs_pricing_wholesale_cost.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cs_pricing_list_price.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cs_pricing_sales_price.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cs_pricing_coupon_amt.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cs_pricing_ext_sales_price.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cs_pricing_ext_discount_amt.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cs_pricing_ext_wholesale_cost.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cs_pricing_ext_list_price.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cs_pricing_ext_tax.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cs_pricing_ext_ship_cost.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cs_pricing_net_paid.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cs_pricing_net_paid_inc_tax.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cs_pricing_net_paid_inc_ship.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cs_pricing_net_paid_inc_ship_tax.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(cs_pricing_net_profit.Reserve(batch_rows));

  for (int64_t i = 0; i < batch_rows; ++i) {
    // Order number for this row
    int64_t order_number = impl_->current_order_ + 1;
    internal::CatalogSalesRowData row =
        impl_->row_generator_.GenerateRow(order_number);

    auto is_null = [&](int column_id) {
      return internal::IsNull(row.null_bitmap, CATALOG_SALES, column_id);
    };

    if (is_null(CS_SOLD_DATE_SK)) {
      TPCDS_RETURN_NOT_OK(cs_sold_date_sk.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(cs_sold_date_sk.Append(row.sold_date_sk));
    }

    if (is_null(CS_SOLD_TIME_SK)) {
      TPCDS_RETURN_NOT_OK(cs_sold_time_sk.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(cs_sold_time_sk.Append(row.sold_time_sk));
    }

    if (is_null(CS_SHIP_DATE_SK)) {
      TPCDS_RETURN_NOT_OK(cs_ship_date_sk.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(cs_ship_date_sk.Append(row.ship_date_sk));
    }

    if (is_null(CS_BILL_CUSTOMER_SK)) {
      TPCDS_RETURN_NOT_OK(cs_bill_customer_sk.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(cs_bill_customer_sk.Append(row.bill_customer_sk));
    }

    if (is_null(CS_BILL_CDEMO_SK)) {
      TPCDS_RETURN_NOT_OK(cs_bill_cdemo_sk.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(cs_bill_cdemo_sk.Append(row.bill_cdemo_sk));
    }

    if (is_null(CS_BILL_HDEMO_SK)) {
      TPCDS_RETURN_NOT_OK(cs_bill_hdemo_sk.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(cs_bill_hdemo_sk.Append(row.bill_hdemo_sk));
    }

    if (is_null(CS_BILL_ADDR_SK)) {
      TPCDS_RETURN_NOT_OK(cs_bill_addr_sk.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(cs_bill_addr_sk.Append(row.bill_addr_sk));
    }

    if (is_null(CS_SHIP_CUSTOMER_SK)) {
      TPCDS_RETURN_NOT_OK(cs_ship_customer_sk.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(cs_ship_customer_sk.Append(row.ship_customer_sk));
    }

    if (is_null(CS_SHIP_CDEMO_SK)) {
      TPCDS_RETURN_NOT_OK(cs_ship_cdemo_sk.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(cs_ship_cdemo_sk.Append(row.ship_cdemo_sk));
    }

    if (is_null(CS_SHIP_HDEMO_SK)) {
      TPCDS_RETURN_NOT_OK(cs_ship_hdemo_sk.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(cs_ship_hdemo_sk.Append(row.ship_hdemo_sk));
    }

    if (is_null(CS_SHIP_ADDR_SK)) {
      TPCDS_RETURN_NOT_OK(cs_ship_addr_sk.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(cs_ship_addr_sk.Append(row.ship_addr_sk));
    }

    if (is_null(CS_CALL_CENTER_SK)) {
      TPCDS_RETURN_NOT_OK(cs_call_center_sk.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(cs_call_center_sk.Append(row.call_center_sk));
    }

    if (is_null(CS_CATALOG_PAGE_SK)) {
      TPCDS_RETURN_NOT_OK(cs_catalog_page_sk.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(cs_catalog_page_sk.Append(row.catalog_page_sk));
    }

    if (is_null(CS_SHIP_MODE_SK)) {
      TPCDS_RETURN_NOT_OK(cs_ship_mode_sk.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(cs_ship_mode_sk.Append(row.ship_mode_sk));
    }

    if (is_null(CS_WAREHOUSE_SK)) {
      TPCDS_RETURN_NOT_OK(cs_warehouse_sk.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(cs_warehouse_sk.Append(row.warehouse_sk));
    }

    if (is_null(CS_SOLD_ITEM_SK)) {
      TPCDS_RETURN_NOT_OK(cs_sold_item_sk.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(cs_sold_item_sk.Append(row.sold_item_sk));
    }

    if (is_null(CS_PROMO_SK) || row.promo_sk == -1) {
      TPCDS_RETURN_NOT_OK(cs_promo_sk.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(cs_promo_sk.Append(row.promo_sk));
    }

    TPCDS_RETURN_NOT_OK(cs_order_number.Append(row.order_number));

    if (is_null(CS_PRICING_QUANTITY)) {
      TPCDS_RETURN_NOT_OK(cs_pricing_quantity.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(cs_pricing_quantity.Append(row.pricing.quantity));
    }

    auto append_decimal = [&](arrow::Decimal32Builder& builder, int column_id,
                              const internal::Decimal& val) {
      if (is_null(column_id)) {
        return builder.AppendNull();
      }
      arrow::Decimal32 dec_val(static_cast<int32_t>(val.number));
      return builder.Append(dec_val);
    };

    TPCDS_RETURN_NOT_OK(append_decimal(cs_pricing_wholesale_cost,
                                       CS_PRICING_WHOLESALE_COST,
                                       row.pricing.wholesale_cost));
    TPCDS_RETURN_NOT_OK(append_decimal(
        cs_pricing_list_price, CS_PRICING_LIST_PRICE, row.pricing.list_price));
    TPCDS_RETURN_NOT_OK(append_decimal(cs_pricing_sales_price,
                                       CS_PRICING_SALES_PRICE,
                                       row.pricing.sales_price));
    TPCDS_RETURN_NOT_OK(append_decimal(cs_pricing_ext_discount_amt,
                                       CS_PRICING_EXT_DISCOUNT_AMOUNT,
                                       row.pricing.ext_discount_amt));
    TPCDS_RETURN_NOT_OK(append_decimal(cs_pricing_ext_sales_price,
                                       CS_PRICING_EXT_SALES_PRICE,
                                       row.pricing.ext_sales_price));
    TPCDS_RETURN_NOT_OK(append_decimal(cs_pricing_ext_wholesale_cost,
                                       CS_PRICING_EXT_WHOLESALE_COST,
                                       row.pricing.ext_wholesale_cost));
    TPCDS_RETURN_NOT_OK(append_decimal(cs_pricing_ext_list_price,
                                       CS_PRICING_EXT_LIST_PRICE,
                                       row.pricing.ext_list_price));
    TPCDS_RETURN_NOT_OK(append_decimal(cs_pricing_ext_tax, CS_PRICING_EXT_TAX,
                                       row.pricing.ext_tax));
    TPCDS_RETURN_NOT_OK(append_decimal(
        cs_pricing_coupon_amt, CS_PRICING_COUPON_AMT, row.pricing.coupon_amt));
    TPCDS_RETURN_NOT_OK(append_decimal(cs_pricing_ext_ship_cost,
                                       CS_PRICING_EXT_SHIP_COST,
                                       row.pricing.ext_ship_cost));
    TPCDS_RETURN_NOT_OK(append_decimal(cs_pricing_net_paid, CS_PRICING_NET_PAID,
                                       row.pricing.net_paid));
    TPCDS_RETURN_NOT_OK(append_decimal(cs_pricing_net_paid_inc_tax,
                                       CS_PRICING_NET_PAID_INC_TAX,
                                       row.pricing.net_paid_inc_tax));
    TPCDS_RETURN_NOT_OK(append_decimal(cs_pricing_net_paid_inc_ship,
                                       CS_PRICING_NET_PAID_INC_SHIP,
                                       row.pricing.net_paid_inc_ship));
    TPCDS_RETURN_NOT_OK(append_decimal(cs_pricing_net_paid_inc_ship_tax,
                                       CS_PRICING_NET_PAID_INC_SHIP_TAX,
                                       row.pricing.net_paid_inc_ship_tax));
    TPCDS_RETURN_NOT_OK(append_decimal(
        cs_pricing_net_profit, CS_PRICING_NET_PROFIT, row.pricing.net_profit));

    impl_->row_generator_.ConsumeRemainingSeedsForRow();
    ++impl_->current_row_;
    --impl_->remaining_rows_;

    // Track order number
    if (impl_->row_generator_.LastRowInOrder()) {
      impl_->current_order_ = order_number;
    }
  }

  std::vector<std::shared_ptr<arrow::Array>> arrays;
  arrays.reserve(35);
  std::shared_ptr<arrow::Array> array;

  TPCDS_RETURN_NOT_OK(cs_sold_date_sk.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cs_sold_time_sk.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cs_ship_date_sk.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cs_bill_customer_sk.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cs_bill_cdemo_sk.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cs_bill_hdemo_sk.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cs_bill_addr_sk.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cs_ship_customer_sk.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cs_ship_cdemo_sk.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cs_ship_hdemo_sk.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cs_ship_addr_sk.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cs_call_center_sk.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cs_catalog_page_sk.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cs_ship_mode_sk.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cs_warehouse_sk.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cs_sold_item_sk.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cs_promo_sk.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cs_order_number.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cs_pricing_quantity.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cs_pricing_wholesale_cost.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cs_pricing_list_price.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cs_pricing_sales_price.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cs_pricing_ext_discount_amt.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cs_pricing_ext_sales_price.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cs_pricing_ext_wholesale_cost.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cs_pricing_ext_list_price.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cs_pricing_ext_tax.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cs_pricing_coupon_amt.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cs_pricing_ext_ship_cost.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cs_pricing_net_paid.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cs_pricing_net_paid_inc_tax.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cs_pricing_net_paid_inc_ship.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cs_pricing_net_paid_inc_ship_tax.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(cs_pricing_net_profit.Finish(&array));
  arrays.push_back(array);

  return impl_->column_selection_.MakeRecordBatch(batch_rows, std::move(arrays),
                                                  out);
}

int64_t CatalogSalesGenerator::total_rows() const { return impl_->total_rows_; }

int64_t CatalogSalesGenerator::remaining_rows() const {
  return impl_->remaining_rows_;
}

int64_t CatalogSalesGenerator::TotalRows(double scale_factor) {
  return ComputeCatalogSalesLineItems(scale_factor);
}

}  // namespace benchgen::tpcds
