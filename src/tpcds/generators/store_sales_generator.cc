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

#include "generators/store_sales_generator.h"

#include <algorithm>
#include <stdexcept>
#include <string>

#include "distribution/scaling.h"
#include "generators/store_sales_row_generator.h"
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

std::shared_ptr<arrow::Schema> BuildStoreSalesSchema() {
  return arrow::schema({
      arrow::field("ss_sold_date_sk", arrow::int32()),
      arrow::field("ss_sold_time_sk", arrow::int32()),
      arrow::field("ss_item_sk", arrow::int64()),
      arrow::field("ss_customer_sk", arrow::int64()),
      arrow::field("ss_cdemo_sk", arrow::int64()),
      arrow::field("ss_hdemo_sk", arrow::int64()),
      arrow::field("ss_addr_sk", arrow::int64()),
      arrow::field("ss_store_sk", arrow::int64()),
      arrow::field("ss_promo_sk", arrow::int64()),
      arrow::field("ss_ticket_number", arrow::int64(), false),
      arrow::field("ss_quantity", arrow::int32()),
      arrow::field("ss_wholesale_cost", arrow::smallest_decimal(7, 2)),
      arrow::field("ss_list_price", arrow::smallest_decimal(7, 2)),
      arrow::field("ss_sales_price", arrow::smallest_decimal(7, 2)),
      arrow::field("ss_ext_discount_amt", arrow::smallest_decimal(7, 2)),
      arrow::field("ss_ext_sales_price", arrow::smallest_decimal(7, 2)),
      arrow::field("ss_ext_wholesale_cost", arrow::smallest_decimal(7, 2)),
      arrow::field("ss_ext_list_price", arrow::smallest_decimal(7, 2)),
      arrow::field("ss_ext_tax", arrow::smallest_decimal(7, 2)),
      arrow::field("ss_coupon_amt", arrow::smallest_decimal(7, 2)),
      arrow::field("ss_net_paid", arrow::smallest_decimal(7, 2)),
      arrow::field("ss_net_paid_inc_tax", arrow::smallest_decimal(7, 2)),
      arrow::field("ss_net_profit", arrow::smallest_decimal(7, 2)),
  });
}

int64_t ComputeStoreSalesLineItems(double scale_factor) {
  internal::Scaling scaling(scale_factor);
  int64_t orders = scaling.RowCountByTableNumber(STORE_SALES);
  internal::RandomNumberStream stream(SS_TICKET_NUMBER,
                                      internal::SeedsPerRow(SS_TICKET_NUMBER));
  int64_t total = 0;
  for (int64_t i = 0; i < orders; ++i) {
    total += internal::GenerateUniformRandomInt(8, 16, &stream);
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

struct StoreSalesGenerator::Impl {
  explicit Impl(GeneratorOptions options)
      : options_(std::move(options)),
        schema_(BuildStoreSalesSchema()),
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
            .RowCountByTableNumber(STORE_SALES);
    total_rows_ = ComputeStoreSalesLineItems(options_.scale_factor);
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
    int64_t order_number = OrderNumberForRow(next_row, SS_TICKET_NUMBER, 8, 16);
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
  internal::StoreSalesRowGenerator row_generator_;
};

StoreSalesGenerator::StoreSalesGenerator(GeneratorOptions options)
    : impl_(std::make_unique<Impl>(std::move(options))) {}

StoreSalesGenerator::~StoreSalesGenerator() = default;

std::shared_ptr<arrow::Schema> StoreSalesGenerator::schema() const {
  return impl_->schema_;
}

std::string_view StoreSalesGenerator::name() const {
  return TableIdToString(TableId::kStoreSales);
}

std::string_view StoreSalesGenerator::suite_name() const { return "tpcds"; }

arrow::Status StoreSalesGenerator::Next(
    std::shared_ptr<arrow::RecordBatch>* out) {
  if (impl_->remaining_rows_ == 0) {
    *out = nullptr;
    return arrow::Status::OK();
  }

  const int64_t batch_rows =
      std::min(impl_->remaining_rows_, impl_->options_.chunk_size);

  arrow::MemoryPool* pool = arrow::default_memory_pool();
  arrow::Int32Builder ss_sold_date_sk(pool);
  arrow::Int32Builder ss_sold_time_sk(pool);
  arrow::Int64Builder ss_sold_item_sk(pool);
  arrow::Int64Builder ss_sold_customer_sk(pool);
  arrow::Int64Builder ss_sold_cdemo_sk(pool);
  arrow::Int64Builder ss_sold_hdemo_sk(pool);
  arrow::Int64Builder ss_sold_addr_sk(pool);
  arrow::Int64Builder ss_sold_store_sk(pool);
  arrow::Int64Builder ss_sold_promo_sk(pool);
  arrow::Int64Builder ss_ticket_number(pool);
  arrow::Int32Builder ss_pricing_quantity(pool);
  arrow::Decimal32Builder ss_pricing_wholesale_cost(
      arrow::smallest_decimal(7, 2), pool);
  arrow::Decimal32Builder ss_pricing_list_price(arrow::smallest_decimal(7, 2),
                                                pool);
  arrow::Decimal32Builder ss_pricing_sales_price(arrow::smallest_decimal(7, 2),
                                                 pool);
  arrow::Decimal32Builder ss_pricing_coupon_amt(arrow::smallest_decimal(7, 2),
                                                pool);
  arrow::Decimal32Builder ss_pricing_ext_sales_price(
      arrow::smallest_decimal(7, 2), pool);
  arrow::Decimal32Builder ss_pricing_ext_wholesale_cost(
      arrow::smallest_decimal(7, 2), pool);
  arrow::Decimal32Builder ss_pricing_ext_list_price(
      arrow::smallest_decimal(7, 2), pool);
  arrow::Decimal32Builder ss_pricing_ext_tax(arrow::smallest_decimal(7, 2),
                                             pool);
  arrow::Decimal32Builder ss_pricing_coupon_amt_dup(
      arrow::smallest_decimal(7, 2), pool);
  arrow::Decimal32Builder ss_pricing_net_paid(arrow::smallest_decimal(7, 2),
                                              pool);
  arrow::Decimal32Builder ss_pricing_net_paid_inc_tax(
      arrow::smallest_decimal(7, 2), pool);
  arrow::Decimal32Builder ss_pricing_net_profit(arrow::smallest_decimal(7, 2),
                                                pool);

#define TPCDS_RETURN_NOT_OK(status)   \
  do {                                \
    arrow::Status _status = (status); \
    if (!_status.ok()) {              \
      return _status;                 \
    }                                 \
  } while (false)

  TPCDS_RETURN_NOT_OK(ss_sold_date_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(ss_sold_time_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(ss_sold_item_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(ss_sold_customer_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(ss_sold_cdemo_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(ss_sold_hdemo_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(ss_sold_addr_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(ss_sold_store_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(ss_sold_promo_sk.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(ss_ticket_number.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(ss_pricing_quantity.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(ss_pricing_wholesale_cost.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(ss_pricing_list_price.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(ss_pricing_sales_price.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(ss_pricing_coupon_amt.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(ss_pricing_ext_sales_price.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(ss_pricing_ext_wholesale_cost.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(ss_pricing_ext_list_price.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(ss_pricing_ext_tax.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(ss_pricing_coupon_amt_dup.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(ss_pricing_net_paid.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(ss_pricing_net_paid_inc_tax.Reserve(batch_rows));
  TPCDS_RETURN_NOT_OK(ss_pricing_net_profit.Reserve(batch_rows));

  for (int64_t i = 0; i < batch_rows; ++i) {
    int64_t order_number = impl_->current_order_ + 1;
    internal::StoreSalesRowData row =
        impl_->row_generator_.GenerateRow(order_number);

    auto is_null = [&](int column_id) {
      return internal::IsNull(row.null_bitmap, STORE_SALES, column_id);
    };

    auto append_decimal = [&](arrow::Decimal32Builder& builder, int column_id,
                              const internal::Decimal& val) {
      if (is_null(column_id)) {
        return builder.AppendNull();
      }
      arrow::Decimal32 dec_val(val.number);
      return builder.Append(dec_val);
    };

    if (is_null(SS_SOLD_DATE_SK)) {
      TPCDS_RETURN_NOT_OK(ss_sold_date_sk.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(ss_sold_date_sk.Append(row.sold_date_sk));
    }

    if (is_null(SS_SOLD_TIME_SK)) {
      TPCDS_RETURN_NOT_OK(ss_sold_time_sk.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(ss_sold_time_sk.Append(row.sold_time_sk));
    }

    if (is_null(SS_SOLD_ITEM_SK)) {
      TPCDS_RETURN_NOT_OK(ss_sold_item_sk.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(ss_sold_item_sk.Append(row.sold_item_sk));
    }

    if (is_null(SS_SOLD_CUSTOMER_SK)) {
      TPCDS_RETURN_NOT_OK(ss_sold_customer_sk.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(ss_sold_customer_sk.Append(row.sold_customer_sk));
    }

    if (is_null(SS_SOLD_CDEMO_SK)) {
      TPCDS_RETURN_NOT_OK(ss_sold_cdemo_sk.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(ss_sold_cdemo_sk.Append(row.sold_cdemo_sk));
    }

    if (is_null(SS_SOLD_HDEMO_SK)) {
      TPCDS_RETURN_NOT_OK(ss_sold_hdemo_sk.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(ss_sold_hdemo_sk.Append(row.sold_hdemo_sk));
    }

    if (is_null(SS_SOLD_ADDR_SK)) {
      TPCDS_RETURN_NOT_OK(ss_sold_addr_sk.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(ss_sold_addr_sk.Append(row.sold_addr_sk));
    }

    if (is_null(SS_SOLD_STORE_SK)) {
      TPCDS_RETURN_NOT_OK(ss_sold_store_sk.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(ss_sold_store_sk.Append(row.sold_store_sk));
    }

    if (is_null(SS_SOLD_PROMO_SK) || row.sold_promo_sk == -1) {
      TPCDS_RETURN_NOT_OK(ss_sold_promo_sk.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(ss_sold_promo_sk.Append(row.sold_promo_sk));
    }

    TPCDS_RETURN_NOT_OK(ss_ticket_number.Append(row.ticket_number));

    if (is_null(SS_PRICING_QUANTITY)) {
      TPCDS_RETURN_NOT_OK(ss_pricing_quantity.AppendNull());
    } else {
      TPCDS_RETURN_NOT_OK(ss_pricing_quantity.Append(row.pricing.quantity));
    }

    TPCDS_RETURN_NOT_OK(append_decimal(ss_pricing_wholesale_cost,
                                       SS_PRICING_WHOLESALE_COST,
                                       row.pricing.wholesale_cost));
    TPCDS_RETURN_NOT_OK(append_decimal(
        ss_pricing_list_price, SS_PRICING_LIST_PRICE, row.pricing.list_price));
    TPCDS_RETURN_NOT_OK(append_decimal(ss_pricing_sales_price,
                                       SS_PRICING_SALES_PRICE,
                                       row.pricing.sales_price));
    TPCDS_RETURN_NOT_OK(append_decimal(
        ss_pricing_coupon_amt, SS_PRICING_COUPON_AMT, row.pricing.coupon_amt));
    TPCDS_RETURN_NOT_OK(append_decimal(ss_pricing_ext_sales_price,
                                       SS_PRICING_EXT_SALES_PRICE,
                                       row.pricing.ext_sales_price));
    TPCDS_RETURN_NOT_OK(append_decimal(ss_pricing_ext_wholesale_cost,
                                       SS_PRICING_EXT_WHOLESALE_COST,
                                       row.pricing.ext_wholesale_cost));
    TPCDS_RETURN_NOT_OK(append_decimal(ss_pricing_ext_list_price,
                                       SS_PRICING_EXT_LIST_PRICE,
                                       row.pricing.ext_list_price));
    TPCDS_RETURN_NOT_OK(append_decimal(ss_pricing_ext_tax, SS_PRICING_EXT_TAX,
                                       row.pricing.ext_tax));
    TPCDS_RETURN_NOT_OK(append_decimal(ss_pricing_coupon_amt_dup,
                                       SS_PRICING_COUPON_AMT,
                                       row.pricing.coupon_amt));
    TPCDS_RETURN_NOT_OK(append_decimal(ss_pricing_net_paid, SS_PRICING_NET_PAID,
                                       row.pricing.net_paid));
    TPCDS_RETURN_NOT_OK(append_decimal(ss_pricing_net_paid_inc_tax,
                                       SS_PRICING_NET_PAID_INC_TAX,
                                       row.pricing.net_paid_inc_tax));
    TPCDS_RETURN_NOT_OK(append_decimal(
        ss_pricing_net_profit, SS_PRICING_NET_PROFIT, row.pricing.net_profit));

    impl_->row_generator_.ConsumeRemainingSeedsForRow();
    ++impl_->current_row_;
    --impl_->remaining_rows_;

    if (impl_->row_generator_.LastRowInTicket()) {
      impl_->current_order_ = order_number;
    }
  }

  std::vector<std::shared_ptr<arrow::Array>> arrays;
  arrays.reserve(23);
  std::shared_ptr<arrow::Array> array;

  TPCDS_RETURN_NOT_OK(ss_sold_date_sk.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(ss_sold_time_sk.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(ss_sold_item_sk.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(ss_sold_customer_sk.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(ss_sold_cdemo_sk.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(ss_sold_hdemo_sk.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(ss_sold_addr_sk.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(ss_sold_store_sk.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(ss_sold_promo_sk.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(ss_ticket_number.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(ss_pricing_quantity.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(ss_pricing_wholesale_cost.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(ss_pricing_list_price.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(ss_pricing_sales_price.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(ss_pricing_coupon_amt.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(ss_pricing_ext_sales_price.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(ss_pricing_ext_wholesale_cost.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(ss_pricing_ext_list_price.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(ss_pricing_ext_tax.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(ss_pricing_coupon_amt_dup.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(ss_pricing_net_paid.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(ss_pricing_net_paid_inc_tax.Finish(&array));
  arrays.push_back(array);
  TPCDS_RETURN_NOT_OK(ss_pricing_net_profit.Finish(&array));
  arrays.push_back(array);

  return impl_->column_selection_.MakeRecordBatch(batch_rows, std::move(arrays),
                                                  out);
}

int64_t StoreSalesGenerator::total_rows() const { return impl_->total_rows_; }

int64_t StoreSalesGenerator::remaining_rows() const {
  return impl_->remaining_rows_;
}

int64_t StoreSalesGenerator::TotalRows(double scale_factor) {
  return ComputeStoreSalesLineItems(scale_factor);
}

}  // namespace benchgen::tpcds
