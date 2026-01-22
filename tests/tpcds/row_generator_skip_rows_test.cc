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

#include <gtest/gtest.h>

#include <tuple>
#include <utility>

#include "generators/call_center_row_generator.h"
#include "generators/catalog_page_row_generator.h"
#include "generators/catalog_returns_row_generator.h"
#include "generators/catalog_sales_row_generator.h"
#include "generators/customer_address_row_generator.h"
#include "generators/customer_row_generator.h"
#include "generators/inventory_row_generator.h"
#include "generators/item_row_generator.h"
#include "generators/promotion_row_generator.h"
#include "generators/ship_mode_row_generator.h"
#include "generators/store_returns_row_generator.h"
#include "generators/store_row_generator.h"
#include "generators/store_sales_row_generator.h"
#include "generators/warehouse_row_generator.h"
#include "generators/web_page_row_generator.h"
#include "generators/web_returns_row_generator.h"
#include "generators/web_sales_row_generator.h"
#include "generators/web_site_row_generator.h"

namespace benchgen::tpcds::internal {

bool operator==(const Decimal& lhs, const Decimal& rhs) {
  return std::tie(lhs.scale, lhs.precision, lhs.number) ==
         std::tie(rhs.scale, rhs.precision, rhs.number);
}

bool operator==(const Address& lhs, const Address& rhs) {
  return std::tie(lhs.street_num, lhs.street_name1, lhs.street_name2,
                  lhs.street_type, lhs.suite_num, lhs.city, lhs.county,
                  lhs.state, lhs.country, lhs.zip, lhs.plus4, lhs.gmt_offset) ==
         std::tie(rhs.street_num, rhs.street_name1, rhs.street_name2,
                  rhs.street_type, rhs.suite_num, rhs.city, rhs.county,
                  rhs.state, rhs.country, rhs.zip, rhs.plus4, rhs.gmt_offset);
}

bool operator==(const Pricing& lhs, const Pricing& rhs) {
  return std::tie(lhs.wholesale_cost, lhs.list_price, lhs.sales_price,
                  lhs.quantity, lhs.ext_discount_amt, lhs.ext_sales_price,
                  lhs.ext_wholesale_cost, lhs.ext_list_price, lhs.tax_pct,
                  lhs.ext_tax, lhs.coupon_amt, lhs.ship_cost, lhs.ext_ship_cost,
                  lhs.net_paid, lhs.net_paid_inc_tax, lhs.net_paid_inc_ship,
                  lhs.net_paid_inc_ship_tax, lhs.net_profit, lhs.refunded_cash,
                  lhs.reversed_charge, lhs.store_credit, lhs.fee,
                  lhs.net_loss) ==
         std::tie(rhs.wholesale_cost, rhs.list_price, rhs.sales_price,
                  rhs.quantity, rhs.ext_discount_amt, rhs.ext_sales_price,
                  rhs.ext_wholesale_cost, rhs.ext_list_price, rhs.tax_pct,
                  rhs.ext_tax, rhs.coupon_amt, rhs.ship_cost, rhs.ext_ship_cost,
                  rhs.net_paid, rhs.net_paid_inc_tax, rhs.net_paid_inc_ship,
                  rhs.net_paid_inc_ship_tax, rhs.net_profit, rhs.refunded_cash,
                  rhs.reversed_charge, rhs.store_credit, rhs.fee, rhs.net_loss);
}

bool operator==(const CatalogPageRowData& lhs, const CatalogPageRowData& rhs) {
  return std::tie(lhs.catalog_page_sk, lhs.catalog_page_id, lhs.start_date_id,
                  lhs.end_date_id, lhs.department, lhs.catalog_number,
                  lhs.catalog_page_number, lhs.description, lhs.type,
                  lhs.null_bitmap) ==
         std::tie(rhs.catalog_page_sk, rhs.catalog_page_id, rhs.start_date_id,
                  rhs.end_date_id, rhs.department, rhs.catalog_number,
                  rhs.catalog_page_number, rhs.description, rhs.type,
                  rhs.null_bitmap);
}

bool operator==(const CustomerAddressRowData& lhs,
                const CustomerAddressRowData& rhs) {
  return std::tie(lhs.address_sk, lhs.address_id, lhs.street_num,
                  lhs.street_name, lhs.street_type, lhs.suite_num, lhs.city,
                  lhs.county, lhs.state, lhs.zip, lhs.country, lhs.gmt_offset,
                  lhs.location_type, lhs.null_bitmap) ==
         std::tie(rhs.address_sk, rhs.address_id, rhs.street_num,
                  rhs.street_name, rhs.street_type, rhs.suite_num, rhs.city,
                  rhs.county, rhs.state, rhs.zip, rhs.country, rhs.gmt_offset,
                  rhs.location_type, rhs.null_bitmap);
}

bool operator==(const CustomerRowData& lhs, const CustomerRowData& rhs) {
  return std::tie(lhs.customer_sk, lhs.customer_id, lhs.current_cdemo_sk,
                  lhs.current_hdemo_sk, lhs.current_addr_sk,
                  lhs.first_shipto_date_sk, lhs.first_sales_date_sk,
                  lhs.salutation, lhs.first_name, lhs.last_name,
                  lhs.preferred_cust_flag, lhs.birth_day, lhs.birth_month,
                  lhs.birth_year, lhs.birth_country, lhs.email_address,
                  lhs.last_review_date_sk, lhs.null_bitmap) ==
         std::tie(rhs.customer_sk, rhs.customer_id, rhs.current_cdemo_sk,
                  rhs.current_hdemo_sk, rhs.current_addr_sk,
                  rhs.first_shipto_date_sk, rhs.first_sales_date_sk,
                  rhs.salutation, rhs.first_name, rhs.last_name,
                  rhs.preferred_cust_flag, rhs.birth_day, rhs.birth_month,
                  rhs.birth_year, rhs.birth_country, rhs.email_address,
                  rhs.last_review_date_sk, rhs.null_bitmap);
}

bool operator==(const WarehouseRowData& lhs, const WarehouseRowData& rhs) {
  return std::tie(lhs.warehouse_sk, lhs.warehouse_id, lhs.warehouse_name,
                  lhs.warehouse_sq_ft, lhs.address, lhs.null_bitmap) ==
         std::tie(rhs.warehouse_sk, rhs.warehouse_id, rhs.warehouse_name,
                  rhs.warehouse_sq_ft, rhs.address, rhs.null_bitmap);
}

bool operator==(const InventoryRowData& lhs, const InventoryRowData& rhs) {
  return std::tie(lhs.date_sk, lhs.item_sk, lhs.warehouse_sk,
                  lhs.quantity_on_hand, lhs.null_bitmap) ==
         std::tie(rhs.date_sk, rhs.item_sk, rhs.warehouse_sk,
                  rhs.quantity_on_hand, rhs.null_bitmap);
}

bool operator==(const ShipModeRowData& lhs, const ShipModeRowData& rhs) {
  return std::tie(lhs.ship_mode_sk, lhs.ship_mode_id, lhs.type, lhs.code,
                  lhs.carrier, lhs.contract, lhs.null_bitmap) ==
         std::tie(rhs.ship_mode_sk, rhs.ship_mode_id, rhs.type, rhs.code,
                  rhs.carrier, rhs.contract, rhs.null_bitmap);
}

bool operator==(const PromotionRowData& lhs, const PromotionRowData& rhs) {
  return std::tie(lhs.promo_sk, lhs.promo_id, lhs.start_date_id,
                  lhs.end_date_id, lhs.item_sk, lhs.cost, lhs.response_target,
                  lhs.promo_name, lhs.channel_dmail, lhs.channel_email,
                  lhs.channel_catalog, lhs.channel_tv, lhs.channel_radio,
                  lhs.channel_press, lhs.channel_event, lhs.channel_demo,
                  lhs.channel_details, lhs.purpose, lhs.discount_active,
                  lhs.null_bitmap) ==
         std::tie(rhs.promo_sk, rhs.promo_id, rhs.start_date_id,
                  rhs.end_date_id, rhs.item_sk, rhs.cost, rhs.response_target,
                  rhs.promo_name, rhs.channel_dmail, rhs.channel_email,
                  rhs.channel_catalog, rhs.channel_tv, rhs.channel_radio,
                  rhs.channel_press, rhs.channel_event, rhs.channel_demo,
                  rhs.channel_details, rhs.purpose, rhs.discount_active,
                  rhs.null_bitmap);
}

bool operator==(const StoreSalesRowData& lhs, const StoreSalesRowData& rhs) {
  return std::tie(lhs.sold_date_sk, lhs.sold_time_sk, lhs.sold_item_sk,
                  lhs.sold_customer_sk, lhs.sold_cdemo_sk, lhs.sold_hdemo_sk,
                  lhs.sold_addr_sk, lhs.sold_store_sk, lhs.sold_promo_sk,
                  lhs.ticket_number, lhs.pricing, lhs.null_bitmap,
                  lhs.is_returned, lhs.last_row_in_ticket,
                  lhs.remaining_items) ==
         std::tie(rhs.sold_date_sk, rhs.sold_time_sk, rhs.sold_item_sk,
                  rhs.sold_customer_sk, rhs.sold_cdemo_sk, rhs.sold_hdemo_sk,
                  rhs.sold_addr_sk, rhs.sold_store_sk, rhs.sold_promo_sk,
                  rhs.ticket_number, rhs.pricing, rhs.null_bitmap,
                  rhs.is_returned, rhs.last_row_in_ticket, rhs.remaining_items);
}

bool operator==(const WebSalesRowData& lhs, const WebSalesRowData& rhs) {
  return std::tie(lhs.sold_date_sk, lhs.sold_time_sk, lhs.ship_date_sk,
                  lhs.item_sk, lhs.bill_customer_sk, lhs.bill_cdemo_sk,
                  lhs.bill_hdemo_sk, lhs.bill_addr_sk, lhs.ship_customer_sk,
                  lhs.ship_cdemo_sk, lhs.ship_hdemo_sk, lhs.ship_addr_sk,
                  lhs.web_page_sk, lhs.web_site_sk, lhs.ship_mode_sk,
                  lhs.warehouse_sk, lhs.promo_sk, lhs.order_number, lhs.pricing,
                  lhs.null_bitmap, lhs.is_returned) ==
         std::tie(rhs.sold_date_sk, rhs.sold_time_sk, rhs.ship_date_sk,
                  rhs.item_sk, rhs.bill_customer_sk, rhs.bill_cdemo_sk,
                  rhs.bill_hdemo_sk, rhs.bill_addr_sk, rhs.ship_customer_sk,
                  rhs.ship_cdemo_sk, rhs.ship_hdemo_sk, rhs.ship_addr_sk,
                  rhs.web_page_sk, rhs.web_site_sk, rhs.ship_mode_sk,
                  rhs.warehouse_sk, rhs.promo_sk, rhs.order_number, rhs.pricing,
                  rhs.null_bitmap, rhs.is_returned);
}

bool operator==(const CatalogSalesRowData& lhs,
                const CatalogSalesRowData& rhs) {
  return std::tie(lhs.sold_date_sk, lhs.sold_time_sk, lhs.ship_date_sk,
                  lhs.bill_customer_sk, lhs.bill_cdemo_sk, lhs.bill_hdemo_sk,
                  lhs.bill_addr_sk, lhs.ship_customer_sk, lhs.ship_cdemo_sk,
                  lhs.ship_hdemo_sk, lhs.ship_addr_sk, lhs.call_center_sk,
                  lhs.catalog_page_sk, lhs.ship_mode_sk, lhs.warehouse_sk,
                  lhs.sold_item_sk, lhs.promo_sk, lhs.order_number, lhs.pricing,
                  lhs.null_bitmap, lhs.is_returned) ==
         std::tie(rhs.sold_date_sk, rhs.sold_time_sk, rhs.ship_date_sk,
                  rhs.bill_customer_sk, rhs.bill_cdemo_sk, rhs.bill_hdemo_sk,
                  rhs.bill_addr_sk, rhs.ship_customer_sk, rhs.ship_cdemo_sk,
                  rhs.ship_hdemo_sk, rhs.ship_addr_sk, rhs.call_center_sk,
                  rhs.catalog_page_sk, rhs.ship_mode_sk, rhs.warehouse_sk,
                  rhs.sold_item_sk, rhs.promo_sk, rhs.order_number, rhs.pricing,
                  rhs.null_bitmap, rhs.is_returned);
}

bool operator==(const StoreReturnsRowData& lhs,
                const StoreReturnsRowData& rhs) {
  return std::tie(lhs.returned_date_sk, lhs.returned_time_sk, lhs.item_sk,
                  lhs.customer_sk, lhs.cdemo_sk, lhs.hdemo_sk, lhs.addr_sk,
                  lhs.store_sk, lhs.reason_sk, lhs.ticket_number, lhs.pricing,
                  lhs.null_bitmap) ==
         std::tie(rhs.returned_date_sk, rhs.returned_time_sk, rhs.item_sk,
                  rhs.customer_sk, rhs.cdemo_sk, rhs.hdemo_sk, rhs.addr_sk,
                  rhs.store_sk, rhs.reason_sk, rhs.ticket_number, rhs.pricing,
                  rhs.null_bitmap);
}

bool operator==(const WebReturnsRowData& lhs, const WebReturnsRowData& rhs) {
  return std::tie(lhs.returned_date_sk, lhs.returned_time_sk, lhs.item_sk,
                  lhs.refunded_customer_sk, lhs.refunded_cdemo_sk,
                  lhs.refunded_hdemo_sk, lhs.refunded_addr_sk,
                  lhs.returning_customer_sk, lhs.returning_cdemo_sk,
                  lhs.returning_hdemo_sk, lhs.returning_addr_sk,
                  lhs.web_page_sk, lhs.reason_sk, lhs.order_number, lhs.pricing,
                  lhs.null_bitmap) ==
         std::tie(rhs.returned_date_sk, rhs.returned_time_sk, rhs.item_sk,
                  rhs.refunded_customer_sk, rhs.refunded_cdemo_sk,
                  rhs.refunded_hdemo_sk, rhs.refunded_addr_sk,
                  rhs.returning_customer_sk, rhs.returning_cdemo_sk,
                  rhs.returning_hdemo_sk, rhs.returning_addr_sk,
                  rhs.web_page_sk, rhs.reason_sk, rhs.order_number, rhs.pricing,
                  rhs.null_bitmap);
}

bool operator==(const CatalogReturnsRowData& lhs,
                const CatalogReturnsRowData& rhs) {
  return std::tie(lhs.returned_date_sk, lhs.returned_time_sk, lhs.item_sk,
                  lhs.refunded_customer_sk, lhs.refunded_cdemo_sk,
                  lhs.refunded_hdemo_sk, lhs.refunded_addr_sk,
                  lhs.returning_customer_sk, lhs.returning_cdemo_sk,
                  lhs.returning_hdemo_sk, lhs.returning_addr_sk,
                  lhs.call_center_sk, lhs.catalog_page_sk, lhs.ship_mode_sk,
                  lhs.warehouse_sk, lhs.reason_sk, lhs.order_number,
                  lhs.pricing, lhs.null_bitmap) ==
         std::tie(rhs.returned_date_sk, rhs.returned_time_sk, rhs.item_sk,
                  rhs.refunded_customer_sk, rhs.refunded_cdemo_sk,
                  rhs.refunded_hdemo_sk, rhs.refunded_addr_sk,
                  rhs.returning_customer_sk, rhs.returning_cdemo_sk,
                  rhs.returning_hdemo_sk, rhs.returning_addr_sk,
                  rhs.call_center_sk, rhs.catalog_page_sk, rhs.ship_mode_sk,
                  rhs.warehouse_sk, rhs.reason_sk, rhs.order_number,
                  rhs.pricing, rhs.null_bitmap);
}

bool operator==(const StoreRowData& lhs, const StoreRowData& rhs) {
  return std::tie(lhs.store_sk, lhs.store_id, lhs.rec_start_date_id,
                  lhs.rec_end_date_id, lhs.closed_date_id, lhs.store_name,
                  lhs.employees, lhs.floor_space, lhs.hours, lhs.store_manager,
                  lhs.market_id, lhs.tax_percentage, lhs.geography_class,
                  lhs.market_desc, lhs.market_manager, lhs.division_id,
                  lhs.division_name, lhs.company_id, lhs.company_name,
                  lhs.address, lhs.null_bitmap) ==
         std::tie(rhs.store_sk, rhs.store_id, rhs.rec_start_date_id,
                  rhs.rec_end_date_id, rhs.closed_date_id, rhs.store_name,
                  rhs.employees, rhs.floor_space, rhs.hours, rhs.store_manager,
                  rhs.market_id, rhs.tax_percentage, rhs.geography_class,
                  rhs.market_desc, rhs.market_manager, rhs.division_id,
                  rhs.division_name, rhs.company_id, rhs.company_name,
                  rhs.address, rhs.null_bitmap);
}

bool operator==(const CallCenterRowData& lhs, const CallCenterRowData& rhs) {
  return std::tie(lhs.call_center_sk, lhs.call_center_id, lhs.rec_start_date_id,
                  lhs.rec_end_date_id, lhs.closed_date_id, lhs.open_date_id,
                  lhs.name, lhs.class_name, lhs.employees, lhs.sq_ft, lhs.hours,
                  lhs.manager, lhs.market_id, lhs.market_class, lhs.market_desc,
                  lhs.market_manager, lhs.division_id, lhs.division_name,
                  lhs.company, lhs.company_name, lhs.address,
                  lhs.tax_percentage, lhs.null_bitmap) ==
         std::tie(rhs.call_center_sk, rhs.call_center_id, rhs.rec_start_date_id,
                  rhs.rec_end_date_id, rhs.closed_date_id, rhs.open_date_id,
                  rhs.name, rhs.class_name, rhs.employees, rhs.sq_ft, rhs.hours,
                  rhs.manager, rhs.market_id, rhs.market_class, rhs.market_desc,
                  rhs.market_manager, rhs.division_id, rhs.division_name,
                  rhs.company, rhs.company_name, rhs.address,
                  rhs.tax_percentage, rhs.null_bitmap);
}

bool operator==(const WebSiteRowData& lhs, const WebSiteRowData& rhs) {
  return std::tie(lhs.site_sk, lhs.site_id, lhs.rec_start_date_id,
                  lhs.rec_end_date_id, lhs.name, lhs.open_date, lhs.close_date,
                  lhs.class_name, lhs.manager, lhs.market_id, lhs.market_class,
                  lhs.market_desc, lhs.market_manager, lhs.company_id,
                  lhs.company_name, lhs.address, lhs.tax_percentage,
                  lhs.null_bitmap) ==
         std::tie(rhs.site_sk, rhs.site_id, rhs.rec_start_date_id,
                  rhs.rec_end_date_id, rhs.name, rhs.open_date, rhs.close_date,
                  rhs.class_name, rhs.manager, rhs.market_id, rhs.market_class,
                  rhs.market_desc, rhs.market_manager, rhs.company_id,
                  rhs.company_name, rhs.address, rhs.tax_percentage,
                  rhs.null_bitmap);
}

bool operator==(const WebPageRowData& lhs, const WebPageRowData& rhs) {
  return std::tie(lhs.page_sk, lhs.page_id, lhs.rec_start_date_id,
                  lhs.rec_end_date_id, lhs.creation_date_sk, lhs.access_date_sk,
                  lhs.autogen_flag, lhs.customer_sk, lhs.url, lhs.type,
                  lhs.char_count, lhs.link_count, lhs.image_count,
                  lhs.max_ad_count, lhs.null_bitmap) ==
         std::tie(rhs.page_sk, rhs.page_id, rhs.rec_start_date_id,
                  rhs.rec_end_date_id, rhs.creation_date_sk, rhs.access_date_sk,
                  rhs.autogen_flag, rhs.customer_sk, rhs.url, rhs.type,
                  rhs.char_count, rhs.link_count, rhs.image_count,
                  rhs.max_ad_count, rhs.null_bitmap);
}

bool operator==(const ItemRowData& lhs, const ItemRowData& rhs) {
  return std::tie(lhs.item_sk, lhs.item_id, lhs.rec_start_date_id,
                  lhs.rec_end_date_id, lhs.item_desc, lhs.current_price,
                  lhs.wholesale_cost, lhs.brand_id, lhs.brand, lhs.class_id,
                  lhs.class_name, lhs.category_id, lhs.category,
                  lhs.manufact_id, lhs.manufact, lhs.size, lhs.formulation,
                  lhs.color, lhs.units, lhs.container, lhs.manager_id,
                  lhs.product_name, lhs.promo_sk, lhs.null_bitmap) ==
         std::tie(rhs.item_sk, rhs.item_id, rhs.rec_start_date_id,
                  rhs.rec_end_date_id, rhs.item_desc, rhs.current_price,
                  rhs.wholesale_cost, rhs.brand_id, rhs.brand, rhs.class_id,
                  rhs.class_name, rhs.category_id, rhs.category,
                  rhs.manufact_id, rhs.manufact, rhs.size, rhs.formulation,
                  rhs.color, rhs.units, rhs.container, rhs.manager_id,
                  rhs.product_name, rhs.promo_sk, rhs.null_bitmap);
}

}  // namespace benchgen::tpcds::internal

namespace {

constexpr double kScale = 1.0;

template <typename Generator, typename Row>
Row GenerateSequentialRow(Generator& generator, int64_t start_row) {
  Row row;
  for (int64_t i = 0; i <= start_row; ++i) {
    row = generator.GenerateRow(i + 1);
    generator.ConsumeRemainingSeedsForRow();
  }
  return row;
}

template <typename Generator, typename Row>
Row GenerateSkippedRow(Generator& generator, int64_t start_row) {
  generator.SkipRows(start_row);
  return generator.GenerateRow(start_row + 1);
}

template <typename Generator, typename Row, typename LastRowFn>
std::pair<Row, int64_t> GenerateSalesSequentialRow(Generator& generator,
                                                   int64_t start_row,
                                                   LastRowFn last_row_fn) {
  int64_t current_order = 0;
  int64_t order_number = 0;
  Row row;
  for (int64_t i = 0; i <= start_row; ++i) {
    order_number = current_order + 1;
    row = generator.GenerateRow(order_number);
    generator.ConsumeRemainingSeedsForRow();
    if (last_row_fn(generator)) {
      current_order = order_number;
    }
  }
  return {row, order_number};
}

TEST(RowGeneratorSkipRowsTest, CatalogPage) {
  constexpr int64_t kStartRow = 10;
  benchgen::tpcds::internal::CatalogPageRowGenerator sequential(
      kScale);
  auto expected =
      GenerateSequentialRow<benchgen::tpcds::internal::CatalogPageRowGenerator,
                            benchgen::tpcds::internal::CatalogPageRowData>(
          sequential, kStartRow);

  benchgen::tpcds::internal::CatalogPageRowGenerator skipped(kScale);
  auto actual =
      GenerateSkippedRow<benchgen::tpcds::internal::CatalogPageRowGenerator,
                         benchgen::tpcds::internal::CatalogPageRowData>(
          skipped, kStartRow);

  EXPECT_EQ(expected, actual);
}

TEST(RowGeneratorSkipRowsTest, CustomerAddress) {
  constexpr int64_t kStartRow = 10;
  benchgen::tpcds::internal::CustomerAddressRowGenerator sequential(
      kScale);
  auto expected = GenerateSequentialRow<
      benchgen::tpcds::internal::CustomerAddressRowGenerator,
      benchgen::tpcds::internal::CustomerAddressRowData>(sequential, kStartRow);

  benchgen::tpcds::internal::CustomerAddressRowGenerator skipped(
      kScale);
  auto actual =
      GenerateSkippedRow<benchgen::tpcds::internal::CustomerAddressRowGenerator,
                         benchgen::tpcds::internal::CustomerAddressRowData>(
          skipped, kStartRow);

  EXPECT_EQ(expected, actual);
}

TEST(RowGeneratorSkipRowsTest, Customer) {
  constexpr int64_t kStartRow = 10;
  benchgen::tpcds::internal::CustomerRowGenerator sequential(kScale);
  auto expected =
      GenerateSequentialRow<benchgen::tpcds::internal::CustomerRowGenerator,
                            benchgen::tpcds::internal::CustomerRowData>(
          sequential, kStartRow);

  benchgen::tpcds::internal::CustomerRowGenerator skipped(kScale);
  auto actual =
      GenerateSkippedRow<benchgen::tpcds::internal::CustomerRowGenerator,
                         benchgen::tpcds::internal::CustomerRowData>(skipped,
                                                                     kStartRow);

  EXPECT_EQ(expected, actual);
}

TEST(RowGeneratorSkipRowsTest, Warehouse) {
  constexpr int64_t kStartRow = 10;
  benchgen::tpcds::internal::WarehouseRowGenerator sequential(kScale);
  auto expected =
      GenerateSequentialRow<benchgen::tpcds::internal::WarehouseRowGenerator,
                            benchgen::tpcds::internal::WarehouseRowData>(
          sequential, kStartRow);

  benchgen::tpcds::internal::WarehouseRowGenerator skipped(kScale);
  auto actual =
      GenerateSkippedRow<benchgen::tpcds::internal::WarehouseRowGenerator,
                         benchgen::tpcds::internal::WarehouseRowData>(
          skipped, kStartRow);

  EXPECT_EQ(expected, actual);
}

TEST(RowGeneratorSkipRowsTest, Inventory) {
  constexpr int64_t kStartRow = 10;
  benchgen::tpcds::internal::InventoryRowGenerator sequential(kScale);
  auto expected =
      GenerateSequentialRow<benchgen::tpcds::internal::InventoryRowGenerator,
                            benchgen::tpcds::internal::InventoryRowData>(
          sequential, kStartRow);

  benchgen::tpcds::internal::InventoryRowGenerator skipped(kScale);
  auto actual =
      GenerateSkippedRow<benchgen::tpcds::internal::InventoryRowGenerator,
                         benchgen::tpcds::internal::InventoryRowData>(
          skipped, kStartRow);

  EXPECT_EQ(expected, actual);
}

TEST(RowGeneratorSkipRowsTest, ShipMode) {
  constexpr int64_t kStartRow = 10;
  benchgen::tpcds::internal::ShipModeRowGenerator sequential(kScale);
  auto expected =
      GenerateSequentialRow<benchgen::tpcds::internal::ShipModeRowGenerator,
                            benchgen::tpcds::internal::ShipModeRowData>(
          sequential, kStartRow);

  benchgen::tpcds::internal::ShipModeRowGenerator skipped(kScale);
  auto actual =
      GenerateSkippedRow<benchgen::tpcds::internal::ShipModeRowGenerator,
                         benchgen::tpcds::internal::ShipModeRowData>(skipped,
                                                                     kStartRow);

  EXPECT_EQ(expected, actual);
}

TEST(RowGeneratorSkipRowsTest, Promotion) {
  constexpr int64_t kStartRow = 10;
  benchgen::tpcds::internal::PromotionRowGenerator sequential(kScale);
  auto expected =
      GenerateSequentialRow<benchgen::tpcds::internal::PromotionRowGenerator,
                            benchgen::tpcds::internal::PromotionRowData>(
          sequential, kStartRow);

  benchgen::tpcds::internal::PromotionRowGenerator skipped(kScale);
  auto actual =
      GenerateSkippedRow<benchgen::tpcds::internal::PromotionRowGenerator,
                         benchgen::tpcds::internal::PromotionRowData>(
          skipped, kStartRow);

  EXPECT_EQ(expected, actual);
}

TEST(RowGeneratorSkipRowsTest, StoreRow) {
  constexpr int64_t kStartRow = 5;
  benchgen::tpcds::internal::StoreRowGenerator sequential(kScale);
  auto expected =
      GenerateSequentialRow<benchgen::tpcds::internal::StoreRowGenerator,
                            benchgen::tpcds::internal::StoreRowData>(sequential,
                                                                     kStartRow);

  benchgen::tpcds::internal::StoreRowGenerator skipped(kScale);
  auto actual = GenerateSkippedRow<benchgen::tpcds::internal::StoreRowGenerator,
                                   benchgen::tpcds::internal::StoreRowData>(
      skipped, kStartRow);

  EXPECT_EQ(expected, actual);
}

TEST(RowGeneratorSkipRowsTest, CallCenter) {
  constexpr int64_t kStartRow = 5;
  benchgen::tpcds::internal::CallCenterRowGenerator sequential(
      kScale);
  auto expected =
      GenerateSequentialRow<benchgen::tpcds::internal::CallCenterRowGenerator,
                            benchgen::tpcds::internal::CallCenterRowData>(
          sequential, kStartRow);

  benchgen::tpcds::internal::CallCenterRowGenerator skipped(kScale);
  auto actual =
      GenerateSkippedRow<benchgen::tpcds::internal::CallCenterRowGenerator,
                         benchgen::tpcds::internal::CallCenterRowData>(
          skipped, kStartRow);

  EXPECT_EQ(expected, actual);
}

TEST(RowGeneratorSkipRowsTest, CallCenterNoSkip) {
  constexpr int64_t kStartRow = 0;
  benchgen::tpcds::internal::CallCenterRowGenerator sequential(
      kScale);
  auto expected =
      GenerateSequentialRow<benchgen::tpcds::internal::CallCenterRowGenerator,
                            benchgen::tpcds::internal::CallCenterRowData>(
          sequential, kStartRow);

  benchgen::tpcds::internal::CallCenterRowGenerator skipped(kScale);
  auto actual =
      GenerateSkippedRow<benchgen::tpcds::internal::CallCenterRowGenerator,
                         benchgen::tpcds::internal::CallCenterRowData>(
          skipped, kStartRow);

  EXPECT_EQ(expected, actual);
}

TEST(RowGeneratorSkipRowsTest, WebSite) {
  constexpr int64_t kStartRow = 5;
  benchgen::tpcds::internal::WebSiteRowGenerator sequential(kScale);
  auto expected =
      GenerateSequentialRow<benchgen::tpcds::internal::WebSiteRowGenerator,
                            benchgen::tpcds::internal::WebSiteRowData>(
          sequential, kStartRow);

  benchgen::tpcds::internal::WebSiteRowGenerator skipped(kScale);
  auto actual =
      GenerateSkippedRow<benchgen::tpcds::internal::WebSiteRowGenerator,
                         benchgen::tpcds::internal::WebSiteRowData>(skipped,
                                                                    kStartRow);

  EXPECT_EQ(expected, actual);
}

TEST(RowGeneratorSkipRowsTest, WebPage) {
  constexpr int64_t kStartRow = 5;
  benchgen::tpcds::internal::WebPageRowGenerator sequential(kScale);
  auto expected =
      GenerateSequentialRow<benchgen::tpcds::internal::WebPageRowGenerator,
                            benchgen::tpcds::internal::WebPageRowData>(
          sequential, kStartRow);

  benchgen::tpcds::internal::WebPageRowGenerator skipped(kScale);
  auto actual =
      GenerateSkippedRow<benchgen::tpcds::internal::WebPageRowGenerator,
                         benchgen::tpcds::internal::WebPageRowData>(skipped,
                                                                    kStartRow);

  EXPECT_EQ(expected, actual);
}

TEST(RowGeneratorSkipRowsTest, Item) {
  constexpr int64_t kStartRow = 5;
  benchgen::tpcds::internal::ItemRowGenerator sequential(kScale);
  auto expected =
      GenerateSequentialRow<benchgen::tpcds::internal::ItemRowGenerator,
                            benchgen::tpcds::internal::ItemRowData>(sequential,
                                                                    kStartRow);

  benchgen::tpcds::internal::ItemRowGenerator skipped(kScale);
  auto actual = GenerateSkippedRow<benchgen::tpcds::internal::ItemRowGenerator,
                                   benchgen::tpcds::internal::ItemRowData>(
      skipped, kStartRow);

  EXPECT_EQ(expected, actual);
}

TEST(RowGeneratorSkipRowsTest, StoreSalesNoSkip) {
  constexpr int64_t kStartRow = 0;
  benchgen::tpcds::internal::StoreSalesRowGenerator sequential(
      kScale);
  auto [expected, order_number] = GenerateSalesSequentialRow<
      benchgen::tpcds::internal::StoreSalesRowGenerator,
      benchgen::tpcds::internal::StoreSalesRowData>(
      sequential, kStartRow,
      [](const benchgen::tpcds::internal::StoreSalesRowGenerator& generator) {
        return generator.LastRowInTicket();
      });

  benchgen::tpcds::internal::StoreSalesRowGenerator skipped(kScale);
  skipped.SkipRows(kStartRow);
  auto actual = skipped.GenerateRow(order_number);

  EXPECT_EQ(expected, actual);
}

TEST(RowGeneratorSkipRowsTest, StoreSales) {
  constexpr int64_t kStartRow = 1;
  benchgen::tpcds::internal::StoreSalesRowGenerator sequential(
      kScale);
  auto [expected, order_number] = GenerateSalesSequentialRow<
      benchgen::tpcds::internal::StoreSalesRowGenerator,
      benchgen::tpcds::internal::StoreSalesRowData>(
      sequential, kStartRow,
      [](const benchgen::tpcds::internal::StoreSalesRowGenerator& generator) {
        return generator.LastRowInTicket();
      });

  benchgen::tpcds::internal::StoreSalesRowGenerator skipped(kScale);
  skipped.SkipRows(kStartRow);
  auto actual = skipped.GenerateRow(order_number);

  EXPECT_EQ(expected, actual);
}

TEST(RowGeneratorSkipRowsTest, StoreSalesAcrossTickets) {
  constexpr int64_t kStartRow = 20;
  benchgen::tpcds::internal::StoreSalesRowGenerator sequential(
      kScale);
  auto [expected, order_number] = GenerateSalesSequentialRow<
      benchgen::tpcds::internal::StoreSalesRowGenerator,
      benchgen::tpcds::internal::StoreSalesRowData>(
      sequential, kStartRow,
      [](const benchgen::tpcds::internal::StoreSalesRowGenerator& generator) {
        return generator.LastRowInTicket();
      });

  benchgen::tpcds::internal::StoreSalesRowGenerator skipped(kScale);
  skipped.SkipRows(kStartRow);
  auto actual = skipped.GenerateRow(order_number);

  EXPECT_EQ(expected, actual);
}

TEST(RowGeneratorSkipRowsTest, WebSales) {
  constexpr int64_t kStartRow = 1;
  benchgen::tpcds::internal::WebSalesRowGenerator sequential(kScale);
  auto [expected, order_number] = GenerateSalesSequentialRow<
      benchgen::tpcds::internal::WebSalesRowGenerator,
      benchgen::tpcds::internal::WebSalesRowData>(
      sequential, kStartRow,
      [](const benchgen::tpcds::internal::WebSalesRowGenerator& generator) {
        return generator.LastRowInOrder();
      });

  benchgen::tpcds::internal::WebSalesRowGenerator skipped(kScale);
  skipped.SkipRows(kStartRow);
  auto actual = skipped.GenerateRow(order_number);

  EXPECT_EQ(expected, actual);
}

TEST(RowGeneratorSkipRowsTest, WebSalesAcrossOrders) {
  constexpr int64_t kStartRow = 20;
  benchgen::tpcds::internal::WebSalesRowGenerator sequential(kScale);
  auto [expected, order_number] = GenerateSalesSequentialRow<
      benchgen::tpcds::internal::WebSalesRowGenerator,
      benchgen::tpcds::internal::WebSalesRowData>(
      sequential, kStartRow,
      [](const benchgen::tpcds::internal::WebSalesRowGenerator& generator) {
        return generator.LastRowInOrder();
      });

  benchgen::tpcds::internal::WebSalesRowGenerator skipped(kScale);
  skipped.SkipRows(kStartRow);
  auto actual = skipped.GenerateRow(order_number);

  EXPECT_EQ(expected, actual);
}

TEST(RowGeneratorSkipRowsTest, CatalogSales) {
  constexpr int64_t kStartRow = 1;
  benchgen::tpcds::internal::CatalogSalesRowGenerator sequential(
      kScale);
  auto [expected, order_number] = GenerateSalesSequentialRow<
      benchgen::tpcds::internal::CatalogSalesRowGenerator,
      benchgen::tpcds::internal::CatalogSalesRowData>(
      sequential, kStartRow,
      [](const benchgen::tpcds::internal::CatalogSalesRowGenerator& generator) {
        return generator.LastRowInOrder();
      });

  benchgen::tpcds::internal::CatalogSalesRowGenerator skipped(kScale);
  skipped.SkipRows(kStartRow);
  auto actual = skipped.GenerateRow(order_number);

  EXPECT_EQ(expected, actual);
}

TEST(RowGeneratorSkipRowsTest, CatalogSalesAcrossOrders) {
  constexpr int64_t kStartRow = 20;
  benchgen::tpcds::internal::CatalogSalesRowGenerator sequential(
      kScale);
  auto [expected, order_number] = GenerateSalesSequentialRow<
      benchgen::tpcds::internal::CatalogSalesRowGenerator,
      benchgen::tpcds::internal::CatalogSalesRowData>(
      sequential, kStartRow,
      [](const benchgen::tpcds::internal::CatalogSalesRowGenerator& generator) {
        return generator.LastRowInOrder();
      });

  benchgen::tpcds::internal::CatalogSalesRowGenerator skipped(kScale);
  skipped.SkipRows(kStartRow);
  auto actual = skipped.GenerateRow(order_number);

  EXPECT_EQ(expected, actual);
}

TEST(RowGeneratorSkipRowsTest, StoreReturns) {
  constexpr int64_t kStartRow = 5;
  benchgen::tpcds::internal::StoreReturnsRowGenerator sequential(
      kScale);
  auto expected =
      GenerateSequentialRow<benchgen::tpcds::internal::StoreReturnsRowGenerator,
                            benchgen::tpcds::internal::StoreReturnsRowData>(
          sequential, kStartRow);

  benchgen::tpcds::internal::StoreReturnsRowGenerator skipped(kScale);
  skipped.SkipRows(kStartRow);
  auto actual = skipped.GenerateRow(kStartRow + 1);

  EXPECT_EQ(expected, actual);
}

TEST(RowGeneratorSkipRowsTest, WebReturns) {
  constexpr int64_t kStartRow = 5;
  benchgen::tpcds::internal::WebReturnsRowGenerator sequential(
      kScale);
  auto expected =
      GenerateSequentialRow<benchgen::tpcds::internal::WebReturnsRowGenerator,
                            benchgen::tpcds::internal::WebReturnsRowData>(
          sequential, kStartRow);

  benchgen::tpcds::internal::WebReturnsRowGenerator skipped(kScale);
  skipped.SkipRows(kStartRow);
  auto actual = skipped.GenerateRow(kStartRow + 1);

  EXPECT_EQ(expected, actual);
}

TEST(RowGeneratorSkipRowsTest, CatalogReturns) {
  constexpr int64_t kStartRow = 5;
  benchgen::tpcds::internal::CatalogReturnsRowGenerator sequential(
      kScale);
  auto expected = GenerateSequentialRow<
      benchgen::tpcds::internal::CatalogReturnsRowGenerator,
      benchgen::tpcds::internal::CatalogReturnsRowData>(sequential, kStartRow);

  benchgen::tpcds::internal::CatalogReturnsRowGenerator skipped(
      kScale);
  skipped.SkipRows(kStartRow);
  auto actual = skipped.GenerateRow(kStartRow + 1);

  EXPECT_EQ(expected, actual);
}

}  // namespace
