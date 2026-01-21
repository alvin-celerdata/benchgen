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

#include "benchgen/record_batch_iterator_factory.h"

#include <string>
#include <utility>

#include "ssb/generators/customer_generator.h"
#include "ssb/generators/date_generator.h"
#include "ssb/generators/lineorder_generator.h"
#include "ssb/generators/part_generator.h"
#include "ssb/generators/supplier_generator.h"
#include "tpcds/generators/call_center_generator.h"
#include "tpcds/generators/catalog_page_generator.h"
#include "tpcds/generators/catalog_returns_generator.h"
#include "tpcds/generators/catalog_sales_generator.h"
#include "tpcds/generators/customer_address_generator.h"
#include "tpcds/generators/customer_demographics_generator.h"
#include "tpcds/generators/customer_generator.h"
#include "tpcds/generators/date_dim_generator.h"
#include "tpcds/generators/household_demographics_generator.h"
#include "tpcds/generators/income_band_generator.h"
#include "tpcds/generators/inventory_generator.h"
#include "tpcds/generators/item_generator.h"
#include "tpcds/generators/promotion_generator.h"
#include "tpcds/generators/reason_generator.h"
#include "tpcds/generators/ship_mode_generator.h"
#include "tpcds/generators/store_generator.h"
#include "tpcds/generators/store_returns_generator.h"
#include "tpcds/generators/store_sales_generator.h"
#include "tpcds/generators/time_dim_generator.h"
#include "tpcds/generators/warehouse_generator.h"
#include "tpcds/generators/web_page_generator.h"
#include "tpcds/generators/web_returns_generator.h"
#include "tpcds/generators/web_sales_generator.h"
#include "tpcds/generators/web_site_generator.h"
#include "tpch/generators/customer_generator.h"
#include "tpch/generators/lineitem_generator.h"
#include "tpch/generators/nation_generator.h"
#include "tpch/generators/orders_generator.h"
#include "tpch/generators/part_generator.h"
#include "tpch/generators/partsupp_generator.h"
#include "tpch/generators/region_generator.h"
#include "tpch/generators/supplier_generator.h"

namespace benchgen {
namespace {

arrow::Status MakeTpchRecordBatchIterator(
    tpch::TableId table, GeneratorOptions options,
    std::unique_ptr<RecordBatchIterator>* out) {
  if (out == nullptr) {
    return arrow::Status::Invalid("out iterator must not be null");
  }

  switch (table) {
    case tpch::TableId::kPart: {
      auto iter = std::make_unique<tpch::PartGenerator>(std::move(options));
      ARROW_RETURN_NOT_OK(iter->Init());
      *out = std::move(iter);
      return arrow::Status::OK();
    }
    case tpch::TableId::kPartSupp: {
      auto iter = std::make_unique<tpch::PartSuppGenerator>(std::move(options));
      ARROW_RETURN_NOT_OK(iter->Init());
      *out = std::move(iter);
      return arrow::Status::OK();
    }
    case tpch::TableId::kSupplier: {
      auto iter = std::make_unique<tpch::SupplierGenerator>(std::move(options));
      ARROW_RETURN_NOT_OK(iter->Init());
      *out = std::move(iter);
      return arrow::Status::OK();
    }
    case tpch::TableId::kCustomer: {
      auto iter = std::make_unique<tpch::CustomerGenerator>(std::move(options));
      ARROW_RETURN_NOT_OK(iter->Init());
      *out = std::move(iter);
      return arrow::Status::OK();
    }
    case tpch::TableId::kOrders: {
      auto iter = std::make_unique<tpch::OrdersGenerator>(std::move(options));
      ARROW_RETURN_NOT_OK(iter->Init());
      *out = std::move(iter);
      return arrow::Status::OK();
    }
    case tpch::TableId::kLineItem: {
      auto iter = std::make_unique<tpch::LineItemGenerator>(std::move(options));
      ARROW_RETURN_NOT_OK(iter->Init());
      *out = std::move(iter);
      return arrow::Status::OK();
    }
    case tpch::TableId::kNation: {
      auto iter = std::make_unique<tpch::NationGenerator>(std::move(options));
      ARROW_RETURN_NOT_OK(iter->Init());
      *out = std::move(iter);
      return arrow::Status::OK();
    }
    case tpch::TableId::kRegion: {
      auto iter = std::make_unique<tpch::RegionGenerator>(std::move(options));
      ARROW_RETURN_NOT_OK(iter->Init());
      *out = std::move(iter);
      return arrow::Status::OK();
    }
    case tpch::TableId::kTableCount:
      break;
  }

  return arrow::Status::Invalid("unknown table id");
}

arrow::Status MakeTpcdsRecordBatchIterator(
    tpcds::TableId table, GeneratorOptions options,
    std::unique_ptr<RecordBatchIterator>* out) {
  if (out == nullptr) {
    return arrow::Status::Invalid("out iterator must not be null");
  }

  switch (table) {
    case tpcds::TableId::kCustomer: {
      auto iter =
          std::make_unique<tpcds::CustomerGenerator>(std::move(options));
      ARROW_RETURN_NOT_OK(iter->Init());
      *out = std::move(iter);
      return arrow::Status::OK();
    }
    case tpcds::TableId::kCustomerAddress: {
      auto iter =
          std::make_unique<tpcds::CustomerAddressGenerator>(std::move(options));
      ARROW_RETURN_NOT_OK(iter->Init());
      *out = std::move(iter);
      return arrow::Status::OK();
    }
    case tpcds::TableId::kCustomerDemographics: {
      auto iter = std::make_unique<tpcds::CustomerDemographicsGenerator>(
          std::move(options));
      ARROW_RETURN_NOT_OK(iter->Init());
      *out = std::move(iter);
      return arrow::Status::OK();
    }
    case tpcds::TableId::kDateDim: {
      auto iter = std::make_unique<tpcds::DateDimGenerator>(std::move(options));
      ARROW_RETURN_NOT_OK(iter->Init());
      *out = std::move(iter);
      return arrow::Status::OK();
    }
    case tpcds::TableId::kCallCenter: {
      auto iter =
          std::make_unique<tpcds::CallCenterGenerator>(std::move(options));
      *out = std::move(iter);
      return arrow::Status::OK();
    }
    case tpcds::TableId::kCatalogPage: {
      auto iter =
          std::make_unique<tpcds::CatalogPageGenerator>(std::move(options));
      *out = std::move(iter);
      return arrow::Status::OK();
    }
    case tpcds::TableId::kCatalogReturns: {
      auto iter =
          std::make_unique<tpcds::CatalogReturnsGenerator>(std::move(options));
      *out = std::move(iter);
      return arrow::Status::OK();
    }
    case tpcds::TableId::kCatalogSales: {
      auto iter =
          std::make_unique<tpcds::CatalogSalesGenerator>(std::move(options));
      *out = std::move(iter);
      return arrow::Status::OK();
    }
    case tpcds::TableId::kHouseholdDemographics: {
      auto iter = std::make_unique<tpcds::HouseholdDemographicsGenerator>(
          std::move(options));
      ARROW_RETURN_NOT_OK(iter->Init());
      *out = std::move(iter);
      return arrow::Status::OK();
    }
    case tpcds::TableId::kTimeDim: {
      auto iter = std::make_unique<tpcds::TimeDimGenerator>(std::move(options));
      ARROW_RETURN_NOT_OK(iter->Init());
      *out = std::move(iter);
      return arrow::Status::OK();
    }
    case tpcds::TableId::kIncomeBand: {
      auto iter =
          std::make_unique<tpcds::IncomeBandGenerator>(std::move(options));
      ARROW_RETURN_NOT_OK(iter->Init());
      *out = std::move(iter);
      return arrow::Status::OK();
    }
    case tpcds::TableId::kReason: {
      auto iter = std::make_unique<tpcds::ReasonGenerator>(std::move(options));
      ARROW_RETURN_NOT_OK(iter->Init());
      *out = std::move(iter);
      return arrow::Status::OK();
    }
    case tpcds::TableId::kShipMode: {
      auto iter =
          std::make_unique<tpcds::ShipModeGenerator>(std::move(options));
      ARROW_RETURN_NOT_OK(iter->Init());
      *out = std::move(iter);
      return arrow::Status::OK();
    }
    case tpcds::TableId::kInventory: {
      auto iter =
          std::make_unique<tpcds::InventoryGenerator>(std::move(options));
      *out = std::move(iter);
      return arrow::Status::OK();
    }
    case tpcds::TableId::kItem: {
      auto iter = std::make_unique<tpcds::ItemGenerator>(std::move(options));
      *out = std::move(iter);
      return arrow::Status::OK();
    }
    case tpcds::TableId::kPromotion: {
      auto iter =
          std::make_unique<tpcds::PromotionGenerator>(std::move(options));
      *out = std::move(iter);
      return arrow::Status::OK();
    }
    case tpcds::TableId::kStore: {
      auto iter = std::make_unique<tpcds::StoreGenerator>(std::move(options));
      *out = std::move(iter);
      return arrow::Status::OK();
    }
    case tpcds::TableId::kStoreReturns: {
      auto iter =
          std::make_unique<tpcds::StoreReturnsGenerator>(std::move(options));
      *out = std::move(iter);
      return arrow::Status::OK();
    }
    case tpcds::TableId::kStoreSales: {
      auto iter =
          std::make_unique<tpcds::StoreSalesGenerator>(std::move(options));
      *out = std::move(iter);
      return arrow::Status::OK();
    }
    case tpcds::TableId::kWarehouse: {
      auto iter =
          std::make_unique<tpcds::WarehouseGenerator>(std::move(options));
      *out = std::move(iter);
      return arrow::Status::OK();
    }
    case tpcds::TableId::kWebPage: {
      auto iter = std::make_unique<tpcds::WebPageGenerator>(std::move(options));
      *out = std::move(iter);
      return arrow::Status::OK();
    }
    case tpcds::TableId::kWebReturns: {
      auto iter =
          std::make_unique<tpcds::WebReturnsGenerator>(std::move(options));
      *out = std::move(iter);
      return arrow::Status::OK();
    }
    case tpcds::TableId::kWebSales: {
      auto iter =
          std::make_unique<tpcds::WebSalesGenerator>(std::move(options));
      *out = std::move(iter);
      return arrow::Status::OK();
    }
    case tpcds::TableId::kWebSite: {
      auto iter = std::make_unique<tpcds::WebSiteGenerator>(std::move(options));
      *out = std::move(iter);
      return arrow::Status::OK();
    }
    case tpcds::TableId::kTableCount:
      break;
  }

  return arrow::Status::Invalid("unknown table id");
}

arrow::Status UnsupportedSsbTable(ssb::TableId table,
                                  std::unique_ptr<RecordBatchIterator>* out) {
  if (out != nullptr) {
    out->reset();
  }
  return arrow::Status::NotImplemented(
      std::string("generator for table ") +
      std::string(ssb::TableIdToString(table)) + " is not implemented");
}

arrow::Status MakeSsbRecordBatchIterator(
    ssb::TableId table, GeneratorOptions options,
    std::unique_ptr<RecordBatchIterator>* out) {
  if (out == nullptr) {
    return arrow::Status::Invalid("out iterator must not be null");
  }

  switch (table) {
    case ssb::TableId::kCustomer: {
      auto iter = std::make_unique<ssb::CustomerGenerator>(std::move(options));
      ARROW_RETURN_NOT_OK(iter->Init());
      *out = std::move(iter);
      return arrow::Status::OK();
    }
    case ssb::TableId::kPart: {
      auto iter = std::make_unique<ssb::PartGenerator>(std::move(options));
      ARROW_RETURN_NOT_OK(iter->Init());
      *out = std::move(iter);
      return arrow::Status::OK();
    }
    case ssb::TableId::kSupplier: {
      auto iter = std::make_unique<ssb::SupplierGenerator>(std::move(options));
      ARROW_RETURN_NOT_OK(iter->Init());
      *out = std::move(iter);
      return arrow::Status::OK();
    }
    case ssb::TableId::kDate: {
      auto iter = std::make_unique<ssb::DateGenerator>(std::move(options));
      ARROW_RETURN_NOT_OK(iter->Init());
      *out = std::move(iter);
      return arrow::Status::OK();
    }
    case ssb::TableId::kLineorder: {
      auto iter = std::make_unique<ssb::LineorderGenerator>(std::move(options));
      ARROW_RETURN_NOT_OK(iter->Init());
      *out = std::move(iter);
      return arrow::Status::OK();
    }
    case ssb::TableId::kTableCount:
      break;
  }

  return UnsupportedSsbTable(table, out);
}

}  // namespace

arrow::Status MakeRecordBatchIterator(
    SuiteId suite, std::string_view table_name, GeneratorOptions options,
    std::unique_ptr<RecordBatchIterator>* out) {
  if (out == nullptr) {
    return arrow::Status::Invalid("out iterator must not be null");
  }

  switch (suite) {
    case SuiteId::kTpch: {
      tpch::TableId table;
      if (!tpch::TableIdFromString(table_name, &table)) {
        out->reset();
        return arrow::Status::Invalid("unknown table name: " +
                                      std::string(table_name));
      }
      return MakeTpchRecordBatchIterator(table, std::move(options), out);
    }
    case SuiteId::kTpcds: {
      tpcds::TableId table;
      if (!tpcds::TableIdFromString(table_name, &table)) {
        out->reset();
        return arrow::Status::Invalid("unknown table name: " +
                                      std::string(table_name));
      }
      return MakeTpcdsRecordBatchIterator(table, std::move(options), out);
    }
    case SuiteId::kSsb: {
      ssb::TableId table;
      if (!ssb::TableIdFromString(table_name, &table)) {
        out->reset();
        return arrow::Status::Invalid("unknown table name: " +
                                      std::string(table_name));
      }
      return MakeSsbRecordBatchIterator(table, std::move(options), out);
    }
    case SuiteId::kUnknown:
      break;
  }

  out->reset();
  return arrow::Status::Invalid("unknown suite id");
}

}  // namespace benchgen
