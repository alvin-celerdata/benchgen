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

#include <exception>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include "benchgen/benchmark_suite.h"
#include "benchgen/table.h"
#include "distribution/scaling.h"
#include "generators/catalog_returns_generator.h"
#include "generators/catalog_sales_generator.h"
#include "generators/store_returns_generator.h"
#include "generators/store_sales_generator.h"
#include "generators/web_returns_generator.h"
#include "generators/web_sales_generator.h"

namespace benchgen {
namespace {

class TpcdsSuite final : public BenchmarkSuite {
 public:
  SuiteId suite_id() const override { return SuiteId::kTpcds; }
  std::string_view name() const override { return "tpcds"; }

  int table_count() const override {
    return static_cast<int>(tpcds::TableId::kTableCount);
  }

  std::string_view TableName(int table_index) const override {
    if (table_index < 0 || table_index >= table_count()) {
      return std::string_view();
    }
    return tpcds::TableIdToString(static_cast<tpcds::TableId>(table_index));
  }

  arrow::Status MakeIterator(
      std::string_view table_name, GeneratorOptions options,
      std::unique_ptr<RecordBatchIterator>* out) const override {
    return MakeRecordBatchIterator(SuiteId::kTpcds, table_name,
                                   std::move(options), out);
  }

  arrow::Status ResolveTableRowCount(std::string_view table_name,
                                     const GeneratorOptions& options,
                                     int64_t* out,
                                     bool* is_known) const override {
    if (out == nullptr || is_known == nullptr) {
      return arrow::Status::Invalid(
          "row count output parameters must not be null");
    }
    *is_known = false;

    tpcds::TableId table_id;
    if (!tpcds::TableIdFromString(table_name, &table_id)) {
      return arrow::Status::Invalid("Unknown TPC-DS table: ",
                                    std::string(table_name));
    }

    try {
      switch (table_id) {
        case tpcds::TableId::kCatalogSales:
          *out =
              tpcds::CatalogSalesGenerator::TotalRows(options.scale_factor);
          *is_known = true;
          return arrow::Status::OK();
        case tpcds::TableId::kCatalogReturns:
          *out =
              tpcds::CatalogReturnsGenerator::TotalRows(options.scale_factor);
          *is_known = true;
          return arrow::Status::OK();
        case tpcds::TableId::kStoreSales:
          *out = tpcds::StoreSalesGenerator::TotalRows(options.scale_factor);
          *is_known = true;
          return arrow::Status::OK();
        case tpcds::TableId::kStoreReturns:
          *out = tpcds::StoreReturnsGenerator::TotalRows(options.scale_factor);
          *is_known = true;
          return arrow::Status::OK();
        case tpcds::TableId::kWebSales:
          *out = tpcds::WebSalesGenerator::TotalRows(options.scale_factor);
          *is_known = true;
          return arrow::Status::OK();
        case tpcds::TableId::kWebReturns:
          *out = tpcds::WebReturnsGenerator::TotalRows(options.scale_factor);
          *is_known = true;
          return arrow::Status::OK();
        case tpcds::TableId::kCallCenter:
        case tpcds::TableId::kCatalogPage:
        case tpcds::TableId::kCustomer:
        case tpcds::TableId::kCustomerAddress:
        case tpcds::TableId::kCustomerDemographics:
        case tpcds::TableId::kDateDim:
        case tpcds::TableId::kHouseholdDemographics:
        case tpcds::TableId::kIncomeBand:
        case tpcds::TableId::kInventory:
        case tpcds::TableId::kItem:
        case tpcds::TableId::kPromotion:
        case tpcds::TableId::kReason:
        case tpcds::TableId::kShipMode:
        case tpcds::TableId::kStore:
        case tpcds::TableId::kTimeDim:
        case tpcds::TableId::kWarehouse:
        case tpcds::TableId::kWebPage:
        case tpcds::TableId::kWebSite:
        case tpcds::TableId::kTableCount:
          break;
      }
      tpcds::internal::Scaling scaling(options.scale_factor);
      *out = scaling.RowCount(table_id);
      *is_known = true;
      return arrow::Status::OK();
    } catch (const std::exception& e) {
      return arrow::Status::Invalid("Failed to resolve TPC-DS row counts: ",
                                    e.what());
    }
  }
};

}  // namespace

std::unique_ptr<BenchmarkSuite> MakeTpcdsBenchmarkSuite() {
  return std::make_unique<TpcdsSuite>();
}

}  // namespace benchgen
