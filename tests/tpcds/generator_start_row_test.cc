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

#include <algorithm>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "benchgen/arrow_compat.h"
#include "benchgen/generator_options.h"
#include "benchgen/record_batch_iterator.h"
#include "generators/call_center_generator.h"
#include "generators/catalog_page_generator.h"
#include "generators/catalog_returns_generator.h"
#include "generators/catalog_sales_generator.h"
#include "generators/customer_address_generator.h"
#include "generators/customer_demographics_generator.h"
#include "generators/customer_generator.h"
#include "generators/date_dim_generator.h"
#include "generators/household_demographics_generator.h"
#include "generators/income_band_generator.h"
#include "generators/inventory_generator.h"
#include "generators/item_generator.h"
#include "generators/promotion_generator.h"
#include "generators/reason_generator.h"
#include "generators/ship_mode_generator.h"
#include "generators/store_generator.h"
#include "generators/store_returns_generator.h"
#include "generators/store_sales_generator.h"
#include "generators/time_dim_generator.h"
#include "generators/warehouse_generator.h"
#include "generators/web_page_generator.h"
#include "generators/web_returns_generator.h"
#include "generators/web_sales_generator.h"
#include "generators/web_site_generator.h"

namespace {

void AppendValue(std::string* out, const std::shared_ptr<arrow::Array>& array,
                 int64_t row) {
  if (array->IsNull(row)) {
    return;
  }

  switch (array->type_id()) {
    case arrow::Type::INT32: {
      auto values = std::static_pointer_cast<arrow::Int32Array>(array);
      out->append(std::to_string(values->Value(row)));
      return;
    }
    case arrow::Type::INT64: {
      auto values = std::static_pointer_cast<arrow::Int64Array>(array);
      out->append(std::to_string(values->Value(row)));
      return;
    }
    case arrow::Type::BOOL: {
      auto values = std::static_pointer_cast<arrow::BooleanArray>(array);
      out->push_back(values->Value(row) ? '1' : '0');
      return;
    }
    case arrow::Type::FLOAT: {
      auto values = std::static_pointer_cast<arrow::FloatArray>(array);
      out->append(std::to_string(values->Value(row)));
      return;
    }
    case arrow::Type::STRING: {
      auto values = std::static_pointer_cast<arrow::StringArray>(array);
      auto view = values->GetView(row);
      out->append(view.data(), view.size());
      return;
    }
    case arrow::Type::DATE32: {
      auto values = std::static_pointer_cast<arrow::Date32Array>(array);
      out->append(std::to_string(values->Value(row)));
      return;
    }
    case arrow::Type::DECIMAL32: {
      auto values = std::static_pointer_cast<arrow::Decimal32Array>(array);
      out->append(values->FormatValue(row));
      return;
    }
    case arrow::Type::DECIMAL64: {
      auto values = std::static_pointer_cast<arrow::Decimal64Array>(array);
      out->append(values->FormatValue(row));
      return;
    }
    case arrow::Type::DECIMAL128: {
      auto values = std::static_pointer_cast<arrow::Decimal128Array>(array);
      out->append(values->FormatValue(row));
      return;
    }
    case arrow::Type::DECIMAL256: {
      auto values = std::static_pointer_cast<arrow::Decimal256Array>(array);
      out->append(values->FormatValue(row));
      return;
    }
    default:
      ADD_FAILURE() << "Unhandled Arrow type: " << array->type()->ToString();
      return;
  }
}

std::vector<std::string> CollectRows(benchgen::RecordBatchIterator* iterator) {
  std::vector<std::string> rows;
  std::shared_ptr<arrow::RecordBatch> batch;
  while (true) {
    auto status = iterator->Next(&batch);
    EXPECT_TRUE(status.ok()) << status.ToString();
    if (!status.ok() || batch == nullptr) {
      break;
    }
    for (int64_t row = 0; row < batch->num_rows(); ++row) {
      std::string line;
      for (int col = 0; col < batch->num_columns(); ++col) {
        AppendValue(&line, batch->column(col), row);
        line.push_back('|');
      }
      rows.push_back(std::move(line));
    }
  }
  return rows;
}

template <typename T, typename = void>
struct HasInit : std::false_type {};

template <typename T>
struct HasInit<T, std::void_t<decltype(std::declval<T&>().Init())>>
    : std::true_type {};

template <typename Generator>
arrow::Status InitGeneratorIfSupported(Generator* generator) {
  if constexpr (HasInit<Generator>::value) {
    return generator->Init();
  }
  return arrow::Status::OK();
}

struct StartRowRange {
  int64_t start_row = 0;
  int64_t row_count = 0;
};

StartRowRange ChooseRange(int64_t total_rows) {
  StartRowRange range;
  if (total_rows <= 0) {
    return range;
  }
  if (total_rows == 1) {
    range.start_row = 0;
    range.row_count = 1;
    return range;
  }
  range.start_row = std::min<int64_t>(25, total_rows / 3);
  if (range.start_row < 1) {
    range.start_row = 1;
  }
  range.row_count = std::min<int64_t>(10, total_rows - range.start_row);
  if (range.row_count <= 0) {
    range.start_row = total_rows - 1;
    range.row_count = 1;
  }
  return range;
}

template <typename Generator>
void ExpectStartRowMatches(double scale_factor, int64_t total_rows) {
  StartRowRange range = ChooseRange(total_rows);
  if (range.row_count <= 0) {
    GTEST_SKIP() << "Not enough rows to validate start_row behavior";
  }

  benchgen::GeneratorOptions base_options;
  base_options.scale_factor = scale_factor;
  base_options.chunk_size = 11;
  base_options.start_row = 0;
  base_options.row_count = range.start_row + range.row_count;
  Generator baseline(base_options);
  auto baseline_status = InitGeneratorIfSupported(&baseline);
  ASSERT_TRUE(baseline_status.ok()) << baseline_status.ToString();
  std::vector<std::string> baseline_rows = CollectRows(&baseline);
  ASSERT_GE(static_cast<int64_t>(baseline_rows.size()),
            range.start_row + range.row_count);
  std::vector<std::string> expected(
      baseline_rows.begin() + range.start_row,
      baseline_rows.begin() + range.start_row + range.row_count);

  benchgen::GeneratorOptions skip_options = base_options;
  skip_options.chunk_size = 4;
  skip_options.start_row = range.start_row;
  skip_options.row_count = range.row_count;
  Generator skipped(skip_options);
  auto skipped_status = InitGeneratorIfSupported(&skipped);
  ASSERT_TRUE(skipped_status.ok()) << skipped_status.ToString();
  std::vector<std::string> actual_rows = CollectRows(&skipped);

  EXPECT_EQ(static_cast<int64_t>(actual_rows.size()), range.row_count);
  EXPECT_EQ(actual_rows, expected);
}

}  // namespace

TEST(GeneratorStartRowTest, StoreSales) {
  constexpr double kScaleFactor = 0.1;
  int64_t total = benchgen::tpcds::StoreSalesGenerator::TotalRows(kScaleFactor);
  ExpectStartRowMatches<benchgen::tpcds::StoreSalesGenerator>(kScaleFactor,
                                                              total);
}

TEST(GeneratorStartRowTest, WebSales) {
  constexpr double kScaleFactor = 0.1;
  int64_t total = benchgen::tpcds::WebSalesGenerator::TotalRows(kScaleFactor);
  ExpectStartRowMatches<benchgen::tpcds::WebSalesGenerator>(kScaleFactor,
                                                            total);
}

TEST(GeneratorStartRowTest, CatalogSales) {
  constexpr double kScaleFactor = 0.1;
  int64_t total =
      benchgen::tpcds::CatalogSalesGenerator::TotalRows(kScaleFactor);
  ExpectStartRowMatches<benchgen::tpcds::CatalogSalesGenerator>(kScaleFactor,
                                                                total);
}

TEST(GeneratorStartRowTest, StoreReturns) {
  constexpr double kScaleFactor = 0.1;
  int64_t total =
      benchgen::tpcds::StoreReturnsGenerator::TotalRows(kScaleFactor);
  ExpectStartRowMatches<benchgen::tpcds::StoreReturnsGenerator>(kScaleFactor,
                                                                total);
}

TEST(GeneratorStartRowTest, WebReturns) {
  constexpr double kScaleFactor = 0.1;
  int64_t total = benchgen::tpcds::WebReturnsGenerator::TotalRows(kScaleFactor);
  ExpectStartRowMatches<benchgen::tpcds::WebReturnsGenerator>(kScaleFactor,
                                                              total);
}

TEST(GeneratorStartRowTest, CatalogReturns) {
  constexpr double kScaleFactor = 0.1;
  int64_t total =
      benchgen::tpcds::CatalogReturnsGenerator::TotalRows(kScaleFactor);
  ExpectStartRowMatches<benchgen::tpcds::CatalogReturnsGenerator>(kScaleFactor,
                                                                  total);
}

TEST(GeneratorStartRowTest, CallCenter) {
  constexpr double kScaleFactor = 0.1;
  int64_t total = benchgen::tpcds::CallCenterGenerator::TotalRows(kScaleFactor);
  ExpectStartRowMatches<benchgen::tpcds::CallCenterGenerator>(kScaleFactor,
                                                              total);
}

TEST(GeneratorStartRowTest, CatalogPage) {
  constexpr double kScaleFactor = 0.1;
  int64_t total =
      benchgen::tpcds::CatalogPageGenerator::TotalRows(kScaleFactor);
  ExpectStartRowMatches<benchgen::tpcds::CatalogPageGenerator>(kScaleFactor,
                                                               total);
}

TEST(GeneratorStartRowTest, CustomerAddress) {
  constexpr double kScaleFactor = 0.1;
  int64_t total =
      benchgen::tpcds::CustomerAddressGenerator::TotalRows(kScaleFactor);
  ExpectStartRowMatches<benchgen::tpcds::CustomerAddressGenerator>(kScaleFactor,
                                                                   total);
}

TEST(GeneratorStartRowTest, Customer) {
  constexpr double kScaleFactor = 0.1;
  int64_t total = benchgen::tpcds::CustomerGenerator::TotalRows(kScaleFactor);
  ExpectStartRowMatches<benchgen::tpcds::CustomerGenerator>(kScaleFactor,
                                                            total);
}

TEST(GeneratorStartRowTest, CustomerDemographics) {
  constexpr double kScaleFactor = 0.1;
  int64_t total =
      benchgen::tpcds::CustomerDemographicsGenerator::TotalRows(kScaleFactor);
  ExpectStartRowMatches<benchgen::tpcds::CustomerDemographicsGenerator>(
      kScaleFactor, total);
}

TEST(GeneratorStartRowTest, DateDim) {
  constexpr double kScaleFactor = 0.1;
  int64_t total = benchgen::tpcds::DateDimGenerator::TotalRows(kScaleFactor);
  ExpectStartRowMatches<benchgen::tpcds::DateDimGenerator>(kScaleFactor, total);
}

TEST(GeneratorStartRowTest, HouseholdDemographics) {
  constexpr double kScaleFactor = 0.1;
  int64_t total =
      benchgen::tpcds::HouseholdDemographicsGenerator::TotalRows(kScaleFactor);
  ExpectStartRowMatches<benchgen::tpcds::HouseholdDemographicsGenerator>(
      kScaleFactor, total);
}

TEST(GeneratorStartRowTest, IncomeBand) {
  constexpr double kScaleFactor = 0.1;
  int64_t total = benchgen::tpcds::IncomeBandGenerator::TotalRows(kScaleFactor);
  ExpectStartRowMatches<benchgen::tpcds::IncomeBandGenerator>(kScaleFactor,
                                                              total);
}

TEST(GeneratorStartRowTest, Inventory) {
  constexpr double kScaleFactor = 0.1;
  int64_t total = benchgen::tpcds::InventoryGenerator::TotalRows(kScaleFactor);
  ExpectStartRowMatches<benchgen::tpcds::InventoryGenerator>(kScaleFactor,
                                                             total);
}

TEST(GeneratorStartRowTest, Item) {
  constexpr double kScaleFactor = 0.1;
  int64_t total = benchgen::tpcds::ItemGenerator::TotalRows(kScaleFactor);
  ExpectStartRowMatches<benchgen::tpcds::ItemGenerator>(kScaleFactor, total);
}

TEST(GeneratorStartRowTest, Promotion) {
  constexpr double kScaleFactor = 0.1;
  int64_t total = benchgen::tpcds::PromotionGenerator::TotalRows(kScaleFactor);
  ExpectStartRowMatches<benchgen::tpcds::PromotionGenerator>(kScaleFactor,
                                                             total);
}

TEST(GeneratorStartRowTest, Reason) {
  constexpr double kScaleFactor = 0.1;
  int64_t total = benchgen::tpcds::ReasonGenerator::TotalRows(kScaleFactor);
  ExpectStartRowMatches<benchgen::tpcds::ReasonGenerator>(kScaleFactor, total);
}

TEST(GeneratorStartRowTest, ShipMode) {
  constexpr double kScaleFactor = 0.1;
  int64_t total = benchgen::tpcds::ShipModeGenerator::TotalRows(kScaleFactor);
  ExpectStartRowMatches<benchgen::tpcds::ShipModeGenerator>(kScaleFactor,
                                                            total);
}

TEST(GeneratorStartRowTest, Store) {
  constexpr double kScaleFactor = 0.1;
  int64_t total = benchgen::tpcds::StoreGenerator::TotalRows(kScaleFactor);
  ExpectStartRowMatches<benchgen::tpcds::StoreGenerator>(kScaleFactor, total);
}

TEST(GeneratorStartRowTest, TimeDim) {
  constexpr double kScaleFactor = 0.1;
  int64_t total = benchgen::tpcds::TimeDimGenerator::TotalRows(kScaleFactor);
  ExpectStartRowMatches<benchgen::tpcds::TimeDimGenerator>(kScaleFactor, total);
}

TEST(GeneratorStartRowTest, Warehouse) {
  constexpr double kScaleFactor = 0.1;
  int64_t total = benchgen::tpcds::WarehouseGenerator::TotalRows(kScaleFactor);
  ExpectStartRowMatches<benchgen::tpcds::WarehouseGenerator>(kScaleFactor,
                                                             total);
}

TEST(GeneratorStartRowTest, WebPage) {
  constexpr double kScaleFactor = 0.1;
  int64_t total = benchgen::tpcds::WebPageGenerator::TotalRows(kScaleFactor);
  ExpectStartRowMatches<benchgen::tpcds::WebPageGenerator>(kScaleFactor, total);
}

TEST(GeneratorStartRowTest, WebSite) {
  constexpr double kScaleFactor = 0.1;
  int64_t total = benchgen::tpcds::WebSiteGenerator::TotalRows(kScaleFactor);
  ExpectStartRowMatches<benchgen::tpcds::WebSiteGenerator>(kScaleFactor, total);
}
