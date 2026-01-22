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

#include <cstdint>
#include <string>

#include "benchgen/generator_options.h"
#include "generators/customer_row_generator.h"
#include "generators/date_row_generator.h"
#include "generators/lineorder_row_generator.h"
#include "generators/part_row_generator.h"
#include "generators/supplier_row_generator.h"

namespace benchgen::ssb::internal {
namespace {

struct LineorderSnapshot {
  int64_t orderkey = 0;
  int32_t linenumber = 0;
  int64_t custkey = 0;
  int64_t partkey = 0;
  int64_t suppkey = 0;
  std::string orderdate;
  std::string orderpriority;
  int32_t shippriority = 0;
  int32_t quantity = 0;
  int64_t extendedprice = 0;
  int64_t ordertotalprice = 0;
  int32_t discount = 0;
  int64_t revenue = 0;
  int64_t supplycost = 0;
  int32_t tax = 0;
  std::string commitdate;
  std::string shipmode;
};

LineorderSnapshot Snapshot(const lineorder_t* row) {
  LineorderSnapshot snap;
  snap.orderkey = static_cast<int64_t>(row->okey);
  snap.linenumber = static_cast<int32_t>(row->linenumber);
  snap.custkey = row->custkey;
  snap.partkey = row->partkey;
  snap.suppkey = row->suppkey;
  snap.orderdate = row->orderdate;
  snap.orderpriority = row->opriority;
  snap.shippriority = static_cast<int32_t>(row->ship_priority);
  snap.quantity = static_cast<int32_t>(row->quantity);
  snap.extendedprice = static_cast<int64_t>(row->extended_price);
  snap.ordertotalprice = static_cast<int64_t>(row->order_totalprice);
  snap.discount = static_cast<int32_t>(row->discount);
  snap.revenue = static_cast<int64_t>(row->revenue);
  snap.supplycost = static_cast<int64_t>(row->supp_cost);
  snap.tax = static_cast<int32_t>(row->tax);
  snap.commitdate = row->commit_date;
  snap.shipmode = row->shipmode;
  return snap;
}

}  // namespace

TEST(RowGeneratorSkipRows, Customer) {
  CustomerRowGenerator gen(1.0, benchgen::DbgenSeedMode::kAllTables);
  ASSERT_TRUE(gen.Init().ok());

  customer_t row1;
  const int64_t target_row = 6;
  for (int64_t i = 1; i <= target_row; ++i) {
    gen.GenerateRow(i, &row1);
  }

  CustomerRowGenerator gen_skip(1.0,
                                benchgen::DbgenSeedMode::kAllTables);
  ASSERT_TRUE(gen_skip.Init().ok());
  gen_skip.SkipRows(target_row - 1);
  customer_t row2;
  gen_skip.GenerateRow(target_row, &row2);

  EXPECT_EQ(row1.custkey, row2.custkey);
  EXPECT_STREQ(row1.name, row2.name);
  EXPECT_STREQ(row1.address, row2.address);
  EXPECT_STREQ(row1.city, row2.city);
  EXPECT_STREQ(row1.nation_name, row2.nation_name);
  EXPECT_STREQ(row1.region_name, row2.region_name);
  EXPECT_STREQ(row1.phone, row2.phone);
  EXPECT_STREQ(row1.mktsegment, row2.mktsegment);
}

TEST(RowGeneratorSkipRows, Part) {
  PartRowGenerator gen(1.0, benchgen::DbgenSeedMode::kAllTables);
  ASSERT_TRUE(gen.Init().ok());

  part_t row1;
  const int64_t target_row = 11;
  for (int64_t i = 1; i <= target_row; ++i) {
    gen.GenerateRow(i, &row1);
  }

  PartRowGenerator gen_skip(1.0, benchgen::DbgenSeedMode::kAllTables);
  ASSERT_TRUE(gen_skip.Init().ok());
  gen_skip.SkipRows(target_row - 1);
  part_t row2;
  gen_skip.GenerateRow(target_row, &row2);

  EXPECT_EQ(row1.partkey, row2.partkey);
  EXPECT_STREQ(row1.name, row2.name);
  EXPECT_STREQ(row1.mfgr, row2.mfgr);
  EXPECT_STREQ(row1.category, row2.category);
  EXPECT_STREQ(row1.brand, row2.brand);
  EXPECT_STREQ(row1.color, row2.color);
  EXPECT_STREQ(row1.type, row2.type);
  EXPECT_EQ(row1.size, row2.size);
  EXPECT_STREQ(row1.container, row2.container);
}

TEST(RowGeneratorSkipRows, Supplier) {
  SupplierRowGenerator gen(1.0, benchgen::DbgenSeedMode::kAllTables);
  ASSERT_TRUE(gen.Init().ok());

  supplier_t row1;
  const int64_t target_row = 8;
  for (int64_t i = 1; i <= target_row; ++i) {
    gen.GenerateRow(i, &row1);
  }

  SupplierRowGenerator gen_skip(1.0,
                                benchgen::DbgenSeedMode::kAllTables);
  ASSERT_TRUE(gen_skip.Init().ok());
  gen_skip.SkipRows(target_row - 1);
  supplier_t row2;
  gen_skip.GenerateRow(target_row, &row2);

  EXPECT_EQ(row1.suppkey, row2.suppkey);
  EXPECT_STREQ(row1.name, row2.name);
  EXPECT_STREQ(row1.address, row2.address);
  EXPECT_STREQ(row1.city, row2.city);
  EXPECT_STREQ(row1.nation_name, row2.nation_name);
  EXPECT_STREQ(row1.region_name, row2.region_name);
  EXPECT_STREQ(row1.phone, row2.phone);
}

TEST(RowGeneratorSkipRows, Date) {
  DateRowGenerator gen(1.0, benchgen::DbgenSeedMode::kAllTables);
  ASSERT_TRUE(gen.Init().ok());

  date_t row1;
  gen.GenerateRow(25, &row1);

  DateRowGenerator gen_skip(1.0, benchgen::DbgenSeedMode::kAllTables);
  ASSERT_TRUE(gen_skip.Init().ok());
  gen_skip.SkipRows(24);
  date_t row2;
  gen_skip.GenerateRow(25, &row2);

  EXPECT_EQ(row1.datekey, row2.datekey);
  EXPECT_STREQ(row1.date, row2.date);
  EXPECT_STREQ(row1.dayofweek, row2.dayofweek);
  EXPECT_STREQ(row1.month, row2.month);
  EXPECT_EQ(row1.year, row2.year);
  EXPECT_EQ(row1.yearmonthnum, row2.yearmonthnum);
  EXPECT_STREQ(row1.yearmonth, row2.yearmonth);
  EXPECT_EQ(row1.daynuminweek, row2.daynuminweek);
  EXPECT_EQ(row1.daynuminmonth, row2.daynuminmonth);
  EXPECT_EQ(row1.daynuminyear, row2.daynuminyear);
  EXPECT_EQ(row1.monthnuminyear, row2.monthnuminyear);
  EXPECT_EQ(row1.weeknuminyear, row2.weeknuminyear);
  EXPECT_STREQ(row1.sellingseason, row2.sellingseason);
  EXPECT_STREQ(row1.lastdayinweekfl, row2.lastdayinweekfl);
  EXPECT_STREQ(row1.lastdayinmonthfl, row2.lastdayinmonthfl);
  EXPECT_STREQ(row1.holidayfl, row2.holidayfl);
  EXPECT_STREQ(row1.weekdayfl, row2.weekdayfl);
}

TEST(RowGeneratorSkipRows, Lineorder) {
  LineorderRowGenerator gen(1.0, benchgen::DbgenSeedMode::kAllTables);
  ASSERT_TRUE(gen.Init().ok());

  const lineorder_t* row = nullptr;
  LineorderSnapshot expected;
  int64_t target_index = 15;
  for (int64_t i = 0; i <= target_index; ++i) {
    ASSERT_TRUE(gen.NextRow(&row));
    if (i == target_index) {
      expected = Snapshot(row);
    }
  }

  LineorderRowGenerator gen_skip(1.0,
                                 benchgen::DbgenSeedMode::kAllTables);
  ASSERT_TRUE(gen_skip.Init().ok());
  gen_skip.SkipRows(target_index);
  ASSERT_TRUE(gen_skip.NextRow(&row));
  LineorderSnapshot actual = Snapshot(row);

  EXPECT_EQ(expected.orderkey, actual.orderkey);
  EXPECT_EQ(expected.linenumber, actual.linenumber);
  EXPECT_EQ(expected.custkey, actual.custkey);
  EXPECT_EQ(expected.partkey, actual.partkey);
  EXPECT_EQ(expected.suppkey, actual.suppkey);
  EXPECT_EQ(expected.orderdate, actual.orderdate);
  EXPECT_EQ(expected.orderpriority, actual.orderpriority);
  EXPECT_EQ(expected.shippriority, actual.shippriority);
  EXPECT_EQ(expected.quantity, actual.quantity);
  EXPECT_EQ(expected.extendedprice, actual.extendedprice);
  EXPECT_EQ(expected.ordertotalprice, actual.ordertotalprice);
  EXPECT_EQ(expected.discount, actual.discount);
  EXPECT_EQ(expected.revenue, actual.revenue);
  EXPECT_EQ(expected.supplycost, actual.supplycost);
  EXPECT_EQ(expected.tax, actual.tax);
  EXPECT_EQ(expected.commitdate, actual.commitdate);
  EXPECT_EQ(expected.shipmode, actual.shipmode);
}

}  // namespace benchgen::ssb::internal
