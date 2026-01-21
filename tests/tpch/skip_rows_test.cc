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

#include <string>
#include <vector>

#include "benchgen/arrow_compat.h"
#include "generators/customer_generator.h"
#include "generators/lineitem_generator.h"
#include "generators/partsupp_generator.h"

namespace benchgen::tpch {
namespace {

std::vector<std::vector<std::string>> CollectRows(RecordBatchIterator* iter,
                                                  int64_t limit) {
  std::vector<std::vector<std::string>> rows;
  std::shared_ptr<arrow::RecordBatch> batch;
  while (rows.size() < static_cast<size_t>(limit)) {
    auto status = iter->Next(&batch);
    if (!status.ok()) {
      return {};
    }
    if (!batch) {
      break;
    }
    for (int64_t row = 0;
         row < batch->num_rows() && rows.size() < static_cast<size_t>(limit);
         ++row) {
      std::vector<std::string> values;
      values.reserve(batch->num_columns());
      for (int col = 0; col < batch->num_columns(); ++col) {
        auto scalar_result = batch->column(col)->GetScalar(row);
        if (!scalar_result.ok()) {
          return {};
        }
        auto scalar = scalar_result.ValueOrDie();
        values.push_back(scalar->ToString());
      }
      rows.push_back(std::move(values));
    }
  }
  return rows;
}

}  // namespace

TEST(TpchSkipRows, Customer) {
  GeneratorOptions options;
  options.scale_factor = 1.0;
  options.chunk_size = 64;

  CustomerGenerator full_iter(options);
  ASSERT_TRUE(full_iter.Init().ok());
  auto all_rows = CollectRows(&full_iter, 20);
  ASSERT_EQ(all_rows.size(), 20u);

  GeneratorOptions skip_options = options;
  skip_options.start_row = 5;
  skip_options.row_count = 10;
  CustomerGenerator skip_iter(skip_options);
  ASSERT_TRUE(skip_iter.Init().ok());
  auto skipped_rows = CollectRows(&skip_iter, 10);
  ASSERT_EQ(skipped_rows.size(), 10u);

  for (size_t i = 0; i < skipped_rows.size(); ++i) {
    EXPECT_EQ(skipped_rows[i], all_rows[i + 5]);
  }
}

TEST(TpchSkipRows, PartSupp) {
  GeneratorOptions options;
  options.scale_factor = 1.0;
  options.chunk_size = 64;

  PartSuppGenerator full_iter(options);
  ASSERT_TRUE(full_iter.Init().ok());
  auto all_rows = CollectRows(&full_iter, 12);
  ASSERT_EQ(all_rows.size(), 12u);

  GeneratorOptions skip_options = options;
  skip_options.start_row = 3;
  skip_options.row_count = 6;
  PartSuppGenerator skip_iter(skip_options);
  ASSERT_TRUE(skip_iter.Init().ok());
  auto skipped_rows = CollectRows(&skip_iter, 6);
  ASSERT_EQ(skipped_rows.size(), 6u);

  for (size_t i = 0; i < skipped_rows.size(); ++i) {
    EXPECT_EQ(skipped_rows[i], all_rows[i + 3]);
  }
}

TEST(TpchSkipRows, LineItem) {
  GeneratorOptions options;
  options.scale_factor = 1.0;
  options.chunk_size = 64;

  LineItemGenerator full_iter(options);
  ASSERT_TRUE(full_iter.Init().ok());
  auto all_rows = CollectRows(&full_iter, 25);
  ASSERT_EQ(all_rows.size(), 25u);

  GeneratorOptions skip_options = options;
  skip_options.start_row = 10;
  skip_options.row_count = 10;
  LineItemGenerator skip_iter(skip_options);
  ASSERT_TRUE(skip_iter.Init().ok());
  auto skipped_rows = CollectRows(&skip_iter, 10);
  ASSERT_EQ(skipped_rows.size(), 10u);

  for (size_t i = 0; i < skipped_rows.size(); ++i) {
    EXPECT_EQ(skipped_rows[i], all_rows[i + 10]);
  }
}

}  // namespace benchgen::tpch
