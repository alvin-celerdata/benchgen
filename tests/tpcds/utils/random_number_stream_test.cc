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

#include "utils/random_number_stream.h"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <vector>

namespace benchgen::tpcds::internal {
namespace {

TEST(RandomNumberStreamTest, NextRandomSequenceMatchesReference) {
  RandomNumberStream stream(0, 1);
  const std::vector<int64_t> expected = {
      1200409435, 1819994127, 2031708268, 1930872976, 1556717815,
  };

  for (size_t i = 0; i < expected.size(); ++i) {
    EXPECT_EQ(stream.NextRandom(), expected[i]) << "index " << i;
  }
  EXPECT_EQ(stream.seeds_used(), static_cast<int>(expected.size()));
}

TEST(RandomNumberStreamTest, NextRandomDoubleUsesSameSequence) {
  RandomNumberStream stream(0, 1);
  const int64_t expected_first = 1200409435;

  double value = stream.NextRandomDouble();
  double expected =
      static_cast<double>(expected_first) / static_cast<double>(kMaxInt);
  EXPECT_NEAR(value, expected, 1e-12);
  EXPECT_GT(value, 0.0);
  EXPECT_LT(value, 1.0);
  EXPECT_EQ(stream.seeds_used(), 1);
}

TEST(RandomNumberStreamTest, ResetSeedRestartsSequence) {
  RandomNumberStream stream(1, 2);
  int64_t first = stream.NextRandom();
  EXPECT_EQ(stream.seeds_used(), 1);

  stream.NextRandom();
  stream.ResetSeed();
  EXPECT_EQ(stream.seeds_used(), 0);
  EXPECT_EQ(stream.NextRandom(), first);
}

TEST(RandomNumberStreamTest, SkipRowsMatchesIterating) {
  const int column = 5;
  const int seeds_per_row = 3;
  const int64_t row_count = 7;
  const int64_t values_to_skip = row_count * seeds_per_row;

  RandomNumberStream baseline(column, seeds_per_row);
  RandomNumberStream skipped(column, seeds_per_row);

  for (int64_t i = 0; i < values_to_skip; ++i) {
    baseline.NextRandom();
  }
  int64_t expected_next = baseline.NextRandom();

  skipped.SkipRows(row_count);
  EXPECT_EQ(skipped.seeds_used(), 0);
  EXPECT_EQ(skipped.NextRandom(), expected_next);
  EXPECT_EQ(skipped.seeds_used(), 1);
}

TEST(RandomNumberStreamTest, ResetSeedsUsedClearsCounter) {
  RandomNumberStream stream(123, 4);
  stream.NextRandom();
  stream.NextRandom();
  EXPECT_EQ(stream.seeds_used(), 2);
  stream.ResetSeedsUsed();
  EXPECT_EQ(stream.seeds_used(), 0);
  EXPECT_EQ(stream.seeds_per_row(), 4);
}

}  // namespace
}  // namespace benchgen::tpcds::internal
