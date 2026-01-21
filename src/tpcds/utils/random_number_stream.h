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

#pragma once

#include <cstdint>

namespace benchgen::tpcds::internal {

constexpr int64_t kMaxInt = 2147483647;
constexpr int kMaxColumn = 799;
constexpr int kSeedBase = 19620718;

class RandomNumberStream {
 public:
  RandomNumberStream() = default;
  RandomNumberStream(int global_column_number, int seeds_per_row);

  int64_t NextRandom();
  double NextRandomDouble();
  void SkipRows(int64_t row_count);
  void ResetSeed();

  int seeds_used() const { return seeds_used_; }
  void ResetSeedsUsed() { seeds_used_ = 0; }
  int seeds_per_row() const { return seeds_per_row_; }

 private:
  int64_t seed_ = 3;
  int64_t initial_seed_ = 3;
  int seeds_used_ = 0;
  int seeds_per_row_ = 0;
};

}  // namespace benchgen::tpcds::internal
