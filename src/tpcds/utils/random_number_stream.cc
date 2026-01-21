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

namespace benchgen::tpcds::internal {
namespace {

constexpr int64_t kMultiplier = 16807;
constexpr int64_t kQuotient = 127773;
constexpr int64_t kRemainder = 2836;

}  // namespace

RandomNumberStream::RandomNumberStream(int global_column_number,
                                       int seeds_per_row)
    : seeds_per_row_(seeds_per_row) {
  const int64_t skip = kMaxInt / kMaxColumn;
  initial_seed_ = static_cast<int64_t>(kSeedBase) + skip * global_column_number;
  seed_ = initial_seed_;
}

int64_t RandomNumberStream::NextRandom() {
  int64_t div_res = seed_ / kQuotient;
  int64_t mod_res = seed_ - kQuotient * div_res;
  int64_t next = kMultiplier * mod_res - div_res * kRemainder;
  if (next < 0) {
    next += kMaxInt;
  }
  seed_ = next;
  ++seeds_used_;
  return seed_;
}

double RandomNumberStream::NextRandomDouble() {
  return static_cast<double>(NextRandom()) / static_cast<double>(kMaxInt);
}

void RandomNumberStream::SkipRows(int64_t row_count) {
  int64_t values_to_skip = row_count * seeds_per_row_;
  int64_t next_seed = initial_seed_;
  int64_t multiplier = kMultiplier;
  while (values_to_skip > 0) {
    if (values_to_skip % 2 != 0) {
      next_seed = (multiplier * next_seed) % kMaxInt;
    }
    values_to_skip /= 2;
    multiplier = (multiplier * multiplier) % kMaxInt;
  }
  seed_ = next_seed;
  seeds_used_ = 0;
}

void RandomNumberStream::ResetSeed() {
  seed_ = initial_seed_;
  seeds_used_ = 0;
}

}  // namespace benchgen::tpcds::internal
