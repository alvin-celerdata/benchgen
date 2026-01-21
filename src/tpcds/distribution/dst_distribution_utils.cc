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

#include "distribution/dst_distribution_utils.h"

#include <stdexcept>

namespace benchgen::tpcds::internal {

int DistributionSize(const DstDistribution& dist) { return dist.size(); }

int BitmapToIndex(const DstDistribution& dist, int64_t* modulus) {
  if (modulus == nullptr) {
    throw std::invalid_argument("modulus must not be null");
  }
  int size = dist.size();
  if (size <= 0) {
    throw std::runtime_error("distribution is empty");
  }
  int index = static_cast<int>((*modulus % size) + 1);
  *modulus /= size;
  return index;
}

std::string BitmapToString(const DstDistribution& dist, int value_set,
                           int64_t* modulus) {
  int index = BitmapToIndex(dist, modulus);
  return dist.GetString(index, value_set);
}

int BitmapToInt(const DstDistribution& dist, int value_set, int64_t* modulus) {
  int index = BitmapToIndex(dist, modulus);
  return dist.GetInt(index, value_set);
}

}  // namespace benchgen::tpcds::internal
