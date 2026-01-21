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
#include <string>

#include "distribution/dst_distribution.h"

namespace benchgen::tpcds::internal {

int DistributionSize(const DstDistribution& dist);
int BitmapToIndex(const DstDistribution& dist, int64_t* modulus);
std::string BitmapToString(const DstDistribution& dist, int value_set,
                           int64_t* modulus);
int BitmapToInt(const DstDistribution& dist, int value_set, int64_t* modulus);

}  // namespace benchgen::tpcds::internal
