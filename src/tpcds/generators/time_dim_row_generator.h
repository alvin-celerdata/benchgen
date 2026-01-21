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

#include "distribution/dst_distribution_store.h"

namespace benchgen::tpcds::internal {

struct TimeDimRowData {
  int32_t time_sk = 0;
  std::string time_id;
  int32_t time = 0;
  int32_t hour = 0;
  int32_t minute = 0;
  int32_t second = 0;
  std::string am_pm;
  std::string shift;
  std::string sub_shift;
  std::string meal_time;
};

class TimeDimRowGenerator {
 public:
  explicit TimeDimRowGenerator();

  TimeDimRowData GenerateRow(int64_t row_number);

 private:
  DstDistributionStore distribution_store_;
  const DstDistribution* hours_ = nullptr;
};

}  // namespace benchgen::tpcds::internal
