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

struct ReasonRowData {
  int64_t reason_sk = 0;
  std::string reason_id;
  std::string reason_description;
};

class ReasonRowGenerator {
 public:
  explicit ReasonRowGenerator();

  ReasonRowData GenerateRow(int64_t row_number);

 private:
  DstDistributionStore distribution_store_;
  const DstDistribution* return_reasons_ = nullptr;
};

}  // namespace benchgen::tpcds::internal
