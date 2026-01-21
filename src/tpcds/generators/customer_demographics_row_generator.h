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

struct CustomerDemographicsRowData {
  int64_t demo_sk = 0;
  std::string gender;
  std::string marital_status;
  std::string education_status;
  int32_t purchase_estimate = 0;
  std::string credit_rating;
  int32_t dep_count = 0;
  int32_t dep_employed_count = 0;
  int32_t dep_college_count = 0;
};

class CustomerDemographicsRowGenerator {
 public:
  explicit CustomerDemographicsRowGenerator(
      );

  CustomerDemographicsRowData GenerateRow(int64_t row_number);

 private:
  DstDistributionStore distribution_store_;
  const DstDistribution* gender_ = nullptr;
  const DstDistribution* marital_status_ = nullptr;
  const DstDistribution* education_ = nullptr;
  const DstDistribution* purchase_band_ = nullptr;
  const DstDistribution* credit_rating_ = nullptr;
};

}  // namespace benchgen::tpcds::internal
