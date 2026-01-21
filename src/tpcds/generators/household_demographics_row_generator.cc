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

#include "generators/household_demographics_row_generator.h"

#include "distribution/dst_distribution_utils.h"

namespace benchgen::tpcds::internal {

HouseholdDemographicsRowGenerator::HouseholdDemographicsRowGenerator(
    )
    : distribution_store_() {
  income_band_ = &distribution_store_.Get("income_band");
  buy_potential_ = &distribution_store_.Get("buy_potential");
  dependent_count_ = &distribution_store_.Get("dependent_count");
  vehicle_count_ = &distribution_store_.Get("vehicle_count");
}

HouseholdDemographicsRowData HouseholdDemographicsRowGenerator::GenerateRow(
    int64_t row_number) {
  HouseholdDemographicsRowData row;
  row.demo_sk = row_number;

  int64_t temp = row.demo_sk;
  int size = DistributionSize(*income_band_);
  row.income_band_sk = (temp % size) + 1;
  temp /= size;

  row.buy_potential = BitmapToString(*buy_potential_, 1, &temp);
  row.dep_count = BitmapToInt(*dependent_count_, 1, &temp);
  row.vehicle_count = BitmapToInt(*vehicle_count_, 1, &temp);

  return row;
}

}  // namespace benchgen::tpcds::internal
