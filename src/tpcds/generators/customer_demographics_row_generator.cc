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

#include "generators/customer_demographics_row_generator.h"

#include "distribution/dst_distribution_utils.h"

namespace benchgen::tpcds::internal {
namespace {

constexpr int kMaxChildren = 7;
constexpr int kMaxEmployed = 7;
constexpr int kMaxCollege = 7;

}  // namespace

CustomerDemographicsRowGenerator::CustomerDemographicsRowGenerator(
    )
    : distribution_store_() {
  gender_ = &distribution_store_.Get("gender");
  marital_status_ = &distribution_store_.Get("marital_status");
  education_ = &distribution_store_.Get("education");
  purchase_band_ = &distribution_store_.Get("purchase_band");
  credit_rating_ = &distribution_store_.Get("credit_rating");
}

CustomerDemographicsRowData CustomerDemographicsRowGenerator::GenerateRow(
    int64_t row_number) {
  CustomerDemographicsRowData row;
  row.demo_sk = row_number;
  int64_t temp = row.demo_sk - 1;

  row.gender = BitmapToString(*gender_, 1, &temp);
  row.marital_status = BitmapToString(*marital_status_, 1, &temp);
  row.education_status = BitmapToString(*education_, 1, &temp);
  row.purchase_estimate = BitmapToInt(*purchase_band_, 1, &temp);
  row.credit_rating = BitmapToString(*credit_rating_, 1, &temp);

  row.dep_count = static_cast<int32_t>(temp % kMaxChildren);
  temp /= kMaxChildren;
  row.dep_employed_count = static_cast<int32_t>(temp % kMaxEmployed);
  temp /= kMaxEmployed;
  row.dep_college_count = static_cast<int32_t>(temp % kMaxCollege);

  return row;
}

}  // namespace benchgen::tpcds::internal
