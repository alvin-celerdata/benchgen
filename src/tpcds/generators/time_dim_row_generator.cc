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

#include "generators/time_dim_row_generator.h"

#include "utils/random_utils.h"

namespace benchgen::tpcds::internal {

TimeDimRowGenerator::TimeDimRowGenerator()
    : distribution_store_() {
  hours_ = &distribution_store_.Get("hours");
}

TimeDimRowData TimeDimRowGenerator::GenerateRow(int64_t row_number) {
  TimeDimRowData row;
  int64_t index = row_number;
  int32_t ntemp = static_cast<int32_t>(index - 1);

  row.time_sk = static_cast<int32_t>(index - 1);
  row.time_id = MakeBusinessKey(static_cast<uint64_t>(index));
  row.time = ntemp;
  row.second = ntemp % 60;
  ntemp /= 60;
  row.minute = ntemp % 60;
  ntemp /= 60;
  row.hour = ntemp % 24;

  int hour_index = row.hour + 1;
  row.am_pm = hours_->GetString(hour_index, 2);
  row.shift = hours_->GetString(hour_index, 3);
  row.sub_shift = hours_->GetString(hour_index, 4);
  row.meal_time = hours_->GetString(hour_index, 5);

  return row;
}

}  // namespace benchgen::tpcds::internal
