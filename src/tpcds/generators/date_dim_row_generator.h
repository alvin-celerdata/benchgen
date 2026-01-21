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
#include "utils/date.h"

namespace benchgen::tpcds::internal {

struct DateDimRowData {
  int32_t date_sk = 0;
  std::string date_id;
  Date date;
  int32_t month_seq = 0;
  int32_t week_seq = 0;
  int32_t quarter_seq = 0;
  int32_t year = 0;
  int32_t dow = 0;
  int32_t moy = 0;
  int32_t dom = 0;
  int32_t qoy = 0;
  int32_t fy_year = 0;
  int32_t fy_quarter_seq = 0;
  int32_t fy_week_seq = 0;
  std::string day_name;
  std::string quarter_name;
  bool holiday = false;
  bool weekend = false;
  bool following_holiday = false;
  int32_t first_dom = 0;
  int32_t last_dom = 0;
  int32_t same_day_ly = 0;
  int32_t same_day_lq = 0;
  bool current_day = false;
  bool current_week = false;
  bool current_month = false;
  bool current_quarter = false;
  bool current_year = false;
};

class DateDimRowGenerator {
 public:
  explicit DateDimRowGenerator();

  DateDimRowData GenerateRow(int64_t row_number);

 private:
  DstDistributionStore distribution_store_;
  const DstDistribution* calendar_ = nullptr;
  int base_julian_ = 0;
  DayOfWeekState day_of_week_state_;
};

}  // namespace benchgen::tpcds::internal
