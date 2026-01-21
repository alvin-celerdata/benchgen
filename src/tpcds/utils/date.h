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

#include <string_view>

namespace benchgen::tpcds::internal {

struct DayOfWeekState {
  int last_year = -1;
  int dday = 0;
  int known[13] = {0, 3, 0, 0, 4, 9, 6, 11, 8, 5, 10, 7, 12};
};

struct Date {
  int year = 0;
  int month = 0;
  int day = 0;

  static Date FromJulianDays(int julian_days);
  static int ToJulianDays(const Date& date);
  static bool IsLeapYear(int year);

  static Date FromString(std::string_view value);
  static int DayOfWeek(const Date& date);
  static int DayOfWeek(const Date& date, DayOfWeekState* state);
  static int DayNumber(const Date& date);
  static Date FirstDayOfMonth(const Date& date);
  static Date LastDayOfMonth(const Date& date);
  static Date SameDayLastYear(const Date& date);
  static Date SameDayLastQuarter(const Date& date);
  static int DaysSinceEpoch(const Date& date);
};

}  // namespace benchgen::tpcds::internal
