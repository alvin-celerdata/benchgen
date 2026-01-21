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

#include "utils/date.h"

#include <cstdio>
#include <string>

namespace benchgen::tpcds::internal {
namespace {

constexpr int kDaysBeforeMonth[2][13] = {
    {0, 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334},
    {0, 0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335},
};

constexpr int kDaysInMonth[2][13] = {
    {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31},
    {0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31},
};

}  // namespace

Date Date::FromJulianDays(int julian_days) {
  int l = julian_days + 68569;
  int n = (4 * l) / 146097;
  l = l - (146097 * n + 3) / 4;
  int i = (4000 * (l + 1) / 1461001);
  l = l - (1461 * i) / 4 + 31;
  int j = (80 * l) / 2447;
  int day = l - (2447 * j) / 80;
  l = j / 11;
  int month = j + 2 - 12 * l;
  int year = 100 * (n - 49) + i + l;
  return Date{year, month, day};
}

int Date::ToJulianDays(const Date& date) {
  int month = date.month;
  int year = date.year;
  if (month <= 2) {
    month += 12;
    year -= 1;
  }
  constexpr int kDaysBceJulianEpoch = 1721118;
  return date.day + (153 * month - 457) / 5 + 365 * year + year / 4 -
         year / 100 + year / 400 + kDaysBceJulianEpoch + 1;
}

bool Date::IsLeapYear(int year) {
  if (year % 100 == 0) {
    return ((year % 400) % 2) == 0;
  }
  return (year % 4) == 0;
}

Date Date::FromString(std::string_view value) {
  int year = 0;
  int month = 0;
  int day = 0;
  std::string input(value);
  if (std::sscanf(input.c_str(), "%4d-%d-%d", &year, &month, &day) != 3) {
    return Date{};
  }
  return Date{year, month, day};
}

int Date::DayOfWeek(const Date& date) { return DayOfWeek(date, nullptr); }

int Date::DayOfWeek(const Date& date, DayOfWeekState* state) {
  DayOfWeekState local_state;
  if (state == nullptr) {
    state = &local_state;
  }
  int& last_year = state->last_year;
  int& dday = state->dday;
  int* known = state->known;
  static const int doomsday[4] = {3, 2, 0, 5};

  if (date.year != last_year) {
    if (IsLeapYear(date.year)) {
      known[1] = 4;
      known[2] = 1;
    } else {
      known[1] = 3;
      known[2] = 0;
    }

    int century = date.year / 100;
    int century_index = (century - 15) % 4;
    if (century_index < 0) {
      century_index += 4;
    }
    dday = doomsday[century_index];

    int q = date.year % 100;
    int r = q % 12;
    q /= 12;
    int s = r / 4;
    dday += q + r + s;
    dday %= 7;
    last_year = date.year;
  }

  int res = date.day - known[date.month];
  while (res < 0) {
    res += 7;
  }
  while (res > 6) {
    res -= 7;
  }
  res += dday;
  res %= 7;
  return res;
}

int Date::DayNumber(const Date& date) {
  const int leap = IsLeapYear(date.year) ? 1 : 0;
  return kDaysBeforeMonth[leap][date.month] + date.day;
}

Date Date::FirstDayOfMonth(const Date& date) {
  int julian = ToJulianDays(date) - date.day + 1;
  return FromJulianDays(julian);
}

Date Date::LastDayOfMonth(const Date& date) {
  int leap = IsLeapYear(date.year) ? 1 : 0;
  int julian = ToJulianDays(date) - date.day + kDaysInMonth[leap][date.month];
  return FromJulianDays(julian);
}

Date Date::SameDayLastYear(const Date& date) {
  if (IsLeapYear(date.year) && date.month == 2 && date.day == 29) {
    return Date{date.year - 1, 2, 28};
  }
  return Date{date.year - 1, date.month, date.day};
}

Date Date::SameDayLastQuarter(const Date& date) {
  Date current_start;
  Date previous_start;
  if (date.month <= 3) {
    current_start = Date{date.year, 1, 1};
    previous_start = Date{date.year - 1, 10, 1};
  } else if (date.month <= 6) {
    current_start = Date{date.year, 4, 1};
    previous_start = Date{date.year, 1, 1};
  } else if (date.month <= 9) {
    current_start = Date{date.year, 7, 1};
    previous_start = Date{date.year, 4, 1};
  } else {
    current_start = Date{date.year, 10, 1};
    previous_start = Date{date.year, 7, 1};
  }

  int offset = ToJulianDays(date) - ToJulianDays(current_start);
  int target = ToJulianDays(previous_start) + offset;
  return FromJulianDays(target);
}

int Date::DaysSinceEpoch(const Date& date) {
  static const int kEpochJulian = ToJulianDays(Date{1970, 1, 1});
  return ToJulianDays(date) - kEpochJulian;
}

}  // namespace benchgen::tpcds::internal
