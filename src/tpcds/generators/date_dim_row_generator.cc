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

#include "generators/date_dim_row_generator.h"

#include <array>
#include <sstream>

#include "utils/random_utils.h"

namespace benchgen::tpcds::internal {
namespace {

constexpr int kCurrentYear = 2003;
constexpr int kCurrentMonth = 1;
constexpr int kCurrentDay = 8;
constexpr int kCurrentQuarter = 1;
constexpr int kCurrentWeek = 2;

constexpr std::array<const char*, 7> kWeekdayNames = {
    "Sunday",   "Monday", "Tuesday",  "Wednesday",
    "Thursday", "Friday", "Saturday",
};

}  // namespace

DateDimRowGenerator::DateDimRowGenerator()
    : distribution_store_() {
  calendar_ = &distribution_store_.Get("calendar");
  base_julian_ = Date::ToJulianDays(Date::FromString("1900-01-01"));
}

DateDimRowData DateDimRowGenerator::GenerateRow(int64_t row_number) {
  DateDimRowData row;
  int julian = base_julian_ + static_cast<int>(row_number);
  row.date_sk = julian;
  row.date_id = MakeBusinessKey(static_cast<uint64_t>(julian));
  row.date = Date::FromJulianDays(julian);

  row.year = row.date.year;
  row.dow = Date::DayOfWeek(row.date, &day_of_week_state_);
  row.moy = row.date.month;
  row.dom = row.date.day;

  row.week_seq = static_cast<int32_t>((row_number + 6) / 7);
  row.month_seq = (row.year - 1900) * 12 + row.moy - 1;
  row.quarter_seq = (row.year - 1900) * 4 + row.moy / 3 + 1;

  int day_index = Date::DayNumber(row.date);
  row.qoy = calendar_->GetInt(day_index, 6);
  row.fy_year = row.year;
  row.fy_quarter_seq = row.quarter_seq;
  row.fy_week_seq = row.week_seq;
  row.day_name = kWeekdayNames.at(static_cast<size_t>(row.dow));
  {
    std::ostringstream out;
    out << row.year << "Q" << row.qoy;
    row.quarter_name = out.str();
  }

  row.holiday = calendar_->GetInt(day_index, 8) != 0;
  row.weekend = (row.dow == 5 || row.dow == 6);
  if (day_index == 1) {
    int prev_index = 365 + (Date::IsLeapYear(row.year - 1) ? 1 : 0);
    row.following_holiday = calendar_->GetInt(prev_index, 8) != 0;
  } else {
    row.following_holiday = calendar_->GetInt(day_index - 1, 8) != 0;
  }

  row.first_dom = Date::ToJulianDays(Date::FirstDayOfMonth(row.date));
  int days_before_month = Date::DayNumber(row.date) - row.dom;
  // Match TPC-DS kit's legacy last_dom calculation.
  row.last_dom = Date::ToJulianDays(row.date) - row.dom + days_before_month;
  row.same_day_ly = Date::ToJulianDays(Date::SameDayLastYear(row.date));
  row.same_day_lq = Date::ToJulianDays(Date::SameDayLastQuarter(row.date));

  row.current_day = row.date_sk == kCurrentDay;
  row.current_year = row.year == kCurrentYear;
  if (row.current_year) {
    row.current_month = row.moy == kCurrentMonth;
    row.current_quarter = row.qoy == kCurrentQuarter;
    row.current_week = row.week_seq == kCurrentWeek;
  }

  return row;
}

}  // namespace benchgen::tpcds::internal
