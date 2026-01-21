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

#include "utils/constants.h"

namespace benchgen::ssb::internal {

using DSS_HUGE = int64_t;

struct customer_t {
  int64_t custkey = 0;
  char name[kCNameLen + 1]{};
  int nlen = 0;
  char address[kCAddrMax + 1]{};
  int alen = 0;
  char city[kCityFix + 1]{};
  int nation_key = 0;
  char nation_name[kCNationNameLen + 1]{};
  int region_key = 0;
  char region_name[kCRegionNameLen + 1]{};
  char phone[kPhoneLen + 1]{};
  char mktsegment[kMaxAggLen + 1]{};
};

struct lineorder_t {
  DSS_HUGE okey = 0;
  int linenumber = 0;
  int64_t custkey = 0;
  int64_t partkey = 0;
  int64_t suppkey = 0;
  char orderdate[kDateLen + 1]{};
  char opriority[kMaxAggLen + 1]{};
  int ship_priority = 0;
  int64_t quantity = 0;
  int64_t extended_price = 0;
  int64_t order_totalprice = 0;
  int64_t discount = 0;
  int64_t revenue = 0;
  int64_t supp_cost = 0;
  int64_t tax = 0;
  char commit_date[kDateLen + 1]{};
  char shipmode[kOShipModeLen + 1]{};
};

struct order_t {
  DSS_HUGE okey = 0;
  int64_t custkey = 0;
  int totalprice = 0;
  char odate[kDateLen + 1]{};
  char opriority[kMaxAggLen + 1]{};
  char clerk[kOClrkLen + 1]{};
  int spriority = 0;
  int64_t lines = 0;
  lineorder_t lineorders[kOLcntMax]{};
};

struct part_t {
  int64_t partkey = 0;
  char name[kPNameLen + 1]{};
  int nlen = 0;
  char mfgr[kPMfgLen + 1]{};
  char category[kPCatLen + 1]{};
  char brand[kPBrndLen + 1]{};
  char color[kPColorMax + 1]{};
  int clen = 0;
  char type[kPTypeMax + 1]{};
  int tlen = 0;
  int64_t size = 0;
  char container[kPCntrLen + 1]{};
};

struct supplier_t {
  int64_t suppkey = 0;
  char name[kSNameLen + 1]{};
  char address[kSAddrMax + 1]{};
  int alen = 0;
  char city[kCityFix + 1]{};
  int nation_key = 0;
  char nation_name[kSNationNameLen + 1]{};
  int region_key = 0;
  char region_name[kSRegionNameLen + 1]{};
  char phone[kPhoneLen + 1]{};
};

struct date_t {
  int64_t datekey = 0;
  char date[kDDateLen + 1]{};
  char dayofweek[kDDayweekLen + 1]{};
  char month[kDMonthLen + 1]{};
  int year = 0;
  int yearmonthnum = 0;
  char yearmonth[kDYearmonthLen + 1]{};
  int daynuminweek = 0;
  int daynuminmonth = 0;
  int daynuminyear = 0;
  int monthnuminyear = 0;
  int weeknuminyear = 0;
  char sellingseason[kDSeasonLen + 1]{};
  int slen = 0;
  char lastdayinweekfl[2]{};
  char lastdayinmonthfl[2]{};
  char holidayfl[2]{};
  char weekdayfl[2]{};
};

}  // namespace benchgen::ssb::internal
