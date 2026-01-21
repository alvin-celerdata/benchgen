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

#include "utils/utils.h"

#include <algorithm>
#include <cstdio>

#include "utils/constants.h"

namespace benchgen::tpch::internal {
namespace {

constexpr char kAlphaNum[] =
    "0123456789abcdefghijklmnopqrstuvwxyz ABCDEFGHIJKLMNOPQRSTUVWXYZ,";

struct MonthInfo {
  int days;
  int cumulative;
};

constexpr MonthInfo kMonths[] = {
    {0, 0},    {31, 31},  {28, 59},  {31, 90},  {30, 120}, {31, 151}, {30, 181},
    {31, 212}, {31, 243}, {30, 273}, {31, 304}, {30, 334}, {31, 365},
};

bool IsLeapYear(int64_t year) { return (year % 4 == 0) && (year % 100 != 0); }

int64_t LeapAdjustment(int64_t year, int month) {
  return (IsLeapYear(year) && month >= 2) ? 1 : 0;
}

std::string MakeDateString(int64_t index) {
  int64_t julian = JulianDate(index + kStartDate - 1);
  int64_t year = julian / 1000;
  int64_t day = julian % 1000;
  int month = 0;
  while (day > kMonths[month].cumulative + LeapAdjustment(year, month)) {
    ++month;
  }
  int64_t day_in_month = day - kMonths[month - 1].cumulative -
                         ((IsLeapYear(year) && month > 2) ? 1 : 0);
  char buffer[kDateLen + 1] = {};
  std::snprintf(buffer, sizeof(buffer), "19%02lld-%02d-%02d",
                static_cast<long long>(year), month,
                static_cast<int>(day_in_month));
  return buffer;
}

}  // namespace

int RandomString(int min_len, int max_len, int stream, RandomState* rng,
                 std::string* out) {
  if (!rng || !out) {
    return 0;
  }
  if (min_len > max_len) {
    std::swap(min_len, max_len);
  }
  int64_t length = rng->RandomInt(min_len, max_len, stream);
  if (length < 0) {
    length = 0;
  }
  out->assign(static_cast<std::size_t>(length), '\0');
  int64_t char_int = 0;
  for (int64_t i = 0; i < length; ++i) {
    if (i % 5 == 0) {
      char_int = rng->RandomInt(0, kMaxLong, stream);
    }
    (*out)[static_cast<std::size_t>(i)] =
        kAlphaNum[static_cast<std::size_t>(char_int & 077)];
    char_int >>= 6;
  }
  return static_cast<int>(length);
}

int VariableString(int avg_len, int stream, RandomState* rng,
                   std::string* out) {
  int min_len = static_cast<int>(static_cast<double>(avg_len) * kVStrLow);
  int max_len = static_cast<int>(static_cast<double>(avg_len) * kVStrHigh);
  return RandomString(min_len, max_len, stream, rng, out);
}

int PickString(const Distribution& dist, int stream, RandomState* rng,
               std::string* out) {
  if (!rng || !out || dist.list.empty() || dist.max <= 0) {
    return -1;
  }
  int64_t pick = rng->RandomInt(1, dist.max, stream);
  std::size_t index = 0;
  while (dist.list[index].weight < pick) {
    ++index;
  }
  *out = dist.list[index].text;
  return static_cast<int>(index);
}

void AggString(const Distribution& dist, int count, int stream,
               RandomState* rng, std::string* out) {
  if (!rng || !out || dist.list.empty() || count <= 0) {
    if (out) {
      out->clear();
    }
    return;
  }
  int dist_size = static_cast<int>(dist.list.size());
  if (count > dist_size) {
    count = dist_size;
  }
  std::vector<int> permute(dist_size);
  for (int i = 0; i < dist_size; ++i) {
    permute[i] = i;
  }
  for (int i = 0; i < dist_size; ++i) {
    int64_t source = rng->RandomInt(i, dist_size - 1, stream);
    std::swap(permute[i], permute[static_cast<int>(source)]);
  }
  std::string result;
  result.reserve(static_cast<std::size_t>(count) * 8);
  for (int i = 0; i < count; ++i) {
    if (i > 0) {
      result.push_back(' ');
    }
    result += dist.list[permute[i]].text;
  }
  *out = std::move(result);
}

void GeneratePhone(int64_t nation_index, int stream, RandomState* rng,
                   std::string* out) {
  if (!rng || !out) {
    return;
  }
  int64_t acode = rng->RandomInt(100, 999, stream);
  int64_t exchg = rng->RandomInt(100, 999, stream);
  int64_t number = rng->RandomInt(1000, 9999, stream);
  char buffer[kPhoneLen + 1] = {};
  std::snprintf(buffer, sizeof(buffer), "%02d",
                10 + static_cast<int>(nation_index % kNationsMax));
  std::snprintf(buffer + 3, sizeof(buffer) - 3, "%03d",
                static_cast<int>(acode));
  std::snprintf(buffer + 7, sizeof(buffer) - 7, "%03d",
                static_cast<int>(exchg));
  std::snprintf(buffer + 11, sizeof(buffer) - 11, "%04d",
                static_cast<int>(number));
  buffer[2] = buffer[6] = buffer[10] = '-';
  *out = buffer;
}

int64_t RetailPrice(int64_t partkey) {
  int64_t price = 90000;
  price += (partkey / 10) % 20001;
  price += (partkey % 1000) * 100;
  return price;
}

int64_t PartSuppBridge(int64_t partkey, int64_t supp_index,
                       int64_t supplier_count) {
  if (supplier_count <= 0) {
    return 1;
  }
  int64_t stride =
      supplier_count / kSuppPerPart + (partkey - 1) / supplier_count;
  int64_t value = (partkey + supp_index * stride) % supplier_count + 1;
  return value;
}

int64_t MakeSparseKey(int64_t index, int64_t seq) {
  int64_t value = index;
  int64_t low_bits = value & ((1 << kSparseKeep) - 1);
  value >>= kSparseKeep;
  value <<= kSparseBits;
  value += seq;
  value <<= kSparseKeep;
  value += low_bits;
  return value;
}

int64_t OrderDateMax() {
  return kStartDate + kTotalDate - (kLSdteMax + kLRdteMax) - 1;
}

int64_t JulianDate(int64_t date) {
  int64_t offset = date - kStartDate;
  int64_t result = kStartDate;
  while (true) {
    int64_t year = result / 1000;
    int64_t year_end = year * 1000 + 365 + (IsLeapYear(year) ? 1 : 0);
    if (result + offset > year_end) {
      offset -= year_end - result + 1;
      result += 1000;
      continue;
    }
    break;
  }
  return result + offset;
}

void BuildAscDate(std::vector<std::string>* out) {
  if (!out) {
    return;
  }
  out->clear();
  out->reserve(static_cast<std::size_t>(kTotalDate));
  for (int64_t i = 1; i <= kTotalDate; ++i) {
    out->push_back(MakeDateString(i));
  }
}

std::string FormatTagNumber(const char* tag, int width, int64_t number) {
  char buffer[64] = {};
  std::snprintf(buffer, sizeof(buffer), "%s%0*lld", tag, width,
                static_cast<long long>(number));
  return buffer;
}

}  // namespace benchgen::tpch::internal
