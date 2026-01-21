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
#include <cmath>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <string_view>

#include "utils/context.h"

namespace benchgen::ssb::internal {
namespace {

const char kAlphaNum[] =
    "0123456789abcdefghijklmnopqrstuvwxyz ABCDEFGHIJKLMNOPQRSTUVWXYZ,";

struct MonthInfo {
  const char* name;
  int days;
  int cumulative;
};

struct SeasonInfo {
  const char* name;
  int start_day;
  int start_month;
  int end_day;
  int end_month;
};

struct HolidayInfo {
  const char* name;
  int month;
  int day;
};

constexpr MonthInfo kMonths[] = {
    {nullptr, 0, 0},  {"JAN", 31, 31},  {"FEB", 28, 59},  {"MAR", 31, 90},
    {"APR", 30, 120}, {"MAY", 31, 151}, {"JUN", 30, 181}, {"JUL", 31, 212},
    {"AUG", 31, 243}, {"SEP", 30, 273}, {"OCT", 31, 304}, {"NOV", 30, 334},
    {"DEC", 31, 365},
};

constexpr SeasonInfo kSeasons[] = {
    {"Christmas", 1, 11, 31, 12}, {"Summer", 1, 5, 31, 8},
    {"Winter", 1, 1, 31, 3},      {"Spring", 1, 4, 30, 4},
    {"Fall", 1, 9, 31, 10},
};

constexpr HolidayInfo kHolidays[] = {
    {"Christmas", 12, 24}, {"New Years Day", 1, 1}, {"holiday1", 2, 20},
    {"Easter Day", 4, 20}, {"holiday2", 5, 20},     {"holiday3", 7, 20},
    {"holiday4", 8, 20},   {"holiday5", 9, 20},     {"holiday6", 10, 20},
    {"holiday7", 11, 20},
};

const char* kMonthNames[] = {"January",   "February", "March",    "April",
                             "May",       "June",     "July",     "Augest",
                             "September", "Octorber", "November", "December"};

const char* kWeekdayNames[] = {"Sunday",   "Monday", "Tuesday", "Wednesday",
                               "Thursday", "Friday", "Saturday"};

bool IsLeapYear(int year) { return (year % 4 == 0) && (year % 100 != 0); }

bool NextToken(std::string_view text, std::size_t* cursor,
               std::string_view* token) {
  if (!cursor || !token) {
    return false;
  }
  std::size_t pos = *cursor;
  while (pos < text.size() && text[pos] == ' ') {
    ++pos;
  }
  if (pos >= text.size()) {
    *cursor = text.size();
    *token = std::string_view();
    return false;
  }
  std::size_t start = pos;
  while (pos < text.size() && text[pos] != ' ') {
    ++pos;
  }
  *token = text.substr(start, pos - start);
  *cursor = pos;
  return true;
}

bool LocalTimeSafe(const std::time_t* time_value, std::tm* out) {
#if defined(_WIN32)
  return localtime_s(out, time_value) == 0;
#else
  return localtime_r(time_value, out) != nullptr;
#endif
}

int LeapAdjustment(int year, int month) {
  return (IsLeapYear(year) && month >= 2) ? 1 : 0;
}

int64_t Julian(int64_t date) {
  int64_t offset = date - kStartDate;
  int64_t result = kStartDate;
  while (true) {
    int64_t yr = result / 1000;
    int64_t yend = yr * 1000 + 365 + (IsLeapYear(static_cast<int>(yr)) ? 1 : 0);
    if (result + offset > yend) {
      offset -= yend - result + 1;
      result += 1000;
      continue;
    }
    break;
  }
  return result + offset;
}

std::string MakeAsciiDate(int64_t index) {
  int64_t month = 0;
  int64_t julian = Julian(index + kStartDate - 1);
  int64_t year = julian / 1000;
  int64_t day = julian % 1000;
  while (day >
         kMonths[month].cumulative +
             LeapAdjustment(static_cast<int>(year), static_cast<int>(month))) {
    ++month;
  }
  int64_t day_in_month =
      day - kMonths[month - 1].cumulative -
      ((IsLeapYear(static_cast<int>(year)) && month > 2) ? 1 : 0);
  char buffer[kDateLen + 1] = {};
  std::snprintf(buffer, sizeof(buffer), "19%02lld%02lld%02lld",
                static_cast<long long>(year), static_cast<long long>(month),
                static_cast<long long>(day_in_month));
  return buffer;
}

void GenerateSeason(std::string* dest, int month, int day) {
  if (!dest) {
    return;
  }
  for (const auto& season : kSeasons) {
    if (month >= season.start_month && month <= season.end_month &&
        day >= season.start_day && day <= season.end_day) {
      *dest = season.name;
      return;
    }
  }
  dest->clear();
}

void GenerateHolidayFlag(std::string* dest, int month, int day) {
  if (!dest) {
    return;
  }
  for (const auto& holiday : kHolidays) {
    if (holiday.month == month && holiday.day == day) {
      *dest = "1";
      return;
    }
  }
  *dest = "0";
}

int DaysInMonth(int year, int month) {
  static const int kDaysInMonth[] = {31, 28, 31, 30, 31, 30,
                                     31, 31, 30, 31, 30, 31};
  static const int kDaysInMonthLeap[] = {31, 29, 31, 30, 31, 30,
                                         31, 31, 30, 31, 30, 31};
  if (IsLeapYear(year)) {
    return kDaysInMonthLeap[month - 1];
  }
  return kDaysInMonth[month - 1];
}

int TextVerbPhrase(std::string* dest, const DbgenDistributions& dists,
                   int stream, RandomState* rng) {
  if (!dest) {
    return 0;
  }
  std::string syntax;
  if (PickString(*dists.vp, stream, rng, &syntax) < 0) {
    return 0;
  }

  int res = 0;
  std::size_t cursor = 0;
  std::string_view token;
  while (NextToken(syntax, &cursor, &token)) {
    const Distribution* src = nullptr;
    switch (token[0]) {
      case 'D':
        src = dists.adverbs;
        break;
      case 'V':
        src = dists.verbs;
        break;
      case 'X':
        src = dists.auxillaries;
        break;
      default:
        break;
    }
    if (!src) {
      continue;
    }
    std::string picked;
    if (PickString(*src, stream, rng, &picked) < 0) {
      continue;
    }
    int len = static_cast<int>(picked.size());
    dest->append(picked);
    res += len;
    if (token.size() > 1) {
      dest->push_back(token[1]);
      ++res;
    }
    dest->push_back(' ');
    ++res;
  }
  return res;
}

int TextNounPhrase(std::string* dest, const DbgenDistributions& dists,
                   int stream, RandomState* rng) {
  if (!dest) {
    return 0;
  }
  std::string syntax;
  if (PickString(*dists.np, stream, rng, &syntax) < 0) {
    return 0;
  }

  int res = 0;
  std::size_t cursor = 0;
  std::string_view token;
  while (NextToken(syntax, &cursor, &token)) {
    const Distribution* src = nullptr;
    switch (token[0]) {
      case 'A':
        src = dists.articles;
        break;
      case 'J':
        src = dists.adjectives;
        break;
      case 'D':
        src = dists.adverbs;
        break;
      case 'N':
        src = dists.nouns;
        break;
      default:
        break;
    }
    if (!src) {
      continue;
    }
    std::string picked;
    if (PickString(*src, stream, rng, &picked) < 0) {
      continue;
    }
    int len = static_cast<int>(picked.size());
    dest->append(picked);
    res += len;
    if (token.size() > 1) {
      dest->push_back(token[1]);
      ++res;
    }
    dest->push_back(' ');
    ++res;
  }
  return res;
}

int TextSentence(std::string* dest, const DbgenDistributions& dists, int stream,
                 RandomState* rng) {
  if (!dest) {
    return -1;
  }
  std::string syntax;
  if (PickString(*dists.grammar, stream, rng, &syntax) < 0) {
    return -1;
  }
  std::size_t cursor = 0;
  int res = 0;
  int len = 0;

  while (true) {
    while (cursor < syntax.size() && syntax[cursor] == ' ') {
      ++cursor;
    }
    if (cursor >= syntax.size()) {
      break;
    }
    switch (syntax[cursor]) {
      case 'V':
        len = TextVerbPhrase(dest, dists, stream, rng);
        break;
      case 'N':
        len = TextNounPhrase(dest, dists, stream, rng);
        break;
      case 'P': {
        std::string prep;
        if (PickString(*dists.prepositions, stream, rng, &prep) < 0) {
          len = 0;
          break;
        }
        dest->append(prep);
        len = static_cast<int>(prep.size());
        dest->append(" the ");
        len += 5;
        len += TextNounPhrase(dest, dists, stream, rng);
        break;
      }
      case 'T': {
        std::string term;
        if (PickString(*dists.terminators, stream, rng, &term) < 0) {
          len = 0;
          break;
        }
        if (!dest->empty()) {
          dest->pop_back();
        }
        dest->append(term);
        len = static_cast<int>(term.size());
        break;
      }
      default:
        len = 0;
        break;
    }
    res += len;
    ++cursor;
    if (cursor < syntax.size() && syntax[cursor] != ' ') {
      dest->push_back(syntax[cursor]);
      ++res;
    }
  }

  return --res;
}

}  // namespace

int RandomString(int min, int max, int stream, RandomState* rng,
                 std::string* dest) {
  if (!rng || !dest) {
    return 0;
  }
  int64_t len = rng->RandomInt(min, max, stream);
  if (len < 0) {
    dest->clear();
    return 0;
  }
  dest->assign(static_cast<std::size_t>(len), '\0');
  int64_t char_int = 0;
  for (int64_t i = 0; i < len; ++i) {
    if (i % 5 == 0) {
      char_int = rng->RandomInt(0, kMaxLong, stream);
    }
    (*dest)[static_cast<std::size_t>(i)] =
        kAlphaNum[static_cast<std::size_t>(char_int & 077)];
    char_int >>= 6;
  }
  return static_cast<int>(len);
}

int VariableString(int avg, int stream, RandomState* rng, std::string* dest) {
  int min_len = static_cast<int>(static_cast<double>(avg) * kVStrLow);
  int max_len = static_cast<int>(static_cast<double>(avg) * kVStrHigh);
  return RandomString(min_len, max_len, stream, rng, dest);
}

int PickString(const Distribution& dist, int stream, RandomState* rng,
               std::string* target) {
  if (!rng || dist.list.empty() || !target) {
    return -1;
  }
  int64_t j = rng->RandomInt(1, dist.list.back().weight, stream);
  std::size_t i = 0;
  while (dist.list[i].weight < j) {
    ++i;
  }
  *target = dist.list[i].text;
  return static_cast<int>(i);
}

void AggString(const Distribution& dist, int count, int stream,
               RandomState* rng, std::string* dest) {
  if (!rng || !dest || dist.list.empty() || count <= 0) {
    if (dest) {
      dest->clear();
    }
    return;
  }
  int dist_size = static_cast<int>(dist.list.size());
  std::vector<int> permute(dist_size);
  for (int i = 0; i < dist_size; ++i) {
    permute[i] = i;
  }
  for (int i = 0; i < dist_size; ++i) {
    int64_t source = rng->RandomInt(0, dist_size - 1, stream);
    std::swap(permute[static_cast<std::size_t>(source)], permute[i]);
  }

  std::string result;
  result.reserve(static_cast<std::size_t>(count) * 8);
  for (int i = 0; i < count; ++i) {
    if (i > 0) {
      result.push_back(' ');
    }
    result += dist.list[permute[i]].text;
  }
  *dest = std::move(result);
}

void GeneratePhone(int64_t ind, std::string* target, int stream,
                   RandomState* rng) {
  if (!rng || !target) {
    return;
  }
  int64_t acode = rng->RandomInt(100, 999, stream);
  int64_t exchg = rng->RandomInt(100, 999, stream);
  int64_t number = rng->RandomInt(1000, 9999, stream);
  char buffer[kPhoneLen + 1] = {};
  std::snprintf(buffer, sizeof(buffer), "%02d",
                10 + static_cast<int>(ind % kNationsMax));
  std::snprintf(buffer + 3, sizeof(buffer) - 3, "%03d",
                static_cast<int>(acode));
  std::snprintf(buffer + 7, sizeof(buffer) - 7, "%03d",
                static_cast<int>(exchg));
  std::snprintf(buffer + 11, sizeof(buffer) - 11, "%04d",
                static_cast<int>(number));
  buffer[2] = buffer[6] = buffer[10] = '-';
  *target = buffer;
}

void GenerateCategory(std::string* target, int stream, RandomState* rng) {
  if (!rng || !target) {
    return;
  }
  int64_t num1 = rng->RandomInt(1, 5, stream);
  int64_t num2 = rng->RandomInt(1, 5, stream);
  std::string result = "MFGR";
  result.push_back(static_cast<char>('0' + static_cast<int>(num1)));
  result.push_back(static_cast<char>('0' + static_cast<int>(num2)));
  *target = std::move(result);
}

int GenerateCity(std::string* city_name, const std::string& nation_name,
                 RandomState* rng) {
  if (!rng || !city_name) {
    return 0;
  }
  std::string result;
  result.reserve(static_cast<std::size_t>(kCityFix));
  if (nation_name.size() >= static_cast<std::size_t>(kCityFix - 1)) {
    result.assign(nation_name, 0, static_cast<std::size_t>(kCityFix - 1));
  } else {
    result = nation_name;
    result.append(
        static_cast<std::size_t>(kCityFix - 1 - result.size()), ' ');
  }
  int64_t random_pick = rng->RandomInt(0, 9, 98);
  result.push_back(static_cast<char>('0' + static_cast<int>(random_pick)));
  *city_name = std::move(result);
  return 0;
}

int GenerateColor(std::string* source, std::string* dest) {
  if (!source || !dest) {
    return 0;
  }
  std::size_t pos = source->find(' ');
  if (pos == std::string::npos) {
    *dest = *source;
    source->clear();
  } else {
    *dest = source->substr(0, pos);
    if (pos + 1 < source->size()) {
      *source = source->substr(pos + 1);
    } else {
      source->clear();
    }
  }
  return static_cast<int>(dest->size());
}

int GenerateText(int min, int max, const DbgenDistributions& dists, int stream,
                 RandomState* rng, std::string* dest) {
  if (!rng || !dest) {
    return 0;
  }
  int64_t length = rng->RandomInt(min, max, stream);
  if (length < 0) {
    dest->clear();
    return 0;
  }
  dest->clear();
  dest->reserve(static_cast<std::size_t>(length));
  int wordlen = 0;
  std::string sentence;

  while (wordlen < length) {
    sentence.clear();
    int s_len = TextSentence(&sentence, dists, stream, rng);
    if (s_len < 0) {
      return 0;
    }
    int needed = static_cast<int>(length) - wordlen;
    if (needed >= s_len + 1) {
      if (s_len > 0) {
        dest->append(sentence, 0,
                     static_cast<std::size_t>(
                         std::min(s_len,
                                  static_cast<int>(sentence.size()))));
      }
      wordlen += s_len + 1;
      dest->push_back(' ');
    } else if (needed > 0) {
      dest->append(sentence, 0,
                   static_cast<std::size_t>(
                       std::min(needed,
                                static_cast<int>(sentence.size()))));
      wordlen += needed;
    }
  }
  return wordlen;
}

int64_t RetailPrice(int64_t partkey) {
  int64_t price = 90000;
  price += (partkey / 10) % 20001;
  price += (partkey % 1000) * 100;
  return price;
}

void BuildAscDate(std::vector<std::string>* out) {
  if (!out) {
    return;
  }
  out->clear();
  out->reserve(kTotalDate);
  for (int64_t i = 0; i < kTotalDate; ++i) {
    out->push_back(MakeAsciiDate(i + 1));
  }
}

void GenerateDateRow(int64_t index, date_t* out) {
  if (!out) {
    return;
  }
  int64_t espan = (index - 1) * 60 * 60 * 24;
  std::time_t num_date_time = static_cast<std::time_t>(kDStartDate + espan);
  std::tm local_time = {};
  if (!LocalTimeSafe(&num_date_time, &local_time)) {
    return;
  }

  out->daynuminweek = (static_cast<int>(local_time.tm_wday) + 1) % 7 + 1;
  out->monthnuminyear = static_cast<int>(local_time.tm_mon) + 1;
  std::strncpy(out->dayofweek, kWeekdayNames[out->daynuminweek - 1],
               kDDayweekLen + 1);
  std::strncpy(out->month, kMonthNames[out->monthnuminyear - 1],
               kDMonthLen + 1);
  out->year = static_cast<int>(local_time.tm_year) + 1900;
  out->daynuminmonth = static_cast<int>(local_time.tm_mday);
  out->yearmonthnum = out->year * 100 + out->monthnuminyear;

  std::snprintf(out->yearmonth, sizeof(out->yearmonth), "%.3s%d", out->month,
                out->year);
  std::snprintf(out->date, sizeof(out->date), "%s %d, %d", out->month,
                out->daynuminmonth, out->year);
  out->datekey = static_cast<int64_t>(out->year) * 10000 +
                 static_cast<int64_t>(out->monthnuminyear) * 100 +
                 static_cast<int64_t>(out->daynuminmonth);

  out->daynuminyear = static_cast<int>(local_time.tm_yday) + 1;
  out->weeknuminyear = out->daynuminyear / 7 + 1;

  out->lastdayinweekfl[0] = (out->daynuminweek == 7) ? '1' : '0';
  out->lastdayinweekfl[1] = '\0';

  out->lastdayinmonthfl[0] =
      (DaysInMonth(out->year, out->monthnuminyear) == out->daynuminmonth) ? '0'
                                                                          : '1';
  out->lastdayinmonthfl[1] = '\0';

  out->weekdayfl[0] =
      (out->daynuminweek != 1 && out->daynuminweek != 7) ? '1' : '0';
  out->weekdayfl[1] = '\0';

  std::string season;
  GenerateSeason(&season, out->monthnuminyear, out->daynuminmonth);
  std::snprintf(out->sellingseason, sizeof(out->sellingseason), "%s",
                season.c_str());
  out->slen = static_cast<int>(season.size());
  std::string holiday_flag;
  GenerateHolidayFlag(&holiday_flag, out->monthnuminyear, out->daynuminmonth);
  std::snprintf(out->holidayfl, sizeof(out->holidayfl), "%s",
                holiday_flag.c_str());
}

}  // namespace benchgen::ssb::internal
