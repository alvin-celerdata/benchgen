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

#include "utils/random_utils.h"

#include <algorithm>
#include <cmath>
#include <sstream>

#include "distribution/string_values_distribution.h"

namespace benchgen::tpcds::internal {

int GenerateUniformRandomInt(int min, int max, RandomNumberStream* stream) {
  int32_t result = static_cast<int32_t>(stream->NextRandom());
  result %= (max - min + 1);
  result += min;
  return result;
}

int64_t GenerateUniformRandomKey(int64_t min, int64_t max,
                                 RandomNumberStream* stream) {
  int32_t result = static_cast<int32_t>(stream->NextRandom());
  result %= static_cast<int32_t>(max - min + 1);
  result += static_cast<int32_t>(min);
  return static_cast<int64_t>(result);
}

std::string GenerateRandomCharset(const std::string& charset, int min, int max,
                                  RandomNumberStream* stream) {
  int length = GenerateUniformRandomInt(min, max, stream);
  std::string output;
  output.reserve(length);
  for (int i = 0; i < max; ++i) {
    int index = GenerateUniformRandomInt(
        0, static_cast<int>(charset.size() - 1), stream);
    if (i < length) {
      output.push_back(charset[static_cast<size_t>(index)]);
    }
  }
  return output;
}

Date GenerateUniformRandomDate(const Date& min, const Date& max,
                               RandomNumberStream* stream) {
  int range = Date::ToJulianDays(max) - Date::ToJulianDays(min);
  int julian =
      Date::ToJulianDays(min) + GenerateUniformRandomInt(0, range, stream);
  return Date::FromJulianDays(julian);
}

int GenerateRandomInt(RandomDistribution dist, int min, int max, int mean,
                      RandomNumberStream* stream) {
  switch (dist) {
    case RandomDistribution::kUniform: {
      int64_t result = stream->NextRandom();
      result %= static_cast<int64_t>(max - min + 1);
      result += min;
      return static_cast<int>(result);
    }
    case RandomDistribution::kExponential: {
      double fres = 0.0;
      for (int i = 0; i < 12; ++i) {
        fres += static_cast<double>(stream->NextRandom()) /
                    static_cast<double>(kMaxInt) -
                0.5;
      }
      return min + static_cast<int>((max - min + 1) * fres);
    }
  }
  return min;
}

int64_t GenerateRandomKey(RandomDistribution dist, int64_t min, int64_t max,
                          int64_t mean, RandomNumberStream* stream) {
  switch (dist) {
    case RandomDistribution::kUniform: {
      int64_t result = stream->NextRandom();
      result %= (max - min + 1);
      result += min;
      return result;
    }
    case RandomDistribution::kExponential: {
      double fres = 0.0;
      for (int i = 0; i < 12; ++i) {
        fres += static_cast<double>(stream->NextRandom()) /
                    static_cast<double>(kMaxInt) -
                0.5;
      }
      return min + static_cast<int64_t>((max - min + 1) * fres);
    }
  }
  return min;
}

Decimal GenerateRandomDecimal(RandomDistribution dist, const Decimal& min,
                              const Decimal& max, const Decimal* mean,
                              RandomNumberStream* stream) {
  Decimal dest;
  dest.precision =
      min.precision < max.precision ? min.precision : max.precision;
  Decimal res;
  switch (dist) {
    case RandomDistribution::kUniform: {
      int64_t value = stream->NextRandom();
      value %= (max.number - min.number + 1);
      value += min.number;
      res.number = value;
      break;
    }
    case RandomDistribution::kExponential: {
      double fres = 0.0;
      for (int i = 0; i < 12; ++i) {
        fres /= 2.0;
        fres += static_cast<double>(stream->NextRandom()) /
                    static_cast<double>(kMaxInt) -
                0.5;
      }
      int64_t base = mean != nullptr ? mean->number : 0;
      res.number =
          base + static_cast<int64_t>((max.number - min.number + 1) * fres);
      break;
    }
  }

  dest.number = res.number;
  int scale = 0;
  int64_t tmp = res.number;
  while (tmp > 10) {
    tmp /= 10;
    ++scale;
  }
  dest.scale = scale;
  return dest;
}

std::string MakeBusinessKey(uint64_t primary) {
  static constexpr char kKeyChars[] = "ABCDEFGHIJKLMNOP";
  auto to_eight_chars = [](uint64_t value) {
    std::string out(8, 'A');
    for (int i = 0; i < 8; ++i) {
      out[static_cast<size_t>(i)] = kKeyChars[static_cast<size_t>(value & 0xF)];
      value >>= 4;
    }
    return out;
  };
  std::string high = to_eight_chars(primary >> 32);
  std::string low = to_eight_chars(primary);
  return high + low;
}

std::string GenerateRandomEmail(const std::string& first,
                                const std::string& last,
                                RandomNumberStream* stream,
                                const StringValuesDistribution& top_domains) {
  static const std::string kAlphaNum =
      "abcdefghijklmnopqrstuvxyzABCDEFGHIJKLMNOPQRSTUVXYZ0123456789";
  std::string domain = top_domains.PickRandomValue(0, 0, stream);
  int company_length = GenerateUniformRandomInt(10, 20, stream);
  std::string company = GenerateRandomCharset(kAlphaNum, 1, 20, stream);
  if (static_cast<int>(company.size()) > company_length) {
    company.resize(static_cast<size_t>(company_length));
  }
  std::ostringstream out;
  out << first << "." << last << "@" << company << "." << domain;
  return out.str();
}

std::string GenerateRandomUrl(RandomNumberStream* stream) {
  (void)stream;
  return "http://www.foo.com";
}

}  // namespace benchgen::tpcds::internal
