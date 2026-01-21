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

#include "utils/date.h"
#include "utils/decimal.h"
#include "utils/random_number_stream.h"

namespace benchgen::tpcds::internal {

class StringValuesDistribution;

enum class RandomDistribution {
  kUniform,
  kExponential,
};

int GenerateUniformRandomInt(int min, int max, RandomNumberStream* stream);
int64_t GenerateUniformRandomKey(int64_t min, int64_t max,
                                 RandomNumberStream* stream);
std::string GenerateRandomCharset(const std::string& charset, int min, int max,
                                  RandomNumberStream* stream);
Date GenerateUniformRandomDate(const Date& min, const Date& max,
                               RandomNumberStream* stream);

int GenerateRandomInt(RandomDistribution dist, int min, int max, int mean,
                      RandomNumberStream* stream);
int64_t GenerateRandomKey(RandomDistribution dist, int64_t min, int64_t max,
                          int64_t mean, RandomNumberStream* stream);
Decimal GenerateRandomDecimal(RandomDistribution dist, const Decimal& min,
                              const Decimal& max, const Decimal* mean,
                              RandomNumberStream* stream);

std::string MakeBusinessKey(uint64_t primary);

std::string GenerateRandomEmail(const std::string& first,
                                const std::string& last,
                                RandomNumberStream* stream,
                                const StringValuesDistribution& top_domains);
std::string GenerateRandomUrl(RandomNumberStream* stream);

}  // namespace benchgen::tpcds::internal
