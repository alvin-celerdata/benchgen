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

#include "utils/address.h"

#include <algorithm>

#include "utils/random_utils.h"
#include "utils/table_metadata.h"

namespace benchgen::tpcds::internal {
namespace {

std::string PickString(const DstDistribution& dist, int value_set,
                       int weight_set, RandomNumberStream* stream) {
  int index = dist.PickIndex(weight_set, stream);
  return dist.GetString(index, value_set);
}

int PickIndexUniform(int max_value, RandomNumberStream* stream) {
  return GenerateUniformRandomInt(1, max_value, stream);
}

std::string FormatSuiteNumber(int seed) {
  if (seed & 0x01) {
    return "Suite " + std::to_string((seed >> 1) * 10);
  }
  char letter = static_cast<char>('A' + ((seed >> 1) % 25));
  std::string out = "Suite ";
  out.push_back(letter);
  return out;
}

}  // namespace

Address GenerateAddress(int table_number, DstDistributionStore* store,
                        RandomNumberStream* stream, const Scaling& scaling) {
  Address address;
  address.street_num = GenerateUniformRandomInt(1, 1000, stream);

  const auto& street_names = store->Get("street_names");
  address.street_name1 = PickString(street_names, 1, 1, stream);
  address.street_name2 = PickString(street_names, 1, 2, stream);

  const auto& street_type = store->Get("street_type");
  address.street_type = PickString(street_type, 1, 1, stream);

  int suite_seed = GenerateUniformRandomInt(1, 100, stream);
  address.suite_num = FormatSuiteNumber(suite_seed);

  const auto& cities = store->Get("cities");
  if (IsSmallTable(table_number)) {
    int max_cities =
        static_cast<int>(scaling.RowCountByTableNumber(ACTIVE_CITIES));
    int limit =
        std::min(max_cities,
                 static_cast<int>(scaling.RowCountByTableNumber(table_number)));
    int index = PickIndexUniform(limit, stream);
    address.city = cities.GetString(index, 1);
  } else {
    address.city = PickString(cities, 1, 6, stream);
  }

  const auto& fips = store->Get("fips_county");
  int region_index = 0;
  if (IsSmallTable(table_number)) {
    int max_counties =
        static_cast<int>(scaling.RowCountByTableNumber(ACTIVE_COUNTIES));
    int limit =
        std::min(max_counties,
                 static_cast<int>(scaling.RowCountByTableNumber(table_number)));
    region_index = PickIndexUniform(limit, stream);
    address.county = fips.GetString(region_index, 2);
  } else {
    region_index = fips.PickIndex(1, stream);
    address.county = fips.GetString(region_index, 2);
  }

  address.state = fips.GetString(region_index, 3);

  std::string zip_prefix = fips.GetString(region_index, 5);
  int zip = CityHash(address.city);
  if (!zip_prefix.empty() && zip_prefix.front() == '0' && zip < 9400) {
    zip += 600;
  }
  if (!zip_prefix.empty()) {
    zip += (zip_prefix.front() - '0') * 10000;
  }
  address.zip = zip;

  std::string address_line = std::to_string(address.street_num) + " " +
                             address.street_name1 + " " + address.street_name2 +
                             " " + address.street_type;
  address.plus4 = CityHash(address_line);

  address.gmt_offset = fips.GetInt(region_index, 6);
  address.country = "United States";

  return address;
}

int CityHash(const std::string& name) {
  int hash_value = 0;
  int result = 0;
  for (char c : name) {
    hash_value *= 26;
    hash_value -= 'A';
    hash_value += static_cast<unsigned char>(c);
    if (hash_value > 1000000) {
      hash_value %= 10000;
      result += hash_value;
      hash_value = 0;
    }
  }
  hash_value %= 1000;
  result += hash_value;
  result %= 10000;
  return result;
}

}  // namespace benchgen::tpcds::internal
