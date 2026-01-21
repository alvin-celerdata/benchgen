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

#include <string>

#include "distribution/dst_distribution_store.h"
#include "distribution/scaling.h"
#include "utils/random_number_stream.h"

namespace benchgen::tpcds::internal {

struct Address {
  int street_num = 0;
  std::string street_name1;
  std::string street_name2;
  std::string street_type;
  std::string suite_num;
  std::string city;
  std::string county;
  std::string state;
  std::string country;
  int zip = 0;
  int plus4 = 0;
  int gmt_offset = 0;
};

Address GenerateAddress(int table_number, DstDistributionStore* store,
                        RandomNumberStream* stream, const Scaling& scaling);
int CityHash(const std::string& name);

}  // namespace benchgen::tpcds::internal
