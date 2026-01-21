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
#include <vector>

#include "distribution/dst_distribution_store.h"
#include "distribution/scaling.h"
#include "utils/address.h"
#include "utils/row_streams.h"

namespace benchgen::tpcds::internal {

struct CustomerAddressRowData {
  int64_t address_sk = 0;
  std::string address_id;
  int32_t street_num = 0;
  std::string street_name;
  std::string street_type;
  std::string suite_num;
  std::string city;
  std::string county;
  std::string state;
  std::string zip;
  std::string country;
  int32_t gmt_offset = 0;
  std::string location_type;
  int64_t null_bitmap = 0;
};

class CustomerAddressRowGenerator {
 public:
  CustomerAddressRowGenerator(double scale);

  void SkipRows(int64_t start_row);
  CustomerAddressRowData GenerateRow(int64_t row_number);
  void ConsumeRemainingSeedsForRow();

 private:
  static std::vector<int> ColumnIds();

  Scaling scaling_;
  DstDistributionStore distribution_store_;
  RowStreams streams_;
  const DstDistribution* location_type_ = nullptr;
};

}  // namespace benchgen::tpcds::internal
