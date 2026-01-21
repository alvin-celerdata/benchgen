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
#include "utils/row_streams.h"

namespace benchgen::tpcds::internal {

struct ShipModeRowData {
  int64_t ship_mode_sk = 0;
  std::string ship_mode_id;
  std::string type;
  std::string code;
  std::string carrier;
  std::string contract;
  int64_t null_bitmap = 0;
};

class ShipModeRowGenerator {
 public:
  ShipModeRowGenerator(double scale);

  void SkipRows(int64_t start_row);
  ShipModeRowData GenerateRow(int64_t row_number);
  void ConsumeRemainingSeedsForRow();

 private:
  static std::vector<int> ColumnIds();

  Scaling scaling_;
  DstDistributionStore distribution_store_;
  RowStreams streams_;
  const DstDistribution* type_dist_ = nullptr;
  const DstDistribution* code_dist_ = nullptr;
  const DstDistribution* carrier_dist_ = nullptr;
};

}  // namespace benchgen::tpcds::internal
