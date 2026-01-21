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

#include "benchgen/table.h"
#include "distribution/dst_distribution_store.h"
#include "utils/tables.h"

namespace benchgen::tpcds::internal {

class Scaling {
 public:
  explicit Scaling(double scale);

  int64_t RowCount(benchgen::tpcds::TableId table) const;
  int64_t RowCountByTableNumber(int table_number) const;
  int64_t IdCount(int table_number) const;
  int ScaleIndex() const;

 private:
  int64_t RowCountForTableNumber(int table_number) const;
  int64_t BaseRowCount(int table_number) const;
  int64_t RowCountAtScale(int table_number, int scale_slot) const;

  static int ScaleSlot(double scale);
  int64_t LinearScale(int table_number) const;
  int64_t LogScale(int table_number) const;

  double scale_ = 1.0;
  DstDistributionStore distribution_store_;
  const DstDistribution* rowcounts_ = nullptr;
};

}  // namespace benchgen::tpcds::internal
