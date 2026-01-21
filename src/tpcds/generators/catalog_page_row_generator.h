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

struct CatalogPageRowData {
  int64_t catalog_page_sk = 0;
  std::string catalog_page_id;
  int32_t start_date_id = 0;
  int32_t end_date_id = 0;
  std::string department;
  int32_t catalog_number = 0;
  int32_t catalog_page_number = 0;
  std::string description;
  std::string type;
  int64_t null_bitmap = 0;
};

class CatalogPageRowGenerator {
 public:
  CatalogPageRowGenerator(double scale);

  void SkipRows(int64_t start_row);
  CatalogPageRowData GenerateRow(int64_t row_number);
  void ConsumeRemainingSeedsForRow();

 private:
  static std::vector<int> ColumnIds();

  Scaling scaling_;
  DstDistributionStore distribution_store_;
  RowStreams streams_;
  int pages_per_catalog_ = 0;
  int64_t start_julian_ = 0;
};

}  // namespace benchgen::tpcds::internal
