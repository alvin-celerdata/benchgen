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
#include "utils/scd.h"

namespace benchgen::tpcds::internal {

struct WebPageRowData {
  int64_t page_sk = 0;
  std::string page_id;
  int32_t rec_start_date_id = 0;
  int32_t rec_end_date_id = 0;
  int32_t creation_date_sk = 0;
  int32_t access_date_sk = 0;
  bool autogen_flag = false;
  int64_t customer_sk = 0;
  std::string url;
  std::string type;
  int32_t char_count = 0;
  int32_t link_count = 0;
  int32_t image_count = 0;
  int32_t max_ad_count = 0;
  int64_t null_bitmap = 0;
};

class WebPageRowGenerator {
 public:
  WebPageRowGenerator(double scale);

  void SkipRows(int64_t start_row);
  WebPageRowData GenerateRow(int64_t row_number);
  void ConsumeRemainingSeedsForRow();

 private:
  static std::vector<int> ColumnIds();

  Scaling scaling_;
  DstDistributionStore distribution_store_;
  RowStreams streams_;
  WebPageRowData old_values_;
  bool old_values_initialized_ = false;
  ScdState scd_state_;
  int32_t today_julian_ = 0;
};

}  // namespace benchgen::tpcds::internal
