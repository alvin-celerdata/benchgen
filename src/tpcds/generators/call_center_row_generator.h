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
#include "utils/decimal.h"
#include "utils/row_streams.h"
#include "utils/scd.h"

namespace benchgen::tpcds::internal {

struct CallCenterRowData {
  int64_t call_center_sk = 0;
  std::string call_center_id;
  int32_t rec_start_date_id = 0;
  int32_t rec_end_date_id = 0;
  int32_t closed_date_id = -1;
  int32_t open_date_id = 0;
  std::string name;
  std::string class_name;
  int32_t employees = 0;
  int32_t sq_ft = 0;
  std::string hours;
  std::string manager;
  int32_t market_id = 0;
  std::string market_class;
  std::string market_desc;
  std::string market_manager;
  int32_t division_id = 0;
  std::string division_name;
  int32_t company = 0;
  std::string company_name;
  Address address;
  Decimal tax_percentage;
  int64_t null_bitmap = 0;
};

class CallCenterRowGenerator {
 public:
  CallCenterRowGenerator(double scale);

  void SkipRows(int64_t start_row);
  CallCenterRowData GenerateRow(int64_t row_number);
  void ConsumeRemainingSeedsForRow();

 private:
  static std::vector<int> ColumnIds();

  double scale_ = 1.0;
  Scaling scaling_;
  DstDistributionStore distribution_store_;
  RowStreams streams_;
  CallCenterRowData old_values_;
  bool old_values_initialized_ = false;
  ScdState scd_state_;
  Decimal min_tax_;
  Decimal max_tax_;
  int64_t open_date_base_ = 0;
};

}  // namespace benchgen::tpcds::internal
