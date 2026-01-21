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

#include "generators/income_band_row_generator.h"

namespace benchgen::tpcds::internal {

IncomeBandRowGenerator::IncomeBandRowGenerator(
    )
    : distribution_store_() {
  income_band_ = &distribution_store_.Get("income_band");
}

IncomeBandRowData IncomeBandRowGenerator::GenerateRow(int64_t row_number) {
  IncomeBandRowData row;
  int index = static_cast<int>(row_number);
  row.income_band_sk = index;
  row.lower_bound = income_band_->GetInt(index, 1);
  row.upper_bound = income_band_->GetInt(index, 2);
  return row;
}

}  // namespace benchgen::tpcds::internal
