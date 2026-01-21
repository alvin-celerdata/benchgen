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

#include "generators/reason_row_generator.h"

#include "utils/random_utils.h"

namespace benchgen::tpcds::internal {

ReasonRowGenerator::ReasonRowGenerator()
    : distribution_store_() {
  return_reasons_ = &distribution_store_.Get("return_reasons");
}

ReasonRowData ReasonRowGenerator::GenerateRow(int64_t row_number) {
  ReasonRowData row;
  int index = static_cast<int>(row_number);
  row.reason_sk = index;
  row.reason_id = MakeBusinessKey(static_cast<uint64_t>(index));
  row.reason_description = return_reasons_->GetString(index, 1);
  return row;
}

}  // namespace benchgen::tpcds::internal
