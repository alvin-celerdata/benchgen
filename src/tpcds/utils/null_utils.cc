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

#include "utils/null_utils.h"

#include "utils/random_utils.h"
#include "utils/table_metadata.h"

namespace benchgen::tpcds::internal {

int64_t GenerateNullBitmap(int table_number, RandomNumberStream* stream) {
  const auto& meta = GetTableMetadata(table_number);
  int threshold = GenerateUniformRandomInt(0, 9999, stream);
  int64_t bitmap = GenerateUniformRandomKey(1, kMaxInt, stream);
  if (threshold < meta.null_pct) {
    return bitmap & ~meta.not_null_bitmap;
  }
  return 0;
}

bool IsNull(int64_t null_bitmap, int table_number, int column_id) {
  const auto& meta = GetTableMetadata(table_number);
  int bit = column_id - meta.first_column;
  return (null_bitmap & (1LL << bit)) != 0;
}

}  // namespace benchgen::tpcds::internal
