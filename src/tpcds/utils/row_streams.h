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

#include <cstddef>
#include <cstdint>
#include <unordered_map>
#include <vector>

#include "utils/column_streams.h"
#include "utils/random_number_stream.h"

namespace benchgen::tpcds::internal {

class RowStreams {
 public:
  explicit RowStreams(const std::vector<int>& column_ids);

  RandomNumberStream& Stream(int column_id);
  void SkipRows(int64_t row_count);
  void ConsumeRemainingSeedsForRow();

 private:
  struct Entry {
    int column_id = 0;
    RandomNumberStream stream;
  };

  std::vector<Entry> entries_;
  std::unordered_map<int, size_t> index_;
};

}  // namespace benchgen::tpcds::internal
