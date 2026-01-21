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

#include "utils/row_streams.h"

#include <stdexcept>

#include "utils/random_utils.h"

namespace benchgen::tpcds::internal {

RowStreams::RowStreams(const std::vector<int>& column_ids) {
  entries_.reserve(column_ids.size());
  index_.reserve(column_ids.size());
  for (size_t i = 0; i < column_ids.size(); ++i) {
    int column_id = column_ids[i];
    int seeds = SeedsPerRow(column_id);
    Entry entry{column_id, RandomNumberStream(column_id, seeds)};
    entries_.push_back(std::move(entry));
    index_.emplace(column_id, i);
  }
}

RandomNumberStream& RowStreams::Stream(int column_id) {
  auto it = index_.find(column_id);
  if (it == index_.end()) {
    throw std::out_of_range("unknown column id");
  }
  return entries_[it->second].stream;
}

void RowStreams::SkipRows(int64_t row_count) {
  for (auto& entry : entries_) {
    entry.stream.SkipRows(row_count);
  }
}

void RowStreams::ConsumeRemainingSeedsForRow() {
  for (auto& entry : entries_) {
    auto& stream = entry.stream;
    while (stream.seeds_used() < stream.seeds_per_row()) {
      GenerateUniformRandomInt(1, 100, &stream);
    }
    stream.ResetSeedsUsed();
  }
}

}  // namespace benchgen::tpcds::internal
