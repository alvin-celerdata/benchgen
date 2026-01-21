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

#include "distribution/dst_distribution_store.h"
#include "utils/random_number_stream.h"

namespace benchgen::tpcds::internal {

struct HierarchyState {
  int last_category = -1;
  int last_class = -1;
  int brand_base = 0;
  std::string class_dist_name;
};

void HierarchyItem(int level, int64_t* id, std::string* name, int64_t index,
                   DstDistributionStore* store, RandomNumberStream* stream,
                   HierarchyState* state);

void MakeWord(std::string* dest, const std::string& syllable_set, int64_t src,
              int char_count, DstDistributionStore* store);

void MakeCompanyName(std::string* dest, int table_number, int company,
                     DstDistributionStore* store);

void EmbedString(std::string* dest, const std::string& dist_name, int value_set,
                 int weight_set, DstDistributionStore* store,
                 RandomNumberStream* stream);

}  // namespace benchgen::tpcds::internal
