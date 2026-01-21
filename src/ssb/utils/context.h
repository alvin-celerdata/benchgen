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

#include <arrow/status.h>

#include <string>
#include <vector>

#include "benchgen/table.h"
#include "distribution/distribution_provider.h"

namespace benchgen::ssb::internal {

class RandomState;

struct DbgenDistributions {
  arrow::Status Init(const DistributionStore& store);

  const Distribution* p_cntr = nullptr;
  const Distribution* colors = nullptr;
  const Distribution* p_types = nullptr;
  const Distribution* nations = nullptr;
  const Distribution* regions = nullptr;
  const Distribution* o_priority = nullptr;
  const Distribution* l_instruct = nullptr;
  const Distribution* l_smode = nullptr;
  const Distribution* l_category = nullptr;
  const Distribution* l_rflag = nullptr;
  const Distribution* c_mseg = nullptr;
  const Distribution* nouns = nullptr;
  const Distribution* verbs = nullptr;
  const Distribution* adjectives = nullptr;
  const Distribution* adverbs = nullptr;
  const Distribution* auxillaries = nullptr;
  const Distribution* terminators = nullptr;
  const Distribution* articles = nullptr;
  const Distribution* prepositions = nullptr;
  const Distribution* grammar = nullptr;
  const Distribution* np = nullptr;
  const Distribution* vp = nullptr;
};

class DbgenContext {
 public:
  arrow::Status Init(double scale_factor);
  const DbgenDistributions& distributions() const { return distributions_; }
  const std::vector<std::string>& asc_date() const { return asc_date_; }
  bool initialized() const { return initialized_; }

 private:
  DistributionProvider provider_;
  DbgenDistributions distributions_;
  std::vector<std::string> asc_date_;
  bool initialized_ = false;
};

arrow::Status AdvanceSeedsForTable(RandomState* rng, TableId table,
                                   double scale_factor);

}  // namespace benchgen::ssb::internal
