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

#include <string>

#include "distribution/string_values_distribution.h"

namespace benchgen::tpcds::internal {

class DistributionProvider {
 public:
  DistributionProvider();

  const StringValuesDistribution& first_names() const { return first_names_; }
  const StringValuesDistribution& last_names() const { return last_names_; }
  const StringValuesDistribution& salutations() const { return salutations_; }
  const StringValuesDistribution& countries() const { return countries_; }
  const StringValuesDistribution& top_domains() const { return top_domains_; }

 private:
  StringValuesDistribution first_names_;
  StringValuesDistribution last_names_;
  StringValuesDistribution salutations_;
  StringValuesDistribution countries_;
  StringValuesDistribution top_domains_;
};

}  // namespace benchgen::tpcds::internal
