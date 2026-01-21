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
#include <vector>

#include "utils/random_number_stream.h"

namespace benchgen::tpcds::internal {

class DstDistribution;

class StringValuesDistribution {
 public:
  StringValuesDistribution() = default;

  std::string PickRandomValue(int value_list_index, int weight_list_index,
                              RandomNumberStream* stream) const;
  int PickRandomIndex(int weight_list_index, RandomNumberStream* stream) const;
  int GetWeightForIndex(int index, int weight_list_index) const;
  const std::string& GetValueAtIndex(int value_list_index, int index) const;

  static StringValuesDistribution Load(const std::string& dir,
                                       const std::string& filename,
                                       int value_fields, int weight_fields);
  static StringValuesDistribution FromDstDistribution(
      const DstDistribution& dist);

 private:
  std::vector<std::vector<std::string>> values_lists_;
  std::vector<std::vector<int>> weights_lists_;
};

}  // namespace benchgen::tpcds::internal
