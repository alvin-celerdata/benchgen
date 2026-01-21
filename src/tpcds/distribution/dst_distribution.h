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
#include <string>
#include <string_view>
#include <vector>

#include "utils/date.h"
#include "utils/random_number_stream.h"

namespace benchgen::tpcds::internal {

enum class DstValueType { kVarchar, kInt, kDate, kDecimal };

class DstDistribution {
 public:
  explicit DstDistribution(std::string name = {});

  const std::string& name() const { return name_; }

  void SetTypes(std::vector<DstValueType> types);
  void SetWeightSetCount(int count);
  void SetNames(std::vector<std::string> value_names,
                std::vector<std::string> weight_names);
  void AddEntry(const std::vector<std::string>& values,
                const std::vector<int>& weights);

  int size() const { return size_; }
  int value_set_count() const { return static_cast<int>(types_.size()); }
  int weight_set_count() const { return static_cast<int>(weight_sets_.size()); }

  DstValueType type(int value_set) const;
  const std::string& GetString(int index, int value_set) const;
  int GetInt(int index, int value_set) const;
  double GetDecimal(int index, int value_set) const;
  Date GetDate(int index, int value_set) const;

  int PickIndex(int weight_set, RandomNumberStream* stream) const;
  int Weight(int index, int weight_set) const;
  int MaxWeight(int weight_set) const;

 private:
  std::string name_;
  std::vector<DstValueType> types_;
  std::vector<std::vector<std::string>> values_;
  std::vector<std::vector<int>> weight_sets_;
  std::vector<int> maximums_;
  std::vector<std::string> value_names_;
  std::vector<std::string> weight_names_;
  int size_ = 0;
};

}  // namespace benchgen::tpcds::internal
