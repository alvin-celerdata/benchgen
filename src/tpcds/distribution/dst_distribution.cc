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

#include "distribution/dst_distribution.h"

#include <algorithm>
#include <stdexcept>

#include "utils/random_utils.h"

namespace benchgen::tpcds::internal {

DstDistribution::DstDistribution(std::string name) : name_(std::move(name)) {}

void DstDistribution::SetTypes(std::vector<DstValueType> types) {
  types_ = std::move(types);
  values_.assign(types_.size(), {});
}

void DstDistribution::SetWeightSetCount(int count) {
  if (count < 0) {
    throw std::invalid_argument("weight set count must be non-negative");
  }
  weight_sets_.assign(static_cast<size_t>(count), {});
  maximums_.assign(static_cast<size_t>(count), 0);
}

void DstDistribution::SetNames(std::vector<std::string> value_names,
                               std::vector<std::string> weight_names) {
  value_names_ = std::move(value_names);
  weight_names_ = std::move(weight_names);
}

void DstDistribution::AddEntry(const std::vector<std::string>& values,
                               const std::vector<int>& weights) {
  if (!types_.empty() && values.size() != types_.size()) {
    throw std::runtime_error("distribution value count mismatch for " + name_);
  }
  if (!weight_sets_.empty() && weights.size() != weight_sets_.size()) {
    throw std::runtime_error("distribution weight count mismatch for " + name_);
  }

  if (values_.empty()) {
    values_.assign(values.size(), {});
  }

  for (size_t i = 0; i < values.size(); ++i) {
    values_[i].push_back(values[i]);
  }
  for (size_t i = 0; i < weights.size(); ++i) {
    maximums_[i] += weights[i];
    weight_sets_[i].push_back(maximums_[i]);
  }
  ++size_;
}

DstValueType DstDistribution::type(int value_set) const {
  return types_.at(static_cast<size_t>(value_set - 1));
}

const std::string& DstDistribution::GetString(int index, int value_set) const {
  return values_.at(static_cast<size_t>(value_set - 1))
      .at(static_cast<size_t>(index - 1));
}

int DstDistribution::GetInt(int index, int value_set) const {
  const std::string& value = GetString(index, value_set);
  return std::stoi(value);
}

double DstDistribution::GetDecimal(int index, int value_set) const {
  const std::string& value = GetString(index, value_set);
  return std::stod(value);
}

Date DstDistribution::GetDate(int index, int value_set) const {
  const std::string& value = GetString(index, value_set);
  return Date::FromString(value);
}

int DstDistribution::PickIndex(int weight_set,
                               RandomNumberStream* stream) const {
  if (weight_set <= 0 || weight_set > weight_set_count()) {
    throw std::out_of_range("weight_set out of range");
  }
  const auto& weights = weight_sets_.at(static_cast<size_t>(weight_set - 1));
  int max_weight = maximums_.at(static_cast<size_t>(weight_set - 1));
  int pick = GenerateUniformRandomInt(1, max_weight, stream);
  auto it = std::lower_bound(weights.begin(), weights.end(), pick);
  return static_cast<int>(std::distance(weights.begin(), it)) + 1;
}

int DstDistribution::Weight(int index, int weight_set) const {
  if (weight_set <= 0 || weight_set > weight_set_count()) {
    throw std::out_of_range("weight_set out of range");
  }
  const auto& weights = weight_sets_.at(static_cast<size_t>(weight_set - 1));
  int res = weights.at(static_cast<size_t>(index - 1));
  if (index > 1) {
    res -= weights.at(static_cast<size_t>(index - 2));
  }
  return res;
}

int DstDistribution::MaxWeight(int weight_set) const {
  if (weight_set <= 0 || weight_set > weight_set_count()) {
    throw std::out_of_range("weight_set out of range");
  }
  return maximums_.at(static_cast<size_t>(weight_set - 1));
}

}  // namespace benchgen::tpcds::internal
