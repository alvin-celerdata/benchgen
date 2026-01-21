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

#include "distribution/string_values_distribution.h"

#include <cctype>
#include <fstream>
#include <stdexcept>
#include <string_view>

#include "distribution/dst_distribution.h"
#include "utils/random_utils.h"

namespace benchgen::tpcds::internal {
namespace {

std::string Trim(std::string_view input) {
  size_t start = 0;
  while (start < input.size() &&
         std::isspace(static_cast<unsigned char>(input[start]))) {
    ++start;
  }
  size_t end = input.size();
  while (end > start &&
         std::isspace(static_cast<unsigned char>(input[end - 1]))) {
    --end;
  }
  return std::string(input.substr(start, end - start));
}

std::vector<std::string> SplitEscaped(std::string_view input, char delimiter) {
  std::vector<std::string> parts;
  std::string current;
  bool escape = false;
  for (char c : input) {
    if (c == delimiter && !escape) {
      parts.push_back(Trim(current));
      current.clear();
      continue;
    }

    current.push_back(c);
    if (escape) {
      escape = false;
    } else if (c == '\\') {
      escape = true;
    }
  }
  parts.push_back(Trim(current));
  return parts;
}

std::string Unescape(std::string_view input) {
  std::string output;
  output.reserve(input.size());
  for (char c : input) {
    if (c != '\\') {
      output.push_back(c);
    }
  }
  return output;
}

bool IsValidUtf8(std::string_view input) {
  int remaining = 0;
  for (unsigned char c : input) {
    if (remaining == 0) {
      if (c <= 0x7F) {
        continue;
      }
      if (c >= 0xC2 && c <= 0xDF) {
        remaining = 1;
      } else if (c >= 0xE0 && c <= 0xEF) {
        remaining = 2;
      } else if (c >= 0xF0 && c <= 0xF4) {
        remaining = 3;
      } else {
        return false;
      }
    } else {
      if (c < 0x80 || c > 0xBF) {
        return false;
      }
      --remaining;
    }
  }
  return remaining == 0;
}

std::string Latin1ToUtf8(std::string_view input) {
  std::string output;
  output.reserve(input.size());
  for (unsigned char c : input) {
    if (c < 0x80) {
      output.push_back(static_cast<char>(c));
    } else {
      output.push_back(static_cast<char>(0xC0 | (c >> 6)));
      output.push_back(static_cast<char>(0x80 | (c & 0x3F)));
    }
  }
  return output;
}

std::string NormalizeValueEncoding(std::string_view input) {
  std::string unescaped = Unescape(input);
  if (IsValidUtf8(unescaped)) {
    return unescaped;
  }
  return Latin1ToUtf8(unescaped);
}

}  // namespace

std::string StringValuesDistribution::PickRandomValue(
    int value_list_index, int weight_list_index,
    RandomNumberStream* stream) const {
  int index = PickRandomIndex(weight_list_index, stream);
  return values_lists_.at(static_cast<size_t>(value_list_index))
      .at(static_cast<size_t>(index));
}

int StringValuesDistribution::PickRandomIndex(
    int weight_list_index, RandomNumberStream* stream) const {
  const auto& weights =
      weights_lists_.at(static_cast<size_t>(weight_list_index));
  int weight = GenerateUniformRandomInt(1, weights.back(), stream);
  for (size_t i = 0; i < weights.size(); ++i) {
    if (weight <= weights[i]) {
      return static_cast<int>(i);
    }
  }
  throw std::runtime_error("random weight exceeded distribution range");
}

int StringValuesDistribution::GetWeightForIndex(int index,
                                                int weight_list_index) const {
  const auto& weights =
      weights_lists_.at(static_cast<size_t>(weight_list_index));
  if (index == 0) {
    return weights.front();
  }
  return weights.at(static_cast<size_t>(index)) -
         weights.at(static_cast<size_t>(index - 1));
}

const std::string& StringValuesDistribution::GetValueAtIndex(
    int value_list_index, int index) const {
  return values_lists_.at(static_cast<size_t>(value_list_index))
      .at(static_cast<size_t>(index));
}

StringValuesDistribution StringValuesDistribution::Load(
    const std::string& dir, const std::string& filename, int value_fields,
    int weight_fields) {
  std::ifstream file(dir + "/" + filename);
  if (!file.is_open()) {
    throw std::runtime_error("unable to open distribution file: " + filename);
  }

  std::vector<std::vector<std::string>> values_lists(
      static_cast<size_t>(value_fields));
  std::vector<std::vector<int>> weights_lists(
      static_cast<size_t>(weight_fields));
  std::vector<int> weight_accum(static_cast<size_t>(weight_fields), 0);

  std::string line;
  while (std::getline(file, line)) {
    std::string trimmed = Trim(line);
    if (trimmed.empty() || trimmed.rfind("--", 0) == 0) {
      continue;
    }
    auto parts = SplitEscaped(trimmed, ':');
    if (parts.size() != 2) {
      throw std::runtime_error("invalid distribution line: " + trimmed);
    }
    auto values = SplitEscaped(parts[0], ',');
    auto weights = SplitEscaped(parts[1], ',');
    for (auto& value : values) {
      value = NormalizeValueEncoding(value);
    }
    if (static_cast<int>(values.size()) != value_fields) {
      throw std::runtime_error("unexpected value field count in: " + trimmed);
    }
    if (static_cast<int>(weights.size()) != weight_fields) {
      throw std::runtime_error("unexpected weight field count in: " + trimmed);
    }

    for (int i = 0; i < value_fields; ++i) {
      values_lists[static_cast<size_t>(i)].push_back(values[i]);
    }
    for (int i = 0; i < weight_fields; ++i) {
      int w = std::stoi(weights[i]);
      weight_accum[static_cast<size_t>(i)] += w;
      weights_lists[static_cast<size_t>(i)].push_back(
          weight_accum[static_cast<size_t>(i)]);
    }
  }

  StringValuesDistribution dist;
  dist.values_lists_ = std::move(values_lists);
  dist.weights_lists_ = std::move(weights_lists);
  return dist;
}

StringValuesDistribution StringValuesDistribution::FromDstDistribution(
    const DstDistribution& dist) {
  int value_fields = dist.value_set_count();
  int weight_fields = dist.weight_set_count();
  if (value_fields <= 0 || weight_fields <= 0) {
    throw std::runtime_error("invalid string distribution: " + dist.name());
  }

  StringValuesDistribution result;
  result.values_lists_.assign(static_cast<size_t>(value_fields), {});
  result.weights_lists_.assign(static_cast<size_t>(weight_fields), {});
  std::vector<int> weight_accum(static_cast<size_t>(weight_fields), 0);

  for (int row = 1; row <= dist.size(); ++row) {
    for (int value_set = 1; value_set <= value_fields; ++value_set) {
      result.values_lists_[static_cast<size_t>(value_set - 1)].push_back(
          dist.GetString(row, value_set));
    }
    for (int weight_set = 1; weight_set <= weight_fields; ++weight_set) {
      weight_accum[static_cast<size_t>(weight_set - 1)] +=
          dist.Weight(row, weight_set);
      result.weights_lists_[static_cast<size_t>(weight_set - 1)].push_back(
          weight_accum[static_cast<size_t>(weight_set - 1)]);
    }
  }

  return result;
}

}  // namespace benchgen::tpcds::internal
