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

#include "utils/permute.h"

#include <stdexcept>

#include "utils/random_utils.h"

namespace benchgen::tpcds::internal {

std::vector<int> MakePermutation(int size, RandomNumberStream* stream) {
  if (size <= 0) {
    return {};
  }
  if (stream == nullptr) {
    throw std::invalid_argument("stream must not be null");
  }

  std::vector<int> values(static_cast<size_t>(size));
  for (int i = 0; i < size; ++i) {
    values[static_cast<size_t>(i)] = i;
  }

  for (int i = 0; i < size; ++i) {
    int index = GenerateUniformRandomInt(0, size - 1, stream);
    int temp = values[static_cast<size_t>(i)];
    values[static_cast<size_t>(i)] = values[static_cast<size_t>(index)];
    values[static_cast<size_t>(index)] = temp;
  }

  return values;
}

int GetPermutationEntry(const std::vector<int>& permutation, int index) {
  if (index <= 0 || static_cast<size_t>(index) > permutation.size()) {
    throw std::out_of_range("permutation index out of range");
  }
  return permutation[static_cast<size_t>(index - 1)] + 1;
}

}  // namespace benchgen::tpcds::internal
