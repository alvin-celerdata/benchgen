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

#include "utils/build_support.h"

#include <stdexcept>

#include "distribution/dst_distribution_utils.h"
#include "utils/columns.h"
#include "utils/random_utils.h"

namespace benchgen::tpcds::internal {
namespace {

HierarchyState& CheckedHierarchyState(HierarchyState* state) {
  if (state == nullptr) {
    throw std::invalid_argument("hierarchy state must not be null");
  }
  return *state;
}

}  // namespace

void HierarchyItem(int level, int64_t* id, std::string* name, int64_t index,
                   DstDistributionStore* store, RandomNumberStream* stream,
                   HierarchyState* state) {
  if (store == nullptr) {
    throw std::invalid_argument("distribution store must not be null");
  }
  HierarchyState& state_ref = CheckedHierarchyState(state);

  switch (level) {
    case I_CATEGORY: {
      const auto& categories = store->Get("categories");
      int picked = categories.PickIndex(1, stream);
      if (name != nullptr) {
        *name = categories.GetString(picked, 1);
      }
      if (id != nullptr) {
        *id = picked;
      }
      state_ref.last_category = picked;
      state_ref.brand_base = picked;
      state_ref.last_class = -1;
      break;
    }
    case I_CLASS: {
      if (state_ref.last_category == -1) {
        throw std::runtime_error("I_CLASS before I_CATEGORY");
      }
      const auto& categories = store->Get("categories");
      state_ref.class_dist_name =
          categories.GetString(state_ref.last_category, 2);
      const auto& class_dist = store->Get(state_ref.class_dist_name);
      int picked = class_dist.PickIndex(1, stream);
      if (name != nullptr) {
        *name = class_dist.GetString(picked, 1);
      }
      if (id != nullptr) {
        *id = picked;
      }
      state_ref.last_class = picked;
      state_ref.last_category = -1;
      break;
    }
    case I_BRAND: {
      if (state_ref.last_class == -1) {
        throw std::runtime_error("I_BRAND before I_CLASS");
      }
      const auto& class_dist = store->Get(state_ref.class_dist_name);
      int brand_count = class_dist.GetInt(state_ref.last_class, 2);
      if (brand_count <= 0) {
        throw std::runtime_error("invalid brand count");
      }
      int64_t brand_id = (index % brand_count) + 1;
      std::string brand_name;
      MakeWord(&brand_name, "brand_syllables",
               static_cast<int64_t>(state_ref.brand_base * 10 +
                                    state_ref.last_class),
               45, store);
      brand_name.append(" #");
      brand_name.append(std::to_string(brand_id));
      if (name != nullptr) {
        *name = std::move(brand_name);
      }
      brand_id += (state_ref.brand_base * 1000 + state_ref.last_class) * 1000;
      if (id != nullptr) {
        *id = brand_id;
      }
      break;
    }
    default:
      throw std::runtime_error("invalid hierarchy level");
  }
}

void MakeWord(std::string* dest, const std::string& syllable_set, int64_t src,
              int char_count, DstDistributionStore* store) {
  if (dest == nullptr || store == nullptr) {
    return;
  }
  const auto& dist = store->Get(syllable_set);
  int dist_size = DistributionSize(dist);
  if (dist_size <= 0) {
    dest->clear();
    return;
  }

  dest->clear();
  int64_t value = src;
  while (value > 0) {
    int index = static_cast<int>(value % dist_size) + 1;
    const std::string& syllable = dist.GetString(index, 1);
    if (static_cast<int>(dest->size() + syllable.size()) <= char_count) {
      dest->append(syllable);
    } else {
      break;
    }
    value /= dist_size;
  }
}

void MakeCompanyName(std::string* dest, int table_number, int company,
                     DstDistributionStore* store) {
  (void)table_number;
  MakeWord(dest, "syllables", company, 10, store);
}

void EmbedString(std::string* dest, const std::string& dist_name, int value_set,
                 int weight_set, DstDistributionStore* store,
                 RandomNumberStream* stream) {
  if (dest == nullptr || store == nullptr || stream == nullptr) {
    return;
  }
  const auto& dist = store->Get(dist_name);
  int picked = dist.PickIndex(weight_set, stream);
  std::string word = dist.GetString(picked, value_set);
  if (word.empty() || dest->empty()) {
    return;
  }
  int max_pos = static_cast<int>(dest->size() - word.size() - 1);
  if (max_pos < 0) {
    return;
  }
  int pos = GenerateUniformRandomInt(0, max_pos, stream);
  dest->replace(static_cast<size_t>(pos), word.size(), word);
}

}  // namespace benchgen::tpcds::internal
