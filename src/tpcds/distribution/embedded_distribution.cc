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

#include "distribution/embedded_distribution.h"

#include "distribution/embedded_distribution_data.h"

namespace benchgen::tpcds::internal {

const EmbeddedDistributionFile* FindEmbeddedDistributionFile(
    std::string_view name) {
  for (size_t i = 0; i < kEmbeddedDistributionFileCount; ++i) {
    const EmbeddedDistributionFile& file = kEmbeddedDistributionFiles[i];
    if (name == file.name) {
      return &file;
    }
  }
  return nullptr;
}

bool HasEmbeddedDistribution() {
  return kEmbeddedDistributionFileCount > 0;
}

}  // namespace benchgen::tpcds::internal
