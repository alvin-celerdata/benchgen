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
#include <iosfwd>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>

#include "distribution/dst_distribution.h"

namespace benchgen::tpcds::internal {

class DstDistributionStore {
 public:
  DstDistributionStore();

  const DstDistribution& Get(std::string_view name) const;

 private:
  bool LoadIdxFile(const std::string& path);
  bool LoadIdxData(const unsigned char* data, size_t size,
                   const std::string& label);
  bool LoadIdxStream(std::istream* file, const std::string& path);
  void LoadFile(const std::string& path);
  void AddDistribution(DstDistribution distribution);

  std::unordered_map<std::string, DstDistribution> distributions_;
  std::unordered_set<std::string> loaded_files_;
};

}  // namespace benchgen::tpcds::internal
