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

#include <cstddef>
#include <cstdint>
#include <iosfwd>
#include <string>
#include <unordered_map>
#include <vector>

namespace benchgen::ssb::internal {

struct DistributionEntry {
  std::string text;
  int64_t weight = 0;
};

struct Distribution {
  std::vector<DistributionEntry> list;
  int64_t max = 0;
};

class DistributionStore {
 public:
  arrow::Status LoadFromFile(const std::string& path);
  arrow::Status LoadFromBuffer(const unsigned char* data, std::size_t size);

  const Distribution* Find(const std::string& name) const;

 private:
  arrow::Status Parse(std::istream& input);

  std::unordered_map<std::string, Distribution> distributions_;
};

}  // namespace benchgen::ssb::internal
