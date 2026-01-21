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

#include <cstdint>
#include <string>

#include "benchgen/generator_options.h"

namespace benchgen::cli {

struct GenTableArgs {
  std::string table;
  double scale_factor = 1.0;
  int64_t chunk_size = 10000;
  int64_t start_row = 0;
  int64_t row_count = -1;
  std::string output;
  benchgen::DbgenSeedMode seed_mode = benchgen::DbgenSeedMode::kPerTable;
  int64_t parallel = 1;
};

}  // namespace benchgen::cli
