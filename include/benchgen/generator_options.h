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
#include <vector>

namespace benchgen {

enum class DbgenSeedMode {
  kAllTables,
  kPerTable,
};

struct GeneratorOptions {
  double scale_factor = 1.0;
  int64_t start_row = 0;   // 0-based row index.
  int64_t row_count = -1;  // Negative means "to the end of the table".
  int64_t chunk_size = 4096;
  // Override distribution files directory; empty uses embedded resources.
  std::string distribution_dir;
  // When set, only these columns are returned (order preserved). Empty means
  // all columns.
  std::vector<std::string> column_names;
  // Controls dbgen seed initialization. kPerTable matches `dbgen -T <table>`,
  // kAllTables matches `dbgen -T a`.
  DbgenSeedMode seed_mode = DbgenSeedMode::kPerTable;
};

}  // namespace benchgen
