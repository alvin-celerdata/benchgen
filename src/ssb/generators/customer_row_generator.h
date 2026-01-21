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

#include <string>

#include "benchgen/generator_options.h"
#include "ssb_types.h"
#include "utils/context.h"
#include "utils/random.h"

namespace benchgen::ssb::internal {

class CustomerRowGenerator {
 public:
  CustomerRowGenerator(double scale_factor, DbgenSeedMode seed_mode);

  arrow::Status Init();
  void SkipRows(int64_t rows);
  void GenerateRow(int64_t row_number, customer_t* out);

 private:
  double scale_factor_;
  DbgenSeedMode seed_mode_;
  bool initialized_ = false;
  DbgenContext context_;
  RandomState random_state_;
};

}  // namespace benchgen::ssb::internal
