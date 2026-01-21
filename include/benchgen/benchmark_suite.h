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

#include <cstdint>
#include <memory>
#include <string_view>

#include "benchgen/record_batch_iterator_factory.h"

namespace benchgen {

class BenchmarkSuite {
 public:
  virtual ~BenchmarkSuite() = default;

  virtual SuiteId suite_id() const = 0;
  virtual std::string_view name() const = 0;
  virtual int table_count() const = 0;
  virtual std::string_view TableName(int table_index) const = 0;

  virtual arrow::Status MakeIterator(
      std::string_view table_name, GeneratorOptions options,
      std::unique_ptr<RecordBatchIterator>* out) const = 0;

  virtual arrow::Status ResolveTableRowCount(std::string_view table_name,
                                             const GeneratorOptions& options,
                                             int64_t* out,
                                             bool* is_known) const = 0;
};

SuiteId SuiteIdFromString(std::string_view value);
std::string_view SuiteIdToString(SuiteId suite);

std::unique_ptr<BenchmarkSuite> MakeBenchmarkSuite(SuiteId suite);
std::unique_ptr<BenchmarkSuite> MakeBenchmarkSuite(std::string_view name);

}  // namespace benchgen
