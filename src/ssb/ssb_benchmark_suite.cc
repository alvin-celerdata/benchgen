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

#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include "benchgen/benchmark_suite.h"
#include "benchgen/table.h"
#include "utils/scaling.h"

namespace benchgen {
namespace {

class SsbSuite final : public BenchmarkSuite {
 public:
  SuiteId suite_id() const override { return SuiteId::kSsb; }
  std::string_view name() const override { return "ssb"; }

  int table_count() const override {
    return static_cast<int>(ssb::TableId::kTableCount);
  }

  std::string_view TableName(int table_index) const override {
    if (table_index < 0 || table_index >= table_count()) {
      return std::string_view();
    }
    return ssb::TableIdToString(static_cast<ssb::TableId>(table_index));
  }

  arrow::Status MakeIterator(
      std::string_view table_name, GeneratorOptions options,
      std::unique_ptr<RecordBatchIterator>* out) const override {
    return MakeRecordBatchIterator(SuiteId::kSsb, table_name,
                                   std::move(options), out);
  }

  arrow::Status ResolveTableRowCount(std::string_view table_name,
                                     const GeneratorOptions& options,
                                     int64_t* out,
                                     bool* is_known) const override {
    if (out == nullptr || is_known == nullptr) {
      return arrow::Status::Invalid(
          "row count output parameters must not be null");
    }
    *is_known = false;

    ssb::TableId table_id;
    if (!ssb::TableIdFromString(table_name, &table_id)) {
      return arrow::Status::Invalid("Unknown SSB table: ",
                                    std::string(table_name));
    }

    int64_t rows = ssb::internal::RowCount(table_id, options.scale_factor);
    if (rows < 0) {
      return arrow::Status::OK();
    }

    *out = rows;
    *is_known = true;
    return arrow::Status::OK();
  }
};

}  // namespace

std::unique_ptr<BenchmarkSuite> MakeSsbBenchmarkSuite() {
  return std::make_unique<SsbSuite>();
}

}  // namespace benchgen
