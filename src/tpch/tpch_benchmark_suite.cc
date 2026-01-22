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
#include "distribution/scaling.h"

namespace benchgen {
namespace {

class TpchSuite final : public BenchmarkSuite {
 public:
  SuiteId suite_id() const override { return SuiteId::kTpch; }
  std::string_view name() const override { return "tpch"; }

  int table_count() const override {
    return static_cast<int>(tpch::TableId::kTableCount);
  }

  std::string_view TableName(int table_index) const override {
    if (table_index < 0 || table_index >= table_count()) {
      return std::string_view();
    }
    return tpch::TableIdToString(static_cast<tpch::TableId>(table_index));
  }

  arrow::Status MakeIterator(
      std::string_view table_name, GeneratorOptions options,
      std::unique_ptr<RecordBatchIterator>* out) const override {
    return MakeRecordBatchIterator(SuiteId::kTpch, table_name,
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

    tpch::TableId table_id;
    if (!tpch::TableIdFromString(table_name, &table_id)) {
      return arrow::Status::Invalid("Unknown TPC-H table: ",
                                    std::string(table_name));
    }

    switch (table_id) {
      case tpch::TableId::kNation:
        *out = 25;
        *is_known = true;
        return arrow::Status::OK();
      case tpch::TableId::kRegion:
        *out = 5;
        *is_known = true;
        return arrow::Status::OK();
      case tpch::TableId::kPart:
      case tpch::TableId::kPartSupp:
      case tpch::TableId::kSupplier:
      case tpch::TableId::kCustomer:
      case tpch::TableId::kOrders:
      case tpch::TableId::kLineItem: {
        int64_t rows =
            tpch::internal::RowCount(table_id, options.scale_factor);
        if (rows < 0) {
          return arrow::Status::OK();
        }
        *out = rows;
        *is_known = true;
        return arrow::Status::OK();
      }
      case tpch::TableId::kTableCount:
        break;
    }

    return arrow::Status::Invalid("Unknown TPC-H table: ",
                                  std::string(table_name));
  }
};

}  // namespace

std::unique_ptr<BenchmarkSuite> MakeTpchBenchmarkSuite() {
  return std::make_unique<TpchSuite>();
}

}  // namespace benchgen
