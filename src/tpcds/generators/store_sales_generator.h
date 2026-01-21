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

#include <memory>
#include <string>
#include <vector>

#include "benchgen/arrow_compat.h"
#include "benchgen/generator_options.h"
#include "benchgen/record_batch_iterator.h"

namespace benchgen::tpcds {

class StoreSalesGenerator final : public RecordBatchIterator {
 public:
  explicit StoreSalesGenerator(GeneratorOptions options);
  ~StoreSalesGenerator() override;

  std::shared_ptr<arrow::Schema> schema() const override;
  std::string_view name() const override;
  std::string_view suite_name() const override;
  arrow::Status Next(std::shared_ptr<arrow::RecordBatch>* out) override;

  int64_t total_rows() const;
  int64_t remaining_rows() const;

  static int64_t TotalRows(double scale_factor);

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace benchgen::tpcds
